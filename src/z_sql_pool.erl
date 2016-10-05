%% @author Tran Hoan
%% @doc


-module(z_sql_pool).
-behaviour(gen_server).
-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
		 terminate/2,
		 code_change/3
]).


-export([start_link/3,
		 db_type/1,
		 db_schema/1,
		 get_connection/1,
		 get_connection/2
		]).

-include("logger.hrl").

-record(state,{
	active_pids = [],
	ptr = 1,
	pool_size = 1,
	start_args,
	start_opts = []
}).

start_link(PoolName, Args, Opts) ->
	gen_server:start_link({local, PoolName}, ?MODULE, {Args,Opts}, []).

get_connection(Pool) -> get_connection(Pool, roundrobin).
get_connection(Pool, Type) ->
	case catch gen_server:call(Pool, {get_connection,Type}) of
		{'EXIT',Trace} ->
			?ERROR("get_connection crashed ~p",[Trace]),
			{error,crashed};
		Ret -> Ret
	end.

db_type(Pool) ->
	gen_server:call(Pool, db_type).

db_schema(Pool) ->
	gen_server:call(Pool, db_schema).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
	Result :: {ok, State}
			| {ok, State, Timeout}
			| {ok, State, hibernate}
			| {stop, Reason :: term()}
			| ignore,
	State :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
%% Args {DBType, Server, Port, DB, User, Pass, PoolSize}
init({Args, Opts}) ->
	self() ! check_connection,
	process_flag(trap_exit, true),
	case Args of
		{DBType,Server, Port, DB, User, Pass} ->
			{ok, #state{start_args = {DBType,Server, Port, DB, User, Pass}, start_opts = Opts}};
		{DBType,Server, Port, DB, User, Pass, Size} ->
			{ok, #state{start_args = {DBType,Server, Port, DB, User, Pass}, start_opts = Opts, pool_size = Size}}
	end.


%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
	Result :: {reply, Reply, NewState}
			| {reply, Reply, NewState, Timeout}
			| {reply, Reply, NewState, hibernate}
			| {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason, Reply, NewState}
			| {stop, Reason, NewState},
	Reply :: term(),
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity,
	Reason :: term().
%% ====================================================================
handle_call({get_connection,roundrobin}, _From, #state{active_pids = []} = State) ->
	{reply, {error, no_connection}, State};

handle_call({get_connection,roundrobin}, _From, #state{ptr = Ptr, active_pids = Actives} = State) ->
	Ptr0 = if length(Actives) < Ptr -> 1;
			  true -> Ptr
		   end,
	Pid = lists:nth(Ptr0, Actives),
	Ptr1 = Ptr0 + 1,
	{reply, {ok, Pid}, State#state{ptr = Ptr1}};

handle_call(db_schema, _From, State) ->
	{reply, element(4,State#state.start_args), State};

handle_call(db_type, _From, State) ->
	{reply, element(1,State#state.start_args), State};

handle_call(Request, _From, State) ->
    ?NOTICE("!<< ~p",[Request]),
    {reply, {error, noop}, State}.


%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast(Msg, State) ->
	?NOTICE("!<< ~p",[Msg]),
    {noreply, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
	Result :: {noreply, NewState}
			| {noreply, NewState, Timeout}
			| {noreply, NewState, hibernate}
			| {stop, Reason :: term(), NewState},
	NewState :: term(),
	Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info(check_connection, #state{active_pids = Actives, pool_size = Size, start_args = Args, start_opts = Opts} = State) ->
	N  = Size - length(Actives),
	Pids = start_n_connection(N, Args, Opts, lists:reverse(Actives)),
	if length(Pids) < Size ->
		   erlang:send_after(opt(sql_start_interval,Opts, 30000), self(), check_connection);
	   true ->
		   ignore
	end,
	{noreply, State#state{active_pids = Pids}};

handle_info({'EXIT', Pid, Reason}, #state{pool_size = Size, start_opts = Opts} = State) ->
	?DEBUG("may be connection ~p died by ~p",[Pid, Reason]),
	Pids = lists:delete(Pid, State#state.active_pids),
	if length(Pids) < Size ->
		   erlang:send_after(opt(sql_start_interval,Opts, 30000), self(), check_connection);
	   true ->
		   ignore
	end,
	{noreply, State#state{active_pids = Pids}};

handle_info(Info, State) ->
	?NOTICE("!<< ~p",[Info]),
    {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(Reason, _State) ->
	?DEBUG("died by ~p",[Reason]),
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
	Result :: {ok, NewState :: term()} | {error, Reason :: term()},
	OldVsn :: Vsn | {down, Vsn},
	Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


start_n_connection(N,_Args, _Opts,AccIn) when N =< 0 -> lists:reverse(AccIn);
start_n_connection(N, Args, Opts, AccIn) ->
	case z_sql:start_link(Args,Opts) of
		{ok, Pid} ->
			start_n_connection(N - 1, Args, Opts, [Pid | AccIn]);
		Exp ->
			?ERROR("start connection ~p",[Exp]),
			start_n_connection(N - 1, Args, Opts, AccIn)
	end.

opt(Key, Options, Default) ->
	case lists:keyfind(Key, 1, Options) of
		{Key, Value} -> Value;
		_ -> Default
	end.
