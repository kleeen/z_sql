%%%----------------------------------------------------------------------
%%% File    : ejabberd_sql.erl
%%% Author  : Alexey Shchepin <alexey@process-one.net>
%%% Purpose : Serve SQL connection
%%% Created :  8 Dec 2004 by Alexey Shchepin <alexey@process-one.net>
%%%
%%%
%%% ejabberd, Copyright (C) 2002-2016   ProcessOne
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License along
%%% with this program; if not, write to the Free Software Foundation, Inc.,
%%% 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
%%%
%%%----------------------------------------------------------------------

%% @author Tran Hoan
%% @doc only support MySQL and PgSQL
%% Copy from ejabberd project

-module(z_sql).

-author('alexey@process-one.net').

-define(GEN_FSM, gen_fsm).

-behaviour(?GEN_FSM).

%% External exports
-export([start_link/2,
		 sql_query/2,
		 sql_query_t/1,
		 sql_transaction/2,
		 sql_bloc/2,
		 sql_query_to_iolist/1,
		 escape/1,
		 standard_escape/1,
		 escape_like/1,
		 escape_like_arg/1,
		 escape_like_arg_circumflex/1,
		 to_bool/1,
		 encode_term/1,
		 decode_term/1,
		 keep_alive/1
]).

%% gen_fsm callbacks
-export([init/1,
		 handle_event/3,
		 handle_sync_event/4,
		 handle_info/3,
		 terminate/3,
		 print_state/1,
		 code_change/4
		]).

-export([connecting/2,
		 connecting/3,
		 session_established/2,
		 session_established/3
		]).

-include("logger.hrl").
-include("z_sql_pt.hrl").

-record(state,
	{db_ref = self()                     :: pid(),
	 db_type = mysql                     :: pgsql | mysql ,
	 db_version = undefined              :: undefined | non_neg_integer(),
	 start_interval = 0                  :: non_neg_integer(),
	 host = <<"">>                       :: binary(),
	 max_pending_requests_len            :: non_neg_integer(),
	 pending_requests = {0, queue:new()} :: {non_neg_integer(), queue:queue()}}).

-define(STATE_KEY, z_sql_state).

-define(NESTING_KEY, z_sql_nesting_level).

-define(TOP_LEVEL_TXN, 0).

-define(PGSQL_PORT, 5432).

-define(MYSQL_PORT, 3306).

-define(MAX_TRANSACTION_RESTARTS, 3).

-define(TRANSACTION_TIMEOUT, 60000).

-define(KEEPALIVE_TIMEOUT, 600).

-define(KEEPALIVE_QUERY, [<<"SELECT 1;">>]).

-define(PREPARE_KEY, z_sql_prepare).

%%%----------------------------------------------------------------------
%%% API
%%%----------------------------------------------------------------------

-spec start_link(Args, Options) -> {ok, pid()} | ignore | {error, any()} when
	Args :: {DBType, Server, Port, Schema, User, Pass},
	Options :: list(),
	DBType :: mysql | pgsql,
	Server :: list(),
	Port	:: 1..65535,
	Schema :: list(),
	User :: list() ,
	Pass :: list().
start_link(Args, Options) ->
    (?GEN_FSM):start_link(?MODULE,{Args, Options},fsm_limit_opts()).

-type sql_query() :: [sql_query() | binary()] | #sql_query{} |
                     fun(() -> any()) | fun((atom(), _) -> any()).
-type sql_query_result() :: {updated, non_neg_integer()} |
                            {error, binary()} |
                            {selected, [binary()],
                             [[binary()]]} |
                            {selected, [any()]}.

-spec sql_query(binary(), sql_query()) -> sql_query_result().

sql_query(Pool, Query) ->
    check_error(sql_call(Pool, {sql_query, Query}), Query).

%% SQL transaction based on a list of queries
%% This function automatically
-spec sql_transaction(binary(), [sql_query()] | fun(() -> any())) ->
                             {atomic, any()} |
                             {aborted, any()}.

sql_transaction(Pool, Queries)
    when is_list(Queries) ->
    F = fun () ->
		lists:foreach(fun (Query) -> sql_query_t(Query) end,
			      Queries)
	end,
    sql_transaction(Pool, F);
%% SQL transaction, based on a erlang anonymous function (F = fun)
sql_transaction(Pool, F) when is_function(F) ->
    sql_call(Pool, {sql_transaction, F}).

%% SQL bloc, based on a erlang anonymous function (F = fun)
sql_bloc(Host, F) -> sql_call(Host, {sql_bloc, F}).

sql_call(Pool, Msg) ->
    case get(?STATE_KEY) of
      undefined ->
        case z_sql_pool:get_connection(Pool) of
          none -> {error, <<"Unknown Host">>};
          {ok,Pid} ->
            (?GEN_FSM):sync_send_event(Pid,{sql_cmd, Msg,erlang:system_time(milli_seconds)},?TRANSACTION_TIMEOUT)
          end;
      _State -> nested_op(Msg)
    end.

keep_alive(PID) ->
    (?GEN_FSM):sync_send_event(PID,
			       {sql_cmd, {sql_query, ?KEEPALIVE_QUERY},
                                erlang:system_time(milli_seconds)},
			       ?KEEPALIVE_TIMEOUT).

-spec sql_query_t(sql_query()) -> sql_query_result().

%% This function is intended to be used from inside an sql_transaction:
sql_query_t(Query) ->
    QRes = sql_query_internal(Query),
    case QRes of
      {error, Reason} -> throw({aborted, Reason});
      Rs when is_list(Rs) ->
	  case lists:keysearch(error, 1, Rs) of
	    {value, {error, Reason}} -> throw({aborted, Reason});
	    _ -> QRes
	  end;
      _ -> QRes
    end.

%% Escape character that will confuse an SQL engine
escape(S) ->
	<< <<(z_sql_lib:escape(Char))/binary>> || <<Char>> <= S >>.

%% Escape character that will confuse an SQL engine
%% Percent and underscore only need to be escaped for pattern matching like
%% statement
escape_like(S) when is_binary(S) ->
    << <<(escape_like(C))/binary>> || <<C>> <= S >>;
escape_like($%) -> <<"\\%">>;
escape_like($_) -> <<"\\_">>;
escape_like($\\) -> <<"\\\\\\\\">>;
escape_like(C) when is_integer(C), C >= 0, C =< 255 -> z_sql_lib:escape(C).

escape_like_arg(S) when is_binary(S) ->
    << <<(escape_like_arg(C))/binary>> || <<C>> <= S >>;
escape_like_arg($%) -> <<"\\%">>;
escape_like_arg($_) -> <<"\\_">>;
escape_like_arg($\\) -> <<"\\\\">>;
escape_like_arg(C) when is_integer(C), C >= 0, C =< 255 -> <<C>>.

escape_like_arg_circumflex(S) when is_binary(S) ->
    << <<(escape_like_arg_circumflex(C))/binary>> || <<C>> <= S >>;
escape_like_arg_circumflex($%) -> <<"^%">>;
escape_like_arg_circumflex($_) -> <<"^_">>;
escape_like_arg_circumflex($^) -> <<"^^">>;
escape_like_arg_circumflex($[) -> <<"^[">>;     % For MSSQL
escape_like_arg_circumflex($]) -> <<"^]">>;
escape_like_arg_circumflex(C) when is_integer(C), C >= 0, C =< 255 -> <<C>>.

to_bool(<<"t">>) -> true;
to_bool(<<"true">>) -> true;
to_bool(<<"1">>) -> true;
to_bool(true) -> true;
to_bool(1) -> true;
to_bool(_) -> false.

encode_term(Term) ->
    escape(list_to_binary(
             erl_prettypr:format(erl_syntax:abstract(Term),
                                 [{paper, 65535}, {ribbon, 65535}]))).

decode_term(Bin) ->
    Str = binary_to_list(<<Bin/binary, ".">>),
    {ok, Tokens, _} = erl_scan:string(Str),
    {ok, Term} = erl_parse:parse_term(Tokens),
    Term.

%%%----------------------------------------------------------------------
%%% Callback functions from gen_fsm
%%%----------------------------------------------------------------------
init({Args, Options}) ->
	StartInterval = opt(sql_start_interval, Options, 30000),
    KeepaliveInterval =  opt(sql_keepalive_interval, Options, 30000),
    timer:apply_interval(KeepaliveInterval * 1000, ?MODULE,keep_alive, [self()]),
    {DBType, Server, Port, Schema, User, Pass} = Args,
    (?GEN_FSM):send_event(self(), connect),
    {ok, connecting,
     #state{db_type = DBType, host = {Server, Port, Schema, User, Pass},
			max_pending_requests_len = max_fsm_queue(),
			pending_requests = {0, queue:new()},
			start_interval = StartInterval}}.

connecting(connect, #state{db_type = DBType, host = Args} = State) ->
    ConnectRes = case DBType of
		   mysql -> apply(fun mysql_connect/1, [Args]);
           pgsql -> apply(fun pgsql_connect/1, [Args])
		 end,
    {_, PendingRequests} = State#state.pending_requests,
    case ConnectRes of
        {ok, Ref} ->
            erlang:monitor(process, Ref),
            lists:foreach(fun (Req) ->
                                  (?GEN_FSM):send_event(self(), Req)
                          end,
                          queue:to_list(PendingRequests)),
            State1 = State#state{db_ref = Ref,
                                 pending_requests = {0, queue:new()}},
            State2 = get_db_version(State1),
            {next_state, session_established, State2};
      {error, Reason} ->
	  ?INFO("~p connection failed:~n** Reason: ~p~n** "
		    "Retry after: ~p seconds",
		    [State#state.db_type, Reason,
		     State#state.start_interval div 1000]),
	  (?GEN_FSM):send_event_after(State#state.start_interval,
				      connect),
	  {next_state, connecting, State}
    end;
connecting(Event, State) ->
    ?WARN("unexpected event in 'connecting': ~p",
		 [Event]),
    {next_state, connecting, State}.

connecting({sql_cmd, {sql_query, ?KEEPALIVE_QUERY},
	    _Timestamp},
	   From, State) ->
    (?GEN_FSM):reply(From,
		     {error, <<"SQL connection failed">>}),
    {next_state, connecting, State};
connecting({sql_cmd, Command, Timestamp} = Req, From,
	   State) ->
    ?DEBUG("queuing pending request while connecting:~n\t~p",
	   [Req]),
    {Len, PendingRequests} = State#state.pending_requests,
    NewPendingRequests = if Len <
			      State#state.max_pending_requests_len ->
				{Len + 1,
				 queue:in({sql_cmd, Command, From, Timestamp},
					  PendingRequests)};
			    true ->
				lists:foreach(fun ({sql_cmd, _, To,
						    _Timestamp}) ->
						      (?GEN_FSM):reply(To,
								       {error,
									<<"SQL connection failed">>})
					      end,
					      queue:to_list(PendingRequests)),
				{1,
				 queue:from_list([{sql_cmd, Command, From,
						   Timestamp}])}
			 end,
    {next_state, connecting,
     State#state{pending_requests = NewPendingRequests}};
connecting(Request, {Who, _Ref}, State) ->
    ?WARN("unexpected call ~p from ~p in 'connecting'",
		 [Request, Who]),
    {reply, {error, badarg}, connecting, State}.

session_established({sql_cmd, Command, Timestamp}, From,
		    State) ->
    run_sql_cmd(Command, From, State, Timestamp);
session_established(Request, {Who, _Ref}, State) ->
    ?WARN("unexpected call ~p from ~p in 'session_establ"
		 "ished'",
		 [Request, Who]),
    {reply, {error, badarg}, session_established, State}.

session_established({sql_cmd, Command, From, Timestamp},
		    State) ->
    run_sql_cmd(Command, From, State, Timestamp);
session_established(Event, State) ->
    ?WARN("unexpected event in 'session_established': ~p",
		 [Event]),
    {next_state, session_established, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, badarg}, StateName, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% We receive the down signal when we loose the MySQL connection (we are
%% monitoring the connection)
handle_info({'DOWN', _MonitorRef, process, _Pid, _Info},
	    _StateName, State) ->
    (?GEN_FSM):send_event(self(), connect),
    {next_state, connecting, State};
handle_info(Info, StateName, State) ->
    ?WARN("unexpected info in ~p: ~p",
		 [StateName, Info]),
    {next_state, StateName, State}.

terminate(Reason, StateName, State) ->
	?INFO("teminated by ~p in ~p",[Reason, StateName]),
    case State#state.db_type of
        mysql -> catch p1_mysql_conn:stop(State#state.db_ref);
        _ -> ok
    end,
    ok.

%%----------------------------------------------------------------------
%% Func: print_state/1
%% Purpose: Prepare the state to be printed on error log
%% Returns: State to print
%%----------------------------------------------------------------------
print_state(State) -> State.

%%%----------------------------------------------------------------------
%%% Internal functions
%%%----------------------------------------------------------------------

run_sql_cmd(Command, From, State, Timestamp) ->
    case erlang:system_time(milli_seconds) - Timestamp of
      Age when Age < (?TRANSACTION_TIMEOUT) ->
	  put(?NESTING_KEY, ?TOP_LEVEL_TXN),
	  put(?STATE_KEY, State),
	  abort_on_driver_error(outer_op(Command), From);
      Age ->
	  ?ERROR("Database was not available or too slow, "
		     "discarding ~p milliseconds old request~n~p~n",
		     [Age, Command]),
	  {next_state, session_established, State}
    end.

%% Only called by handle_call, only handles top level operations.
%% @spec outer_op(Op) -> {error, Reason} | {aborted, Reason} | {atomic, Result}
outer_op({sql_query, Query}) ->
    sql_query_internal(Query);
outer_op({sql_transaction, F}) ->
    outer_transaction(F, ?MAX_TRANSACTION_RESTARTS, <<"">>);
outer_op({sql_bloc, F}) -> execute_bloc(F).

%% Called via sql_query/transaction/bloc from client code when inside a
%% nested operation
nested_op({sql_query, Query}) ->
    sql_query_internal(Query);
nested_op({sql_transaction, F}) ->
    NestingLevel = get(?NESTING_KEY),
    if NestingLevel =:= (?TOP_LEVEL_TXN) ->
	   outer_transaction(F, ?MAX_TRANSACTION_RESTARTS, <<"">>);
       true -> inner_transaction(F)
    end;
nested_op({sql_bloc, F}) -> execute_bloc(F).

%% Never retry nested transactions - only outer transactions
inner_transaction(F) ->
    PreviousNestingLevel = get(?NESTING_KEY),
    case get(?NESTING_KEY) of
      ?TOP_LEVEL_TXN ->
	  {backtrace, T} = process_info(self(), backtrace),
	  ?ERROR("inner transaction called at outer txn "
		     "level. Trace: ~s",
		     [T]),
	  erlang:exit(implementation_faulty);
      _N -> ok
    end,
    put(?NESTING_KEY, PreviousNestingLevel + 1),
    Result = (catch F()),
    put(?NESTING_KEY, PreviousNestingLevel),
    case Result of
      {aborted, Reason} -> {aborted, Reason};
      {'EXIT', Reason} -> {'EXIT', Reason};
      {atomic, Res} -> {atomic, Res};
      Res -> {atomic, Res}
    end.

outer_transaction(F, NRestarts, _Reason) ->
    PreviousNestingLevel = get(?NESTING_KEY),
    case get(?NESTING_KEY) of
      ?TOP_LEVEL_TXN -> ok;
      _N ->
	  {backtrace, T} = process_info(self(), backtrace),
	  ?ERROR("outer transaction called at inner txn "
		     "level. Trace: ~s",
		     [T]),
	  erlang:exit(implementation_faulty)
    end,
    sql_query_internal([<<"begin;">>]),
    put(?NESTING_KEY, PreviousNestingLevel + 1),
    Result = (catch F()),
    put(?NESTING_KEY, PreviousNestingLevel),
    case Result of
      {aborted, Reason} when NRestarts > 0 ->
	  sql_query_internal([<<"rollback;">>]),
	  outer_transaction(F, NRestarts - 1, Reason);
      {aborted, Reason} when NRestarts =:= 0 ->
	  ?ERROR("SQL transaction restarts exceeded~n** "
		     "Restarts: ~p~n** Last abort reason: "
		     "~p~n** Stacktrace: ~p~n** When State "
		     "== ~p",
		     [?MAX_TRANSACTION_RESTARTS, Reason,
		      erlang:get_stacktrace(), get(?STATE_KEY)]),
	  sql_query_internal([<<"rollback;">>]),
	  {aborted, Reason};
      {'EXIT', Reason} ->
	  sql_query_internal([<<"rollback;">>]), {aborted, Reason};
      Res -> sql_query_internal([<<"commit;">>]), {atomic, Res}
    end.

execute_bloc(F) ->
    case catch F() of
      {aborted, Reason} -> {aborted, Reason};
      {'EXIT', Reason} -> {aborted, Reason};
      Res -> {atomic, Res}
    end.

execute_fun(F) when is_function(F, 0) ->
    F();
execute_fun(F) when is_function(F, 2) ->
    State = get(?STATE_KEY),
    F(State#state.db_type, State#state.db_version).

sql_query_internal([{_, _} | _] = Queries) ->
    State = get(?STATE_KEY),
    case select_sql_query(Queries, State) of
        undefined ->
            {error, <<"no matching query for the current DBMS found">>};
        Query ->
            sql_query_internal(Query)
    end;
sql_query_internal(#sql_query{} = Query) ->
    State = get(?STATE_KEY),
    Res =
        try
            case State#state.db_type of
                pgsql ->
                    Key = {?PREPARE_KEY, Query#sql_query.hash},
                    case get(Key) of
                        undefined ->
                            case pgsql_prepare(Query, State) of
                                {ok, _, _, _} ->
                                    put(Key, prepared);
                                {error, Error} ->
                                    ?ERROR("PREPARE failed for SQL query "
                                               "at ~p: ~p",
                                               [Query#sql_query.loc, Error]),
                                    put(Key, ignore)
                            end;
                        _ ->
                            ok
                    end,
                    case get(Key) of
                        prepared ->
                            pgsql_execute_sql_query(Query, State);
                        _ ->
                            generic_sql_query(Query)
                    end;
                mysql ->
                    generic_sql_query(Query)
            end
        catch
            Class:Reason ->
                ST = erlang:get_stacktrace(),
                ?ERROR("Internal error while processing SQL query: ~p",
                           [{Class, Reason, ST}]),
                {error, <<"internal error">>}
        end,
    case Res of
        {error, <<"No SQL-driver information available.">>} ->
            {updated, 0};
        _Else -> Res
    end;
sql_query_internal(F) when is_function(F) ->
    case catch execute_fun(F) of
        {'EXIT', Reason} -> {error, Reason};
        Res -> Res
    end;
sql_query_internal(Query) ->
    State = get(?STATE_KEY),
%%    ?DEBUG("SQL: \"~s\"", [Query]),
    Res = case State#state.db_type of
			  pgsql ->
				  pgsql_to_odbc(pgsql:squery(State#state.db_ref, Query));
			  mysql ->
				  R = mysql_to_odbc(p1_mysql_conn:squery(State#state.db_ref,
														 [Query], self(),
														 [{timeout, (?TRANSACTION_TIMEOUT) - 1000},
														  {result_type, binary}])),
				  %% ?INFO("MySQL, Received result~n~p~n", [R]),
				  R
		  end,
    case Res of
      {error, <<"No SQL-driver information available.">>} ->
	  {updated, 0};
      _Else -> Res
    end.

select_sql_query(Queries, State) ->
    select_sql_query(
      Queries, State#state.db_type, State#state.db_version, undefined).

select_sql_query([], _Type, _Version, undefined) ->
    undefined;
select_sql_query([], _Type, _Version, Query) ->
    Query;
select_sql_query([{any, Query} | _], _Type, _Version, _) ->
    Query;
select_sql_query([{Type, Query} | _], Type, _Version, _) ->
    Query;
select_sql_query([{{Type, _Version1}, Query1} | Rest], Type, undefined, _) ->
    select_sql_query(Rest, Type, undefined, Query1);
select_sql_query([{{Type, Version1}, Query1} | Rest], Type, Version, Query) ->
    if
        Version >= Version1 ->
            Query1;
        true ->
            select_sql_query(Rest, Type, Version, Query)
    end;
select_sql_query([{_, _} | Rest], Type, Version, Query) ->
    select_sql_query(Rest, Type, Version, Query).

generic_sql_query(SQLQuery) ->
    sql_query_format_res(
      sql_query_internal(generic_sql_query_format(SQLQuery)),
      SQLQuery).

generic_sql_query_format(SQLQuery) ->
    Args = (SQLQuery#sql_query.args)(generic_escape()),
    (SQLQuery#sql_query.format_query)(Args).

generic_escape() ->
    #sql_escape{string = fun(X) -> <<"'", (escape(X))/binary, "'">> end,
                integer = fun(X) -> integer_to_binary(X) end,
                boolean = fun(true) -> <<"1">>;
                             (false) -> <<"0">>
                          end
               }.

standard_escape(S) ->
    << <<(case Char of
              $' -> << "''" >>;
              _ -> << Char >>
          end)/binary>> || <<Char>> <= S >>.

pgsql_prepare(SQLQuery, State) ->
    Escape = #sql_escape{_ = fun(X) -> X end},
    N = length((SQLQuery#sql_query.args)(Escape)),
    Args = [<<$$, (integer_to_binary(I))/binary>> || I <- lists:seq(1, N)],
    Query = (SQLQuery#sql_query.format_query)(Args),
    pgsql:prepare(State#state.db_ref, SQLQuery#sql_query.hash, Query).

pgsql_execute_escape() ->
    #sql_escape{string = fun(X) -> X end,
                integer = fun(X) -> [integer_to_binary(X)] end,
                boolean = fun(true) -> "1";
                             (false) -> "0"
                          end
               }.

pgsql_execute_sql_query(SQLQuery, State) ->
    Args = (SQLQuery#sql_query.args)(pgsql_execute_escape()),
    ExecuteRes =
        pgsql:execute(State#state.db_ref, SQLQuery#sql_query.hash, Args),
%    {T, ExecuteRes} =
%        timer:tc(pgsql, execute, [State#state.db_ref, SQLQuery#sql_query.hash, Args]),
%    io:format("T ~s ~p~n", [SQLQuery#sql_query.hash, T]),
    Res = pgsql_execute_to_odbc(ExecuteRes),
    sql_query_format_res(Res, SQLQuery).


sql_query_format_res({selected, _, Rows}, SQLQuery) ->
    Res =
        lists:flatmap(
          fun(Row) ->
                  try
                      [(SQLQuery#sql_query.format_res)(Row)]
                  catch
                      Class:Reason ->
                          ST = erlang:get_stacktrace(),
                          ?ERROR("Error while processing "
                                     "SQL query result: ~p~n"
                                     "row: ~p",
                                     [{Class, Reason, ST}, Row]),
                          []
                  end
          end, Rows),
    {selected, Res};
sql_query_format_res(Res, _SQLQuery) ->
    Res.

sql_query_to_iolist(SQLQuery) ->
    generic_sql_query_format(SQLQuery).

%% Generate the OTP callback return tuple depending on the driver result.
abort_on_driver_error({error, <<"query timed out">>} =
			  Reply,
		      From) ->
    (?GEN_FSM):reply(From, Reply),
    {stop, timeout, get(?STATE_KEY)};
abort_on_driver_error({error,
		       <<"Failed sending data on socket", _/binary>>} =
			  Reply,
		      From) ->
    (?GEN_FSM):reply(From, Reply),
    {stop, closed, get(?STATE_KEY)};
abort_on_driver_error(Reply, From) ->
    (?GEN_FSM):reply(From, Reply),
    {next_state, session_established, get(?STATE_KEY)}.


%% == Native PostgreSQL code

%% part of init/1
%% Open a database connection to PostgreSQL
pgsql_connect({Server, Port, DB, Username, Password}) ->
    case pgsql:connect([{host, Server},
                        {database, DB},
                        {user, Username},
                        {password, Password},
                        {port, Port},
                        {as_binary, true}]) of
        {ok, Ref} ->
            pgsql:squery(Ref, [<<"alter database ">>, DB, <<" set ">>,
                               <<"standard_conforming_strings='off';">>]),
            pgsql:squery(Ref, [<<"set standard_conforming_strings to 'off';">>]),
            {ok, Ref};
        Err ->
            Err
    end.

%% Convert PostgreSQL query result to Erlang ODBC result formalism
pgsql_to_odbc({ok, PGSQLResult}) ->
	?INFO("PG : ~p",[PGSQLResult]),
    case PGSQLResult of
      [Item] -> pgsql_item_to_odbc(Item);
      Items -> [pgsql_item_to_odbc(Item) || Item <- Items]
    end.

pgsql_item_to_odbc({<<"SELECT", _/binary>>, Rows,
		    Recs}) ->
    {selected, [element(1, Row) || Row <- Rows], Recs};
pgsql_item_to_odbc({<<"FETCH", _/binary>>, Rows,
		    Recs}) ->
    {selected, [element(1, Row) || Row <- Rows], Recs};
pgsql_item_to_odbc(<<"INSERT ", OIDN/binary>>) ->
    [_OID, N] = binary:split(OIDN, <<" ">>),
    {updated, binary_to_integer(N)};
pgsql_item_to_odbc(<<"DELETE ", N/binary>>) ->
    {updated, binary_to_integer(N)};
pgsql_item_to_odbc(<<"UPDATE ", N/binary>>) ->
    {updated, binary_to_integer(N)};
pgsql_item_to_odbc({error, Error}) -> {error, Error};
pgsql_item_to_odbc(_) -> {updated, undefined}.

pgsql_execute_to_odbc({ok, {<<"SELECT", _/binary>>, Rows}}) ->
    {selected, [], [[Field || {_, Field} <- Row] || Row <- Rows]};
pgsql_execute_to_odbc({ok, {'INSERT', N}}) ->
    {updated, N};
pgsql_execute_to_odbc({ok, {'DELETE', N}}) ->
    {updated, N};
pgsql_execute_to_odbc({ok, {'UPDATE', N}}) ->
    {updated, N};
pgsql_execute_to_odbc({error, Error}) -> {error, Error};
pgsql_execute_to_odbc(_) -> {updated, undefined}.


%% == Native MySQL code

%% part of init/1
%% Open a database connection to MySQL
mysql_connect({Server, Port, DB, Username, Password}) ->
    case p1_mysql_conn:start(Server, Port,Username,Password,DB, fun log/3)
	of
	{ok, Ref} ->
	    p1_mysql_conn:fetch(
		Ref, [<<"set names 'utf8mb4' collate 'utf8mb4_bin';">>], self()),
	    {ok, Ref};
	Err -> Err
    end.

%% Convert MySQL query result to Erlang ODBC result formalism
mysql_to_odbc({updated, MySQLRes}) ->
    {updated, p1_mysql:get_result_affected_rows(MySQLRes)};
mysql_to_odbc({data, MySQLRes}) ->
    mysql_item_to_odbc(p1_mysql:get_result_field_info(MySQLRes),
		       p1_mysql:get_result_rows(MySQLRes));
mysql_to_odbc({error, MySQLRes})
  when is_binary(MySQLRes) ->
    {error, MySQLRes};
mysql_to_odbc({error, MySQLRes})
  when is_list(MySQLRes) ->
    {error, list_to_binary(MySQLRes)};
mysql_to_odbc({error, MySQLRes}) ->
    {error, p1_mysql:get_result_reason(MySQLRes)};
mysql_to_odbc(ok) ->
    ok.


%% When tabular data is returned, convert it to the ODBC formalism
mysql_item_to_odbc(Columns, Recs) ->
    {selected, [element(2, Column) || Column <- Columns], Recs}.

get_db_version(#state{db_type = pgsql} = State) ->
    case pgsql:squery(State#state.db_ref,
                      <<"select current_setting('server_version_num')">>) of
        {ok, [{_, _, [[SVersion]]}]} ->
            case catch binary_to_integer(SVersion) of
                Version when is_integer(Version) ->
                    State#state{db_version = Version};
                Error ->
                    ?WARN("error getting pgsql version: ~p", [Error]),
                    State
            end;
        Res ->
            ?WARN("error getting pgsql version: ~p", [Res]),
            State
    end;
get_db_version(State) ->
    State.

log(Level, Format, Args) ->
    case Level of
      debug -> ?DEBUG(Format, Args);
      normal -> ?INFO(Format, Args);
      error -> ?ERROR(Format, Args)
    end.

check_error({error, Why} = Err, #sql_query{} = Query) ->
    ?ERROR("SQL query '~s' at ~p failed: ~p",
               [Query#sql_query.hash, Query#sql_query.loc, Why]),
    Err;
check_error({error, Why} = Err, Query) ->
    case catch iolist_to_binary(Query) of
        SQuery when is_binary(SQuery) ->
            ?ERROR("SQL query '~s' failed: ~p", [SQuery, Why]);
        _ ->
            ?ERROR("SQL query ~p failed: ~p", [Query, Why])
    end,
    Err;
check_error(Result, _Query) ->
    Result.

fsm_limit_opts() -> [].

max_fsm_queue() -> 5000.

opt(Key, Options, Default) ->
	case lists:keyfind(Key, 1, Options) of
		{Key, Value} -> Value;
		_ -> Default
	end.
