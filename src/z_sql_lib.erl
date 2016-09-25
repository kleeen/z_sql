%%%----------------------------------------------------------------------
%%% File    : sql_queries.erl
%%% Author  : Mickael Remond <mremond@process-one.net>
%%% Purpose : ODBC queries dependind on back-end
%%% Created : by Mickael Remond <mremond@process-one.net>
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

-module(z_sql_lib).

%% -compile({parse_transform, z_sql_pt}).

-author("mremond@process-one.net").

-export([update/5,
		update_t/4,
	 	sql_transaction/2,
	 	escape/1
]).

-include("logger.hrl").
-include("z_sql_pt.hrl").

%% Almost a copy of string:join/2.
%% We use this version because string:join/2 is relatively
%% new function (introduced in R12B-0).
join([], _Sep) -> [];
join([H | T], Sep) -> [H, [[Sep, X] || X <- T]].

%% Safe atomic update.
update_t(Table, Fields, Vals, Where) ->
    UPairs = lists:zipwith(fun (A, B) ->
				   <<A/binary, "='", B/binary, "'">>
			   end,
			   Fields, Vals),
    case z_sql:sql_query_t([<<"update ">>, Table,
	<<" set ">>, join(UPairs, <<", ">>),
	<<" where ">>, Where, <<";">>])
	of
      {updated, 1} -> ok;
      _ ->
		Res = z_sql:sql_query_t([<<"insert into ">>, Table,
		<<"(">>, join(Fields, <<", ">>),
		<<") values ('">>, join(Vals, <<"', '">>),
		<<"');">>]),
		case Res of
			{updated,1} -> ok;
			_ -> Res
		end
    end.

update(LServer, Table, Fields, Vals, Where) ->
    UPairs = lists:zipwith(fun (A, B) ->
				   <<A/binary, "='", B/binary, "'">>
			   end,
			   Fields, Vals),
    case z_sql:sql_query(LServer,
[<<"update ">>, Table, <<" set ">>,
	join(UPairs, <<", ">>), <<" where ">>, Where,
	<<";">>])
	of
      {updated, 1} -> ok;
      _ ->
		Res = z_sql:sql_query(LServer,
[<<"insert into ">>, Table, <<"(">>,
join(Fields, <<", ">>), <<") values ('">>,
join(Vals, <<"', '">>), <<"');">>]),
		case Res of
			{updated,1} -> ok;
			_ -> Res
		end		   
    end.

%% F can be either a fun or a list of queries
%% TODO: We should probably move the list of queries transaction
%% wrapper from the ejabberd_sql module to this one (sql_queries)
sql_transaction(LServer, F) ->
    z_sql:sql_transaction(LServer, F).

%% Characters to escape
escape($\000) -> <<"\\0">>;
escape($\n) -> <<"\\n">>;
escape($\t) -> <<"\\t">>;
escape($\b) -> <<"\\b">>;
escape($\r) -> <<"\\r">>;
escape($') -> <<"''">>;
escape($") -> <<"\\\"">>;
escape($\\) -> <<"\\\\">>;
escape(C) -> <<C>>.
