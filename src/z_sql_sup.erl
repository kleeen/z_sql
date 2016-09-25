%%%----------------------------------------------------------------------
%%% File    : ejabberd_sql_sup.erl
%%% Author  : Alexey Shchepin <alexey@process-one.net>
%%% Purpose : SQL connections supervisor
%%% Created : 22 Dec 2004 by Alexey Shchepin <alexey@process-one.net>
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

%%% @doc
%%% Support only MySQL, PostgreSQL

-module(z_sql_sup).

-author('alexey@process-one.net').

-export([start_link/0,
		 start_pool/2,
		 start_pool/3,
		 init/1]).

-include("logger.hrl").

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
	{ok,{#{strategy => one_for_one,intensity => 1, period => 5},[]}}.

start_pool(PoolName, Args) -> start_pool(PoolName, Args, []).
start_pool(PoolName, Args, Opts) ->
	PoolMgr = #{id => {z_sql_pool, PoolName}, start => {z_sql_pool,start_link,[PoolName,Args, Opts]}, restart => permanent,
				shutdown => 2000, type => worker, modules => [z_sql_pool]},
	supervisor:start_child(?MODULE, PoolMgr).
