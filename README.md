z_mysql
=====

An MySQL/PostgresSQL client lib ( copy from ejabberd )

Build
-----

    $ make

Config
-----

| Key 	| Value 	| Default 	|
|-----	|-------	|---------	|
|  sql_start_interval	|  timeout	|  30000	|
|  sql_keepalive_interval	|  timeout	|  30000	|



Example
-------
-compile({parse_transform, z_sql_pt}).

get_last(Pool, LUser) ->
    z_mysql:sql_query(Pool,
?SQL("select @(seconds)d, @(state)s from last"
    " where username=%(LUser)s")).

set_last_t(Pool, LUser, TimeStamp, Status) ->
    ?SQL_UPSERT(Pool, "last",
                ["!username=%(LUser)s",
                 "seconds=%(TimeStamp)d",
                 "state=%(Status)s"]).

del_last(Pool, LUser) ->
    z_mysql:sql_query(Pool,?SQL("delete from last where username=%(LUser)s")).