-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(N, 3).
-define(R, 2).
-define(W, 3).
-define(STATEBOX_EXPIRE, 60000).
-define(DEFAULT_TIMEOUT, 10000).


-type val() ::  statebox:statebox().

-record(spades_obj, {val    :: val(),
		    vclock :: vclock:vclock()}).

-type spades_obj() :: #spades_obj{} | not_found.

-type idx_node() :: {integer(), node()}.

-type vnode_reply() :: {idx_node(), spades_obj() | not_found}.
