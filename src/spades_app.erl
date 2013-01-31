-module(spades_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case spades_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, spades_vnode}]),
            
            ok = riak_core_ring_events:add_guarded_handler(spades_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(spades_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(spades, self()),

            EntryRoute = {["spades", "ping"], spades_wm_ping, []},
            webmachine_router:add_route(EntryRoute),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
