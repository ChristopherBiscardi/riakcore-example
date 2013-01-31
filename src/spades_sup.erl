-module(spades_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = { spades_vnode_master,
                  {riak_core_vnode_master, start_link, [spades_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    WriteFSMs = { spades_entity_write_fsm_sup,
		{ spades_entity_write_fsm_sup, start_link, []},
		permanent, infinity, supervisor, [spades_entity_write_fsm_sup]},

    CoverageFSMs = { spades_entity_coverage_fsm_sup,
		     { spades_entity_coverage_fsm_sup, start_link, []},
		    permanent, infinity, supervisor, [spades_entity_coverage_fsm_sup]},
    
    ReadFSMs = {
      spades_entity_read_fsm_sup,
      {
	spades_entity_read_fsm_sup, start_link, []},
      permanent, infinity, supervisor, [spades_entity_read_fsm_sup]},
    
    { ok,
        { {one_for_one, 5, 10},
          [VMaster, WriteFSMs, ReadFSMs, CoverageFSMs]}}.
