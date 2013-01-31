-module(spades).
-include("spades.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
	 get/1,
	 set/2,
	 list/0
        ]).

-ignore_xref([
              ping/0,
              get/1,
	      set/2,
	      list/0
             ]).

%% Public API

%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, spades),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, spades_vnode_master).

get(Key) ->
    spades_entity_read_fsm:start(
      {
       spades_vnode, 
       spades
      },
      get, Key
     ).

list() ->
    spades_entity_coverage_fsm:start(
      {
       spades_vnode, 
       spades
      },
      list
     ).


set(Key, Value) ->
    spades_entity_write_fsm:write(
      { 
       spades_vnode, 
       spades
      }, Key, set, Value).
