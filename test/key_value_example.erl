-module(key_value_example).
-behaviour(gen_server).
-behaviour(riak_core_vnode).

-export([start_link/0,
         start/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         handle_command/3,
         handle_handoff_command/3,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4]).

-export([ping/0,
        ring_status/0,
        put/2,
        get/1,
        delete/1,
        keys/0,
        values/0,
        clear/0,
        nodes/0,
        hash_key/1]).

% two processes managed by this module, 1 for each behaviour => 2 state records
-record(state, {vmaster}).
-record(vnode, {partition, data=#{}}).

-include_lib("riak_core/include/riak_core_vnode.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, #{}, []).

ping() ->
    sync_command(os:timestamp(), ping).

ring_status() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:pretty_print(Ring, [legend]).

put(Key, Value) ->
    sync_command(Key, {put, Key, Value}).

get(Key) ->
    sync_command(Key, {get, Key}).

delete(Key) ->
    sync_command(Key, {delete, Key}).

keys() ->
    coverage_to_list(coverage_command(keys)).

values() ->
    coverage_to_list(coverage_command(values)).

clear() ->
    {ok, #{list := []}} = coverage_to_list(coverage_command(clear)),
    ok.

nodes() ->
    riak_core_node_watcher:nodes(key_value_example).

hash_key(Key) ->
    riak_core_util:chash_key({<<"key_value_example">>, term_to_binary(Key)}).

sync_command(Key, Command)->
    DocIdx = hash_key(Key),
    PrefList = riak_core_apl:get_apl(DocIdx, 1, key_value_example),
    [IndexNode] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, Command, key_value_example_master).

coverage_command(Command) ->
    Timeout = 5000,
    riak_core_coverage_fold:run_link(fun(Partition, Node, Data, Acc) ->
                                             [{Partition, Node, Data}|Acc]
                                     end, [], {key_value_example_master, key_value_example, Command}, Timeout).

coverage_to_list({ok, CoverageResult}) ->
    {ok, lists:foldl(
      fun({_Partition, _Node, L}, Acc=#{list := List, from := C, n := T}) when L =/= [] ->
              Acc#{list => List ++ L, from => C+1, n => T+1};
          ({_Partition, _Node, []}, Acc=#{n := T}) ->
              Acc#{n => T+1}
      end, #{list => [], from => 0, n => 0}, CoverageResult)};
coverage_to_list(Error) ->
    Error.

% we mix behaviours in this module, init/1 is the only collision
init(#{}) ->
    ok = riak_core:register([{vnode_module, ?MODULE}]),
    ok = riak_core_node_watcher:service_up(key_value_example, self()),

    % Note: registers self as ?MODULE_STRING ++ "_master",
    {ok, VMaster} = riak_core_vnode_master:start_link(?MODULE),

    {ok, #state{vmaster=VMaster}};
init([Partition]) ->
    {ok, #vnode{partition=Partition}}.

handle_command({put, Key, Value}, _Sender, VNode = #vnode{data = Data}) ->
    log("PUT ~p:~p", [Key, Value], VNode),
    NewData = Data#{Key => Value},
    {reply, ok, VNode#vnode{data = NewData}};

handle_command({get, Key}, _Sender, VNode = #vnode{data = Data}) ->
    log("GET ~p", [Key], VNode),
    {reply, maps:get(Key, Data, not_found), VNode};

handle_command({delete, Key}, _Sender, VNode = #vnode{data = Data}) ->
    log("DELETE ~p", [Key], VNode),
    NewData = maps:remove(Key, Data),
    {reply, maps:get(Key, Data, not_found), VNode#vnode{data = NewData}};

handle_command(ping, _Sender, VNode = #vnode{partition = Partition}) ->
    log("Received ping command ~p", [Partition], VNode),
    {reply, {pong, Partition}, VNode};

handle_command(Message, _Sender, VNode) ->
    log("unhandled_command ~p", [Message], VNode),
    {noreply, VNode}.

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender,
                       VNode = #vnode{data = Data}) ->
    [ log("Received fold request for handoff", VNode) || maps:size(Data) > 0 ],
    Result = maps:fold(FoldFun, Acc0, Data),
    {reply, Result, VNode};
handle_handoff_command({get, Key}, Sender, VNode) ->
    %%  if the command is read (a get), we reply with our local copy of the
    %%  data (we know it's up to date because we applied all the writes locally)
    log("GET during handoff, handling locally ~p", [Key], VNode),
    handle_command({get, Key}, Sender, VNode);
handle_handoff_command(Message, Sender, VNode) ->
    %% when the command is a write (a put or a delete), we change our local
    %% copy of the code and we forward it to the receiving vnode (that way,
    %% if it was already migrated, the change is applied in that copy too)
    log("~p during handoff, handling locally and forwarding", [Message], VNode),
    {reply, _Result, NewVNode} = handle_command(Message, Sender, VNode),
    {forward, NewVNode}.

handle_handoff_data(BinData, VNode=#vnode{data = Data}) ->
    {Key, Value} = binary_to_term(BinData),
    log("received handoff data ~p", [{Key, Value}], VNode),
    NewData = Data#{Key => Value},
    {reply, ok, VNode#vnode{data = NewData}}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

handle_coverage(keys, _KeySpaces, {_, ReqId, _}, VNode = #vnode{data = Data}) ->
    log("Received keys coverage", VNode),
    Keys = maps:keys(Data),
    {reply, {ReqId, Keys}, VNode};
handle_coverage(values, _KeySpaces, {_, ReqId, _}, VNode = #vnode{data = Data}) ->
    log("Received values coverage", VNode),
    Values = maps:values(Data),
    {reply, {ReqId, Values}, VNode};
handle_coverage(clear, _KeySpaces, {_, ReqId, _}, VNode) ->
    log("Received clear coverage", VNode),
    NewVNode = VNode#vnode{data = #{}},
    {reply, {ReqId, []}, NewVNode}.

log(String, State) ->
  log(String, [], State).

log(String, Args, #vnode{partition = Partition}) ->
  String2 = "[~.36B] " ++ String,
  Args2 = [Partition | Args],
  %?debugFmt(String2, Args2),
  io:format(String2, Args2),
  ok.

handle_call(_Call, _From, _State) ->
    erlang:error(function_clause).

handle_cast(_Cast, _State) ->
    erlang:error(function_clause).

handle_info(_Info, State) ->
    {noreply, State}.
