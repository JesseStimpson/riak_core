-module(riak_core_coverage_fold).

-behaviour(riak_core_coverage_fsm).

-export([run_link/4,
         init/2,
         process_results/2,
         finish/2]).

-record(state, {req_id, from, request, 'fun', acc}).

%% @doc Run a simple coverage with a folding accumulator over all vnodes
%%
%% Third argument is a tuple with
%% {VNodeMaster,       -- defines the vnodes to be iterated on
%%  NodeCheckService,  -- defines the physical nodes from which to gather vnodes
%%  Command}           -- defines the coverage command that will generate items
%%
%% This ordering corresponds to the order of arguments to the fold function:
%% fun(Partition,      -- vnode id
%%     Node,           -- physical node id
%%     X,              -- data item from coverage command
%%     Acc) ->         -- standard Erlang accumulator
%%   [X|Acc]
%% end
run_link(Fun, Acc0, {VNodeMaster, NodeCheckService, Command}, Timeout) ->
    ReqId = erlang:phash2(make_ref()), % must be an int for now
    {ok, _Process} = start_link(VNodeMaster, NodeCheckService, ReqId, self(), Command, Fun, Acc0, Timeout),
    receive
        {ReqId, AccX} ->
            AccX
    after Timeout ->
              exit(timeout)
    end.

start_link(VNodeMaster, NodeCheckService, ReqId, ClientPid, Request, Fun, Acc0, Timeout) ->
    riak_core_coverage_fsm:start_link(?MODULE, {pid, ReqId, ClientPid},
                                      [VNodeMaster, NodeCheckService, Request, Fun, Acc0, Timeout]).

init({pid, ReqId, ClientPid}, [VNodeMaster, NodeCheckService, Request, Fun, Acc0, Timeout]) ->
    State = #state{req_id = ReqId,
              from = ClientPid,
              request = Request,
              'fun' = Fun,
              acc = Acc0},

    {Request,          %% An opaque data structure representing the command to be handled by the vnodes (in handle_coverage)
     allup,            %% An atom that specifies whether we want to run the command in all vnodes (all) or only those reachable (allup)
     1,                %% ReplicationFactor, used to accurately create a minimal covering set of vnodes
     1,                %% PrimaryVNodeCoverage, the number of primary vnodes from the preflist to use in creating the coverage plan, typically 1
     NodeCheckService, %% NodeCheckService, same as the atom passed to the node_watcher in application startup
     VNodeMaster,      %% VNodeMaster, the atom to use to reach the vnode master module
     Timeout,          %% Timeout for the coverage request
     State}.           %% Initial state for this fsm

process_results({{_ReqId, {Partition, Node}}, Data}, State = #state{'fun' = Fun, acc = Acc}) ->
    %% return 'done' if the vnode has given all its data. Otherwise, returning 'ok' will allow future data to be delivered,
    %% or 'error' will abort the coverage command (with a call to finish)
    {done, State#state{acc = Fun(Partition, Node, Data, Acc)}}.

finish(clean, State = #state{req_id = ReqId, from = From, acc = Acc}) ->
    %% send the result back to the caller
    From ! {ReqId, {ok, Acc}},
    {stop, normal, State};

finish({error, Reason}, State = #state{req_id = ReqId, from = From, acc = Acc}) ->
    From ! {ReqId, {partial, Reason, Acc}},
    {stop, normal, State}.
