-module(gossip_pushsum).
-export([main/3, launchNodesForPushSum/2, launchGossipAlgorithm/2].

main(Nodes, Topology, Algorithm) ->
    %cmd+/
    % if Topology == twoD ->
    %        NumNodes = squareNodes(Nodes);
    %    Topology == imp3D ->
    %        NumNodes = squareNodes(Nodes);
    %    true ->
    %        NumNodes = Nodes
    % end,

    io:format("Number of Nodes to be used : ~p~n", [NumNodes]),
    io:format("Topology to be used : ~p~n", [Topology]),
    io:format("Algorithm followed: ~p~n", [Algorithm]),

    if Algorithm == gossip ->
           launchGossipAlgorithm(NumNodes, Topology);
       true ->
           launchPushsumAlgorithm(NumNodes, Topology)
    end.

launchGossipAlgorithm(NumNodes, Topology) ->
    io:format("~nLaunching the Gossip Algorithm for ~s topology with ~p nodes ~n",
              [Topology, NumNodes]),
    Nodes = createNodes(NumNodes),
    {SelectedNode, SelectedNodeId} =
        lists:nth(
            rand:uniform(length(Nodes)), Nodes),
    io:format("~nRandomly Selected node for propagating rumor is : ~p ~n~n", [SelectedNode]),
    StartTime = erlang:timestamp(),
    SelectedNodeId ! {self(), {Topology, Nodes, NumNodes}},
    isNodeAliveCheck(Nodes),
    EndTime = erlang:timestamp(),
    io:format("~nTotal time taken for convergence ~p MilliSeconds~n",
              [timer:now_diff(EndTime, StartTime) / 1000]).

% implementation of pushsum algorithm
launchPushsumAlgorithm(NumNodes, Topology) ->.