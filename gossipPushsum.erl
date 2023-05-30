-module(gossipPushsum).
-export([main/3, launchNodes/1, launchGossipAlgorithm/2]).

% Main function that takes the number of nodes, topology, and algorithm as input.
main(Nodes, Topology, Algorithm) ->
    %cmd+/
    % if Topology == twoD ->    % Calculate the number of nodes for a 2D topology
    %        NumNodes = squareNodes(Nodes); 
    %    Topology == imp3D ->    % Calculate the number of nodes for an imp3D topology
    %        NumNodes = squareNodes(Nodes);    
    %    true ->
    %        NumNodes = Nodes
    % end,

    io:format("Number of Nodes to be used : ~p~n", [NumNodes]),
    io:format("Topology to be used : ~p~n", [Topology]),
    io:format("Algorithm followed: ~p~n", [Algorithm]),

    if Algorithm == gossip ->
           launchGossipAlgorithm(NumNodes, Topology); % Launch the Gossip Algorithm
       %true ->
         %  launchPushsumAlgorithm(NumNodes, Topology)
    %end.

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
%launchPushsumAlgorithm(NumNodes, Topology) ->

createNodes(NumNodes) ->
    Nodes =
        [{NodeId, spawn(actor, launchNodes, [NodeId])} || NodeId <- lists:seq(1, NumNodes)],
    Nodes.

launchNodes(NodeId) ->
    gossipResponseWait(NodeId, 0).
isNodeAliveCheck(Nodes) ->
    ActiveNodes =
        [{Node, NodeId} || {Node, NodeId} <- Nodes, is_process_alive(NodeId) == true],

    if ActiveNodes == [] ->
           io:format("~nSuccessfully Converged.~n");
       true ->
           isNodeAliveCheck(Nodes)
    end.