-module(gossipPushsum).
-import(math, []).
-export([main/3, launchNodes/1, launchNodesForPushSum/2, launchGossipAlgorithm/2,
         propagateRumor/5]).
        
% Main function that takes the number of nodes, topology, and algorithm as input.
main(Nodes, Topology, Algorithm) ->
    if Topology == twoD ->
           NumNodes = squareNodes(Nodes);   % Calculate the number of nodes for a 2D topology
       Topology == imp3D ->
           NumNodes = squareNodes(Nodes);    % Calculate the number of nodes for an imp3D topology
       true ->
           NumNodes = Nodes
    end,

    io:format("Number of Nodes to be used : ~p~n", [NumNodes]),
    io:format("The topology to be used: ~p~n", [Topology]),
    io:format("Algorithm followed: ~p~n", [Algorithm]),

    if Algorithm == gossip ->
           launchGossipAlgorithm(NumNodes, Topology);    % Launch Gossip algorithm with number of nodes and selected topology as parameter
       true ->
           launchPushsumAlgorithm(NumNodes, Topology)     % Launch Pushsum algorithm with number of nodes and selected topology as parameter
    end.

% Creates Nodes in parallel by invoking actors for gossip algorithm
    createNodes(NumNodes) ->
    Nodes =
        [{NodeId, spawn(actor, launchNodes, [NodeId])} || NodeId <- lists:seq(1, NumNodes)],
    Nodes.

% Launching Gossip Algorithm to spread rumor
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

% checking the lifeline of the nodes in action
isNodeAliveCheck(Nodes) ->
    ActiveNodes =
        [{Node, NodeId} || {Node, NodeId} <- Nodes, is_process_alive(NodeId) == true],

    if ActiveNodes == [] ->
           io:format("~nSuccessfully Converged.~n");
       true ->
           isNodeAliveCheck(Nodes)
    end.

% finding out the nodes that are alive to spread rumor
fetchActiveNodes(Nodes) ->
    ActiveNodes =
        [{Node, NodeId} || {Node, NodeId} <- Nodes, is_process_alive(NodeId) == true],
    ActiveNodes.

    % Launching the nodes if rumor is not spread to 10 actors
launchNodes(NodeId) ->
    gossipResponseWait(NodeId, 0).

% fetching the response to get the count of nodes hearing the rumor
gossipResponseWait(NodeId, Count) ->
    receive
        {From, {Topology, Nodes, NumNodes}} ->
            if Count == 10 ->
                   exit(0);
               true ->
                   spawn(actor, propagateRumor, [self(), Topology, Nodes, NumNodes, NodeId]),
                   gossipResponseWait(NodeId, Count + 1)
            end
    end.

% routine to spread the rumor
propagateRumor(Self, Topology, Nodes, NumNodes, NodeId) ->
    IsNodeActive = is_process_alive(Self),
    if IsNodeActive == true ->
           ActiveNodes = fetchActiveNodes(Nodes),
           Neighbors = generateTopologyNetwork(Topology, ActiveNodes, NumNodes, NodeId),
           if Neighbors == [] ->
                  exit(0);
              true ->
                  {_, SelectedNeighborID} =
                      lists:nth(
                          rand:uniform(length(Neighbors)), Neighbors),
                  SelectedNeighborID ! {Self, {Topology, Nodes, NumNodes}},
                  propagateRumor(Self, Topology, Nodes, NumNodes, NodeId)
           end;
       true ->
           exit(0)
    end.

% Creates Nodes in parallel by invoking actors for pushsum algorithm
createNodesForPushSum(NumNodes, W) ->
    Nodes =
        [{Id, spawn(actor, launchNodesForPushSum, [Id, W])} || Id <- lists:seq(1, NumNodes)],
    Nodes.

% Launching the nodes if rumor is not spread to 10 actors for pushsum algorithm
launchNodesForPushSum(NodeId, W) ->
    pushSumResponseWait(NodeId, NodeId, W, 0, 0).

% fetching the response to get the count of nodes hearing the rumor for pushsum algorithm
pushSumResponseWait(NodeId, S, W, PreviousRatio, Count) ->
    receive
        {_, {S1, W1, Topology, Nodes, NumNodes, Main}} ->
            if Count == 3 ->
                   Main ! {self(), {ok, NodeId, Count}};
               true ->
                   % Upon receiving this the node should add the received pair to its own corresponding values
                   S2 = S + S1,
                   W2 = W + W1,

                   % Upon receiving, each node selects a random neighbor and sends it a message.
                   ActiveNodes = fetchActiveNodes(Nodes),
                   Neighbors = generateTopologyNetwork(Topology, ActiveNodes, NumNodes, NodeId),

                   if Neighbors == [] ->
                          exit(0);
                      true ->
                          {_, SelectedNeighborsId} =
                              lists:nth(
                                  rand:uniform(length(Neighbors)), Neighbors),

                          % SEND: When sending a message to another actor, half of s and w is kept by the sending actor and
                          % half is placed in the message
                          S3 = S2 / 2,
                          W3 = W2 / 2,

                          SelectedNeighborsId ! {self(), {S3, W3, Topology, Nodes, NumNodes, Main}},

                          CurrentRatio = S / W,
                          Difference = math:pow(10, -10),
                          if abs(CurrentRatio - PreviousRatio) < Difference ->
                                 pushSumResponseWait(NodeId, S3, W3, CurrentRatio, Count + 1);
                             true ->
                                 pushSumResponseWait(NodeId, S3, W3, CurrentRatio, 0)
                          end
                   end
            end
    end.

% Startup routine for pushsum algorithm
launchPushsumAlgorithm(NumNodes, Topology) ->
    io:format("Launching the Pushsum Algorithm for ~s topology with ~p nodes ~n",
              [Topology, NumNodes]),

    W = 1,
    Nodes = createNodesForPushSum(NumNodes, W),
    {SelectedActor, SelectedActorId} =
        lists:nth(
            rand:uniform(length(Nodes)), Nodes),
    io:format("~nSelected node for propagating is : ~p ~n", [SelectedActor]),
    StartTime = erlang:timestamp(),
    SelectedActorId ! {self(), {0, 0, Topology, Nodes, NumNodes, self()}},
    receive
        {_, {ok, SubNodeId, SubNodeCount}} ->
            io:format("~nSuccessfully converged || node ~p converged with ~p node "
                      ".~n",
                      [SubNodeId, SubNodeCount])
    end,
    EndTime = erlang:timestamp(),
    io:format("~nTotal time taken for convergence: ~p MilliSeconds~n",
              [timer:now_diff(EndTime, StartTime) / 1000]).

% routine to setup topology network
generateTopologyNetwork(Topology, Nodes, NumNodes, Id) ->
    ListNodes = maps:from_list(Nodes),
    case Topology of
        full ->
            createFullTopologyNetwork(Id, NumNodes, ListNodes);
        twoD ->
            create2DTopologyNetwork(Id, NumNodes, ListNodes);
        line ->
            createLineTopologyNetwork(Id, NumNodes, ListNodes);
        imp3D ->
            createImp3DTolpologyNetwork(Id, NumNodes, ListNodes)
    end.

createFullTopologyNetwork(SelectedNodeId, NumNodes, NodesMap) ->
    % Remove self() from list and send all other nodes
    NeighborList =
        lists:subtract(
            lists:seq(1, NumNodes), [SelectedNodeId]),
    FullNeighborsList =
        [{NumNodes, maps:get(NumNodes, NodesMap)}
         || NumNodes <- NeighborList, maps:is_key(NumNodes, NodesMap)],
    FullNeighborsList.

create2DTopologyNetwork(SelectedNodeId, NumNodes, NodesMap) ->
    % Assumed that 2D grid is always a perfect square.
    Rows =
        erlang:trunc(
            math:sqrt(NumNodes)),
    ModifiedRow = SelectedNodeId rem Rows,
    if ModifiedRow == 1 ->
           NeighborRow1 = [SelectedNodeId + 1];
       ModifiedRow == 0 ->
           NeighborRow1 = [SelectedNodeId - 1];
       true ->
           NeighborRow1 = lists:append([[SelectedNodeId - 1], [SelectedNodeId + 1]])
    end,

    if SelectedNodeId + Rows > NumNodes ->
           NeighborRow2 = NeighborRow1;
       true ->
           NeighborRow2 = lists:append([NeighborRow1, [SelectedNodeId + Rows]])
    end,
    if SelectedNodeId - Rows < 1 ->
           NeighborRow3 = NeighborRow2;
       true ->
           NeighborRow3 = lists:append([NeighborRow2, [SelectedNodeId - Rows]])
    end,

    FullNeighborsList =
        [{NumNodes, maps:get(NumNodes, NodesMap)}
         || NumNodes <- NeighborRow3, maps:is_key(NumNodes, NodesMap)],
    FullNeighborsList.

createLineTopologyNetwork(SelectedNodeId, NumNodes, NodeMap) ->
    if SelectedNodeId > NumNodes ->
           NeighborNodes = [];
       SelectedNodeId < 1 ->
           NeighborNodes = [];
       SelectedNodeId + 1 > NumNodes ->
           if SelectedNodeId - 1 < 1 ->
                  NeighborNodes = [];
              true ->
                  NeighborNodes = [SelectedNodeId - 1]
           end;
       true ->
           if SelectedNodeId - 1 < 1 ->
                  NeighborNodes = [SelectedNodeId + 1];
              true ->
                  NeighborNodes = [SelectedNodeId - 1, SelectedNodeId + 1]
           end
    end,
    FullNeighborsList =
        [{NumNodes, maps:get(NumNodes, NodeMap)}
         || NumNodes <- NeighborNodes, maps:is_key(NumNodes, NodeMap)],
    FullNeighborsList.

createImp3DTolpologyNetwork(SelectedNodeId, NumNode, NodeMap) ->
    Rows =
        erlang:trunc(
            math:sqrt(NumNode)),
    ModifiedRows = SelectedNodeId rem Rows,

    if ModifiedRows == 1 ->
           NeighborRow1 = [SelectedNodeId + 1];
       ModifiedRows == 0 ->
           NeighborRow1 = [SelectedNodeId - 1];
       true ->
           NeighborRow1 = lists:append([[SelectedNodeId - 1], [SelectedNodeId + 1]])
    end,

    if SelectedNodeId + Rows > NumNode ->
           NeighborRow2 = NeighborRow1;
       true ->
           NeighborRow2 = lists:append([NeighborRow1, [SelectedNodeId + Rows]])
    end,
    if SelectedNodeId - Rows < 1 ->
           AdjacentNeighbors = NeighborRow2;
       true ->
           AdjacentNeighbors = lists:append([NeighborRow2, [SelectedNodeId - Rows]])
    end,

    NeighborsToBeIgnored = lists:append([AdjacentNeighbors, [SelectedNodeId]]),
    RemainingNeighbors =
        lists:subtract(
            lists:seq(1, NumNode), NeighborsToBeIgnored),
    RandomRemaningNeighbor =
        lists:nth(
            rand:uniform(length(RemainingNeighbors)), RemainingNeighbors),
    RandomAdjacentNeighbor =
        lists:nth(
            rand:uniform(length(AdjacentNeighbors)), AdjacentNeighbors),
    FinalNeighborsList = lists:append([[RandomRemaningNeighbor], [RandomAdjacentNeighbor]]),
    FullNeighborsList =
        [{NumNode, maps:get(NumNode, NodeMap)}
         || NumNode <- FinalNeighborsList, maps:is_key(NumNode, NodeMap)],
    FullNeighborsList.

squareNodes(NumNodes) ->
    NewNodesNum =
        round(math:pow(
                  math:ceil(
                      math:sqrt(NumNodes)),
                  2)),
    NewNodesNum.