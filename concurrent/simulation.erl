-module(simulation).
-export([run/1, 
    supervisor_wait_for_election_to_finish/3,
    node_start/1, 
    node_wait_for_election_kickoff_message/1, 
    node_handle_election_messages/6,
    supervisor_wait_for_revolt/3]).

-record(node_info, {node_id, machine, name, priority, tolerance}).
-record(probe, {prober_id, priority, leader_flag=true, ttl=1, direction, election_time}).
-record(reply, {prober_id, leader_flag, direction, election_time}).
-record(node_state, {info = #node_info{}, supervisor, elected_earlier = false, 
    revolt_threshold = 0, current_election_time}).
-record(leader, {id, pid, elected_on}).

run(Config) -> 
    {ok, Log} = file:open("output.txt", [write]),
    erlang:group_leader(Log, self()),
    NodeConfigs = lists:map(fun supervisor_convert_to_node_state/1, parser:read(Config)),
    supervisor_kickoff_simulation(NodeConfigs, Log).
    
supervisor_kickoff_simulation(NodeConfigs, _) 
    when length(NodeConfigs) == 0 ->
    io:format("I did expect someone to run this with 1 node, but with no nodes? Shocking Sir! Shocking!~n");

supervisor_kickoff_simulation(NodeConfigs, _) 
    when length(NodeConfigs) == 1 ->
    Insults = ["Your code is so bad, Richard Stallman suggested that you keep it proprietary.",
    "Your code runs so slow your data brings sleeping bags to camp-out in the cache lines.",
    "Your code is so bad your child processes disowned you.",
    "Your code looks as though you have been playing bingo with anti-patterns.",
    "I never believed in chaos theory until I saw your variable naming convention!",
    "The best stairs you ever drew was done by using thread profiler.",
    "I've never seen a priest code before, I mean, you must be a priest right? Your code is running on pure faith and no logic..."],
    io:format("Seriously Mate? You're trying to run simulation on 1 node?~n~n\t~p~n",
        [lists:nth(rand:uniform(length(Insults)), Insults)]),
    exit(normal);

supervisor_kickoff_simulation(NodeConfigs, Log) ->
    %io:format("Starting...~n", []),
    Nodes = lists:map(fun supervisor_boot_node/1, NodeConfigs),
    lists:foreach(fun({PID, _}) -> erlang:group_leader(Log, PID) end, Nodes),
    supervisor_kickoff_election(1, Nodes, 0),
    %io:format("Waiting for election to finish...~n", []),
    supervisor_wait_for_election_to_finish(Nodes, 0, []).

supervisor_wait_for_election_to_finish(Nodes, ElectionTime, PastLeaders) -> 
    receive
        {election_done, LeaderState, Pid, MsgTime} -> 
            case ElectionTime == MsgTime of
                %ignore message if false
                false -> supervisor_wait_for_election_to_finish(Nodes, ElectionTime, PastLeaders);
                true -> 
                    io:format("ID=~p became leader at t=~p~n", 
                        [LeaderState#node_state.info#node_info.node_id, ElectionTime]),
                    LeaderId = LeaderState#node_state.info#node_info.node_id,
                    Leader = #leader{id = LeaderId, pid = Pid, elected_on = ElectionTime},
                    RevoltThreshold = trunc( (length(Nodes) + 1)/2),
                    supervisor_notify_election_results(Nodes, Leader, RevoltThreshold, PastLeaders)
        end
    end.


supervisor_notify_election_results_to_nodes([], _, _) ->
    ok;

supervisor_notify_election_results_to_nodes([{PID, _} | Tail], Leader, RevoltThreshold) ->
    PID ! {leader_elected, Leader, RevoltThreshold},
    supervisor_notify_election_results_to_nodes(Tail, Leader, RevoltThreshold).

supervisor_notify_election_results(Nodes, Leader, RevoltThreshold, PastLeaders) ->
    supervisor_notify_election_results_to_nodes(Nodes, Leader, RevoltThreshold),
    supervisor_wait_for_revolt(Nodes, Leader, PastLeaders).

supervisor_wait_for_revolt(Nodes, Leader, PastLeaders) ->
    receive 
        {leader_deposed, DepositionTime} ->
            io:format("ID=~p was deposed at t=~p~n", [Leader#leader.id, DepositionTime]),
            UpdatedPastLeaders = lists:append(PastLeaders, [Leader]),
            %io:format("Nodes: ~p, PastLeaders: ~p. Nodes: ~p, PastLeaders:~p ~n",
            %    [length(Nodes), length(UpdatedPastLeaders), Nodes, UpdatedPastLeaders]),
            case length(UpdatedPastLeaders) == length(Nodes) of
                    true -> 
                        io:format("End of simulation~n"),
                        exit(normal);
                    false ->
                        % start new election
                        %io:format("supervisor: Start new election ~n"),
                        supervisor_kickoff_election(1, Nodes, DepositionTime+1),
                        supervisor_wait_for_election_to_finish(Nodes, DepositionTime+1, UpdatedPastLeaders)
            end
    end.


supervisor_convert_to_node_state({Id, Machine, Name, Priority, Tolerance}) ->
    #node_info{node_id = Id, machine = Machine, name = Name, priority = Priority, tolerance = Tolerance}.

supervisor_boot_node(NodeInfo) -> 
    NodeState = #node_state{info = NodeInfo, supervisor = self()},
    PID = spawn_link(?MODULE, node_start, [NodeState]),
    %io:format("boot_node: spawned:~p = ~p~n", [PID, NodeInfo]),
    {PID, NodeInfo}.


supervisor_get_left_neighbor(Index, Nodes) when Index == 1 ->
    {LeftPID, _} = lists:nth(length(Nodes), Nodes),
    LeftPID;

supervisor_get_left_neighbor(Index, Nodes) when Index > 1 ->
    {LeftPID, _} = lists:nth(Index-1, Nodes),
    LeftPID.

supervisor_get_right_neighbor(Index, Nodes) when Index == length(Nodes) ->
    {RightPID, _} = lists:nth(1, Nodes),
    RightPID;

supervisor_get_right_neighbor(Index, Nodes) when Index < length(Nodes) ->
    {RightPID, _} = lists:nth(Index+1, Nodes),
    RightPID.
        

supervisor_kickoff_election(Index, Nodes, Time) when Index >= 1 andalso Index =< length(Nodes) ->
    {CurrentPID, _} = lists:nth(Index, Nodes),
    CurrentPID ! {start_election, 
        supervisor_get_left_neighbor(Index, Nodes), 
        supervisor_get_right_neighbor(Index, Nodes),
        Time},
    supervisor_kickoff_election(Index+1, Nodes, Time);

supervisor_kickoff_election(_, _, _) ->
    ok.




% ---- Node Functions -----

node_start(State) ->
    process_flag(trap_exit, true),
    node_wait_for_election_kickoff_message(State).
    
node_wait_for_election_kickoff_message(State) ->
    %io:format("node waiting for start_election~n"),
    receive
        {start_election, LeftPID, RightPID, Time} ->
            %io:format("~p(~p): start_election ~p, ~p, ~p~n", 
            %    [State#node_state.info#node_info.name, self(), LeftPID, RightPID, Time]),
            UpdatedState = State#node_state{current_election_time = Time},
            node_start_election(LeftPID, RightPID, UpdatedState);
        {'EXIT', _, _} ->
            %io:format("~p exiting, got ~p~n", [State#node_state.info#node_info.name, {'EXIT', Reason, From}]),
            exit(normal)
    end.


node_start_election(LeftPID, RightPID, State) ->
    % I'll send probes only if I have not been elected earlier
    case State#node_state.elected_earlier == false of
        true ->
            node_send_probes(LeftPID, RightPID, State, 1),
            node_handle_election_messages(LeftPID, RightPID, State, true, 1, []);
        false ->
            % otherwise I am not an active node in election
            node_handle_election_messages(LeftPID, RightPID, State, false, 1, [])
    end.
    
    

% ---- Node's Utility Functions during election
node_send_probes(LeftPID, RightPID, State, TTL) -> 
    %io:format("~p(~p): sending probes with ttl: ~p~n", [State#node_state.info#node_info.name, self(),
    %        TTL]),
    P = #probe{prober_id = State#node_state.info#node_info.node_id, 
        priority = State#node_state.info#node_info.priority,
        ttl = TTL, election_time = State#node_state.current_election_time},
    LeftPID ! {probe, P#probe{direction=left}},
    RightPID ! {probe, P#probe{direction=right}}.

node_handle_election_messages(LeftPID, RightPID, State, AmIActive, TTL, Replies) ->
    %io:format("~p(~p): State: ~p, AmIActive: ~p, TTL: ~p, ReplyStatus: ~p~n", [State#node_state.info#node_info.name, self(), State, AmIActive, TTL, Replies]),
    receive
        {'EXIT', _, _} ->
            %io:format("~p exiting, got ~p~n", [State#node_state.info#node_info.name, {'EXIT', Reason, From}]),
            exit(normal);

        {leader_elected, Leader, RevoltThreshold} ->
            LeaderId = Leader#leader.id,
            Time = Leader#leader.elected_on,
            %io:format("~p(~p) Leader Elected: ~p at time ~p~n", [State#node_state.info#node_info.name, self(), 
            %    LeaderId, Time]),
            case LeaderId == State#node_state.info#node_info.node_id of
                true -> 
                    UpdatedState = State#node_state{elected_earlier = true, revolt_threshold = RevoltThreshold},
                    ICanRevolt = false,
                    % send tick message
                    LeftPID ! {tick, Time+1, []};

                false -> 
                    UpdatedState = State,
                    ICanRevolt = true
            end,

            node_handle_tick_messages(LeftPID, RightPID, UpdatedState, Leader, ICanRevolt);
            
        {probe, Probe} ->
            node_handle_probe(Probe, LeftPID, RightPID, State, AmIActive, TTL, Replies);

        {reply, Reply} ->
            node_handle_reply(Reply, LeftPID, RightPID, State, AmIActive, TTL, Replies)
    end.

%% ----- PROBE MESSAGES -----


node_can_prober_be_my_leader(Probe, State) ->
    case State#node_state.elected_earlier of 
        true -> true;
        false ->
            ProbePriority = Probe#probe.priority,
            MyPriority = State#node_state.info#node_info.priority,
            ProbeId = Probe#probe.prober_id,
            MyId = State#node_state.info#node_info.node_id,
            if ProbePriority > MyPriority orelse (ProbePriority == MyPriority andalso ProbeId > MyId) ->
                true;
            true ->
                false
            end
    end.


% -- I see my own probe, I am the leader
node_handle_probe_message(_, _, Probe, State) 
    when Probe#probe.prober_id == State#node_state.info#node_info.node_id ->
    %io:format("~p HAVE SEEN MY OWN PROBE ~p, I am the leader!~n", 
    %    [State#node_state.info#node_info.name, Probe]),
    State#node_state.supervisor ! {election_done, State, self(), Probe#probe.election_time};

% -- I see +ve TTL but this prober CANNOT be my leader
% -- I see +ve TTL (but this prober CAN be my leader).
node_handle_probe_message(_, DestNode, Probe, State) 
    when Probe#probe.ttl > 1 ->
    LeaderFlag = (Probe#probe.leader_flag == true andalso node_can_prober_be_my_leader(Probe, State)),
    P = Probe#probe{ttl = Probe#probe.ttl-1, 
        leader_flag = LeaderFlag},
    %io:format("~p(~p): updated probe ~p and sending it to to ~p~n", 
    %    [State#node_state.info#node_info.name, self(), P, Neighbour2]),
    DestNode ! {probe, P};

% -- I see expired TTL, need to reply
node_handle_probe_message(SrcNode, _, Probe, State) 
    when Probe#probe.ttl =< 1 ->
    LeaderFlag = (Probe#probe.leader_flag == true andalso node_can_prober_be_my_leader(Probe, State)),
    R = #reply{prober_id = Probe#probe.prober_id, leader_flag = LeaderFlag, 
        direction = Probe#probe.direction, election_time = Probe#probe.election_time},
    %io:format("~p(~p) replying ~p to ~p~n", [State#node_state.info#node_info.name, self(), R, Neighbour1]),
    SrcNode ! {reply, R}.      



% handle if probe is for current election
node_handle_probe(Probe, LeftPID, RightPID, State, AmIActive, TTL, Replies) 
    when Probe#probe.election_time  == State#node_state.current_election_time ->
    % Note the direction of probes, a probe going to left comes from
    % node on the right and it needs to go towards the left.
    %io:format("~p(~p) PROBE: ~p~n", [State#node_state.info#node_info.name, self(), Probe]),
    if Probe#probe.direction == left ->
        node_handle_probe_message(RightPID, LeftPID, Probe, State);
    Probe#probe.direction == right -> 
         node_handle_probe_message(LeftPID, RightPID, Probe, State)
    
    end,

    case AmIActive andalso node_can_prober_be_my_leader(Probe, State) of
        true -> 
            %io:format("~p: I will no longer be active since this prober is higher than me~n", [self()]),
            node_handle_election_messages(LeftPID, RightPID, State, false, TTL, Replies);
        false ->
            node_handle_election_messages(LeftPID, RightPID, State, AmIActive, TTL, Replies)
    end;

% ignore if the probe is not for current election
node_handle_probe(_, LeftPID, RightPID, State, AmIActive, TTL, Replies) ->
    node_handle_election_messages(LeftPID, RightPID, State, AmIActive, TTL, Replies).

%% -------- REPLY MESSAGES -------
% handle if reply is from current election
node_handle_reply(Reply, LeftPID, RightPID, State, AmIActive, TTL, Replies) 
    when Reply#reply.election_time  == State#node_state.current_election_time ->
    %io:format("~p(~p) REPLY: ~p~n", [State#node_state.info#node_info.name, self(), Reply]),
    case Reply#reply.prober_id /= State#node_state.info#node_info.node_id of
        true ->
            % if reply is not for me, I just forward it to the next node
            case Reply#reply.direction of
                    % Note that the directions are inverted for reply messages,
                    % so a "left" reply comes from left and goes to right.
                left ->
                    %io:format("~p received reply from left: ~p~n", 
                    %    [State#node_state.info#node_info.name, Reply]),
                    node_handle_reply_message(LeftPID, RightPID, Reply, State);
            
                right ->
                    %io:format("~p received reply from right: ~p~n", [State#node_state.info#node_info.name, Reply]),
                    node_handle_reply_message(RightPID, LeftPID, Reply, State)
            end,
            node_handle_election_messages(LeftPID, RightPID, State, AmIActive, TTL, Replies);
        false ->
            UpdatedReplies = lists:append(Replies, [Reply]),
            %io:format("~p(~p): Replies: ~p~n", [State#node_state.info#node_info.name, self(), UpdatedReplies]),
            % if reply is for me, I need to see that:
            %    1. I am not active:
            %           - no further processing needed
            %    2. if I am active
            %           - if have I received both replies, and:
            %                    - leader bit is set: I'll send new probes
            %                    - otherwise: I'll mark myself not active
            %io:format("~p reply for myself ~p, ReplyStatus: ~p, AmIActive: ~p~n", 
            %    [State#node_state.info#node_info.name, Reply, ReplyStatus, AmIActive]),
            case AmIActive of
                false ->
                    node_handle_election_messages(LeftPID, RightPID, State, AmIActive, TTL, UpdatedReplies);
                true ->
                    case Reply#reply.leader_flag of
                        false -> node_handle_election_messages(LeftPID, RightPID, State, false, TTL, UpdatedReplies);
                        true -> 
                            case length(UpdatedReplies) == 2 of
                                false -> node_handle_election_messages(LeftPID, RightPID, State, AmIActive, TTL, UpdatedReplies);
                                true -> 
                                    %io:format("~p(~p): I am active, I received both replies, I shall send new probes~n", 
                                    %    [State#node_state.info#node_info.name, self()]),
                                    node_send_probes(LeftPID, RightPID, State, 2*TTL),
                                    node_handle_election_messages(LeftPID, RightPID, State, AmIActive, 2*TTL,[])
                            end
                    end
            end

    end;

% ignore if reply is not from current election
node_handle_reply(_, LeftPID, RightPID, State, AmIActive, TTL, Replies) ->
    node_handle_election_messages(LeftPID, RightPID, State, AmIActive, TTL, Replies).

node_handle_reply_message(_, DestNode, Reply, State)
    when Reply#reply.prober_id /= State#node_state.info#node_info.node_id ->
        %io:format("~p(~p) forwarding reply ~p to ~p~n", [State#node_state.info#node_info.name, self(), R, Neighbour2]),
        DestNode ! {reply, Reply}.


% ------ Node's functions during clock ticking --------
node_handle_tick_messages(LeftPID, RightPID, State, Leader, ICanRevolt) ->
    %io:format("~p(~p) HANDLE TICK~n", [State#node_state.info#node_info.name, self()]),
    LeaderId = Leader#leader.id,
    MyID = State#node_state.info#node_info.node_id,
    MyTolerance =  State#node_state.info#node_info.tolerance,
    receive
        {'EXIT', _, _} ->
            %io:format("~p exiting, got ~p~n", [State#node_state.info#node_info.name, {'EXIT', Reason, From}]),
            exit(normal);

        {tick, Time, RevoltedNodes} ->
            %io:format("~p(~p) TICKING: time: ~p~n", [State#node_state.info#node_info.name, self(), Time]),
            RevoltThreshold = State#node_state.revolt_threshold,
            NewTime = Time + 1,
            case MyID == LeaderId of 
                true ->
                case length(RevoltedNodes) >= RevoltThreshold of
                    true -> 
                        %io:format("~p(~p) LEADER: sending leader deposed at ~p~n", [State#node_state.info#node_info.name, self(), Time]),
                        State#node_state.supervisor ! {leader_deposed, Time},
                        node_handle_tick_messages(LeftPID, RightPID, State, Leader, ICanRevolt);
                    false -> 
                        LeftPID ! {tick, NewTime, RevoltedNodes},
                        node_handle_tick_messages(LeftPID, RightPID, State, Leader, ICanRevolt)
                end;
                false ->
                    CurrentRegime = Time - Leader#leader.elected_on,
                    % case ICanRevolt of 
                    %     true -> io:format("~p(~p): Time: ~p, CurrentRegime: ~p, MyTolerance: ~p~n", [State#node_state.info#node_info.name, self(), Time, CurrentRegime, MyTolerance]);
                    %     false -> ok
                    % end,
                    case ICanRevolt andalso CurrentRegime >= MyTolerance of 
                        true -> 
                            io:format("ID=~p revolted at t=~p~n", [State#node_state.info#node_info.node_id, Time]),
                            UpdatedICanRevolt = false,
                            LeftPID ! {tick, NewTime, lists:append(RevoltedNodes, [State#node_state.info])};
                        false -> 
                            UpdatedICanRevolt = ICanRevolt,
                            LeftPID ! {tick, NewTime, RevoltedNodes}
                    end,
                    node_handle_tick_messages(LeftPID, RightPID, State, Leader, UpdatedICanRevolt)
            end;

        {start_election, LeftPID, RightPID, Time} ->
            %io:format("~p(~p): start_election ~p, ~p, ~p~n", 
            %    [State#node_state.info#node_info.name, self(), LeftPID, RightPID, Time]),
            UpdatedState = State#node_state{current_election_time = Time},
            node_start_election(LeftPID, RightPID, UpdatedState)
    end.
