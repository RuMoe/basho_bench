%% -------------------------------------------------------------------
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_scalaris).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    Nodes   = basho_bench_config:get(scalarisclient_nodes),

    %% Try to ping each of the nodes
    ping_each(Nodes, Id),

    %% work around to print current seed
    Seed = random:seed(),
    random:seed(Seed),
    ?INFO("Worker with ID ~p uses random seed ~p.", [Id, Seed]),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    case rpc:call(TargetNode, api_vm, number_of_nodes, []) of
        {badrpc, _Reason} ->
            ?FAIL_MSG("Failed to connect to ~p: ~p\n", [TargetNode, _Reason]);
        N when N > 0 ->
            case basho_bench_config:get(scalaris_fill) of
                    true ->
                            % abuse the first started worker to
                            % fill used key range of scalaris before benchmark
                            ValueGenConf  = basho_bench_config:get(value_generator),
                            ValGen = basho_bench_valgen:new(ValueGenConf, Id),
                            Max = basho_bench_config:get(scalaris_fill_max),
                            Pct = basho_bench_config:get(scalaris_fill_pct),
                            case rpc:call(TargetNode, mvcc_on_cseq, read, ["0"]) of
                                key_not_found ->
                                    ?INFO("Write ~p% of data in key range 0-~p", [Pct*100, Max]),
                                    [run(init, Key, ValGen, TargetNode) || Key <- lists:seq(0, Max)];
                                _ ->
                                     % already filled because we are not the first
                                     % worker
                                    ok
                            end;
                    _ -> ok
            end,
            {ok, TargetNode}
    end.
run(init, Key, ValueGen, State) ->
    Pct = basho_bench_config:get(scalaris_fill_pct),
    Max = basho_bench_config:get(scalaris_fill_max),

    _ = [case Max*(R-1)+Key < 3*Max*Pct of
               true ->
                    rpc:call(State, mvcc_on_cseq, write, [integer_to_list(Key), ValueGen()]);
               false ->
                    ok
         end || R <- lists:seq(1, 3)],
    ok;
run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case random:uniform(10) of
        1 ->
            % read specific version
             Version = case rpc:call(State, mvcc_on_cseq,
                                                     read_with_version, [Key]) of
                            {ok, _Val, Ver} ->
                                Ver;
                            Ret ->
                                ?INFO("read with version failed in read, key: ~p, return ~p", [Key, Ret]),
                                1
                              end,
            case rpc:call(State, mvcc_on_cseq, read, [Key, Version]) of
                {ok, _Value} ->
                    {ok, State};
                Error ->
                    {error, {read_specific_version, Key, Error}, State}
            end;
        2 ->
            % read with version
            case rpc:call(State, mvcc_on_cseq, read_with_version, [Key]) of
                {ok, _Value, _Version} ->
                    {ok, State};
                Error ->
                    {error,  {read_with_version, Key, Error}, State}
            end;
        _ ->
            % read
            case rpc:call(State, mvcc_on_cseq, read, [Key]) of
                {ok, _Value} ->
                    {ok, State};
                Error ->
                    {error,  {read, Key, Error}, State}
            end
    end;
run(put, KeyGen, ValueGen, State) ->
    WorkerPid = self(),
    Key = KeyGen(),
    Value = ValueGen(),
    ConcurrentThreads =
        case random:uniform(10) of
            1 -> 2;
            _ -> 1
        end,
    ArgList = case random:uniform(10) of
                1 ->
                    Version = case rpc:call(State, mvcc_on_cseq,
                                                     read_with_version, [Key]) of
                            {ok, _Val, Ver} ->
                                Ver;
                            Rett ->
                                ?INFO("read with version failed in write key: ~p, return: ~p", [Key, Rett]),
                                0
                              end,
                    [Key, Version+1, Value];
                _ ->
                    [Key, Value]
              end,
    _ = [ spawn(fun() ->
                         Return = case rpc:call(State, mvcc_on_cseq, write, ArgList) of
                                    {ok, _} ->
                                        {ok, State};
                                    {fail} ->
                                        {ok, State};
                                    Ret ->
                                        %?INFO("something else returned at write... ~p key ~p", [Ret, Key]),
                                        %{error, {unknwon_return, Ret}, State}
                                        {ok, State}
                               end,
                         WorkerPid ! Return
                end)
        || _Id <- lists:seq(1, ConcurrentThreads)],

    _ = [ receive {ok, _} ->
                      ok
          end || _P <- lists:seq(1, ConcurrentThreads)],
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

ping_each([], _Id) ->
    ok;
ping_each([Node | Rest], Id) ->
    case net_adm:ping(Node) of
        pong ->
            %% ?INFO("Pinging node ~w. Cookie: ~w\n", [Node, erlang:get_cookie()]),
            ping_each(Rest, Id);
        pang ->
            %% ?WARN("Cookie: ~w\n", [erlang:get_cookie()]),
            ?FAIL_MSG("Worker ~w failed to ping node ~p\n", [Id, Node])
    end.
