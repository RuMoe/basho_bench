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
    MyNode  = basho_bench_config:get(scalarisclient_mynode),

    %% Try to spin up net_kernel
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
    end,

    %% Try to ping each of the nodes
    ping_each(Nodes, Id),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    case rpc:call(TargetNode, api_vm, number_of_nodes, []) of
        {badrpc, _Reason} ->
            ?FAIL_MSG("Failed to connect to ~p: ~p\n", [TargetNode, _Reason]);
        N when N > 0 ->
            {ok, TargetNode}
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case rpc:call(State, api_tx, read, [Key]) of
        {ok, _Value} ->
            {ok, State};
        {fail, not_found} ->
            {ok, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value = ValueGen(),
    case rpc:call(State, api_tx, write, [Key, Value]) of
        {ok} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    Value = ValueGen(),
    run(put, fun() -> Key end, fun() -> Value end, State);
run(delete, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case rpc:call(State, api_rdht, delete, [Key]) of
        {ok, 4, [ok, ok, ok, ok]} ->
            {ok, State};
        {ok, 0, [undef, undef, undef, undef]} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end.


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
