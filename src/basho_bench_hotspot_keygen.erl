%% @author Jan Skrzypczak <skrzypczak@zib.de>
%% @doc    Implementation of a hot spot key generator
%% @end
-module(basho_bench_hotspot_keygen).

-export([new/4]).

%% Returns a new key generator which will return in HotSpotAccessPercentage
%% of the time a key of a sub-keyspace which contains HotSpotKeySpacePercentage
%% of the elements from 1 to MaxKey. Otherwise it will return a key which is not 
%% contained in the hotspot sub-keyspace.
%% The elements of the hotspot subspace are uniformly distributed in the keys space.
new(_Id, MaxKey, HotSpotAccessPercentage, HotSpotKeySpacePercentage) ->
    fun() ->
            % every Nth key is a hotspot key
            N = ceiling(1.0 / HotSpotKeySpacePercentage),
            RandKey = random:uniform(MaxKey),
            Rem = RandKey rem N,
            case random:uniform() =< HotSpotAccessPercentage of
                true ->
                    % hotspot
                    RandKey - Rem;
                false ->
                    % no hotspot
                    case Rem =:= 0 of
                        true ->
                            % hit a hotspot key -> reassign value outside hotspot
                            RandKey - random:uniform(N-1);
                        false -> RandKey
                    end
            end
    end.

%% Returns smallest integer >= X
ceiling(X) ->
    T = trunc(X),
    case X - T == 0 of
        true -> T;
        false -> T + 1
    end.