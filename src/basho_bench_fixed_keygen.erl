-module(basho_bench_fixed_keygen).

-export([fixed/2]).

fixed(_Id, Fixed) ->
    fun() ->     
        Fixed
    end.

