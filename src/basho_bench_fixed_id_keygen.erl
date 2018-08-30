-module(basho_bench_fixed_id_keygen).

-export([fixed/2]).

fixed(Id, Fixed) ->
    fun() ->
        {Id, Fixed}
    end.

