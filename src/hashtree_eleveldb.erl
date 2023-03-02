-module(hashtree_eleveldb).
-behaviour(hashtree_storage).

-export([open/1,
         close/1,
         destroy/1,
         get/2,
         put/3,
         write/2,
         delete/2,
         fold/4,
         iterator/1,
         iterator_move/2,
         iterator_close/1]).

-dialyzer({nowarn_function, [open/1,
                             close/1,
                             destroy/1,
                             get/2,
                             put/3,
                             write/2,
                             delete/2,
                             fold/4,
                             iterator/1,
                             iterator_move/2,
                             iterator_close/1]}).

open(Path) ->
    DefaultWriteBufferMin = 4 * 1024 * 1024,
    DefaultWriteBufferMax = 14 * 1024 * 1024,
    ConfigVars = get_env(anti_entropy_leveldb_opts,
                         [{write_buffer_size_min, DefaultWriteBufferMin},
                          {write_buffer_size_max, DefaultWriteBufferMax}]),
    Config = orddict:from_list(ConfigVars),

    %% Use a variable write buffer size to prevent against all buffers being
    %% flushed to disk at once when under a heavy uniform load.
    WriteBufferMin = proplists:get_value(write_buffer_size_min, Config, DefaultWriteBufferMin),
    WriteBufferMax = proplists:get_value(write_buffer_size_max, Config, DefaultWriteBufferMax),
    Offset = rand:uniform(1 + WriteBufferMax - WriteBufferMin),
    WriteBufferSize = WriteBufferMin + Offset,
    Config2 = orddict:store(write_buffer_size, WriteBufferSize, Config),
    Config3 = orddict:erase(write_buffer_size_min, Config2),
    Config4 = orddict:erase(write_buffer_size_max, Config3),
    Config5 = orddict:store(is_internal_db, true, Config4),
    Config6 = orddict:store(use_bloomfilter, true, Config5),
    Options = orddict:store(create_if_missing, true, Config6),

    ok = filelib:ensure_dir(Path),

    eleveldb:open(Path, Options).

close(Ref) ->                    eleveldb:close(Ref).
destroy(Path) ->                 eleveldb:destroy(Path, []).
get(Ref, Key) ->                 eleveldb:get(Ref, Key, []).
put(Ref, Key, Value) ->          eleveldb:put(Ref, Key, Value, []).
write(Ref, Updates) ->           eleveldb:write(Ref, Updates, []).
delete(Ref, Key) ->              eleveldb:delete(Ref, Key, []).
fold(Ref, Fun, Acc, FirstKey) -> eleveldb:fold(Ref, Fun, Acc, [{first_key, FirstKey}]).
iterator(Ref) ->                 eleveldb:iterator(Ref, []).
iterator_move(Itr, Seek) ->      eleveldb:iterator_move(Itr, Seek).
iterator_close(Itr) ->           eleveldb:iterator_close(Itr).

% internal
get_env(Key, Default) ->
    CoreEnv = app_helper:get_env(riak_core, Key, Default),
    app_helper:get_env(riak_kv, Key, CoreEnv).
