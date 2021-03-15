/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Static methods for mapping parameters relating to mapper scripts
 */
public final class ScriptParams {

    private ScriptParams() { }

    /**
     * Defines a script parameter
     * @param compiler      a function that produces a compiled script, given a {@link Script} and {@link ScriptService}
     * @param initializer   retrieves the equivalent parameter from an existing FieldMapper for use in merges
     * @param <T>           the type of the compiled script
     * @return a script parameter
     */
    public static <T> FieldMapper.Parameter<MapperScript<T>> script(
        BiFunction<Script, ScriptService, MapperScript<T>> compiler,
        Function<FieldMapper, MapperScript<T>>initializer
    ) {
        return new FieldMapper.Parameter<>(
            "script",
            false,
            () -> null,
            (n, c, o) -> {
                if (o == null) {
                    return null;
                }
                Script script = Script.parse(o);
                return compiler.apply(script, c.scriptService());
            },
            initializer
        ).acceptsNull();
    }
}
