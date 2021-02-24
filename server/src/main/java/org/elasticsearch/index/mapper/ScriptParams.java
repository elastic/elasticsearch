/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

public final class ScriptParams {

    private ScriptParams() { }

    public static class CompiledScriptParameter<T> implements ToXContent {
        final Script script;
        final T compiledScript;

        public CompiledScriptParameter(Script script, T compiledScript) {
            this.script = script;
            this.compiledScript = compiledScript;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompiledScriptParameter<?> that = (CompiledScriptParameter<?>) o;
            return Objects.equals(script, that.script);
        }

        @Override
        public int hashCode() {
            return Objects.hash(script);
        }

        @Override
        public String toString() {
            return script.toString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return script.toXContent(builder, params);
        }
    }

    public static <T> FieldMapper.Parameter<CompiledScriptParameter<T>> script(
        BiFunction<Script, ScriptService, T> compiler,
        Function<FieldMapper, CompiledScriptParameter<T>>initializer
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
                return new CompiledScriptParameter<>(script, compiler.apply(script, c.scriptService()));
            },
            initializer
        ).acceptsNull();
    }
}
