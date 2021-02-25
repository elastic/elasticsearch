/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields.mapper;

import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.RuntimeFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Abstract base {@linkplain MappedFieldType} for runtime fields based on a script.
 */
abstract class AbstractScriptFieldType<LeafFactory> extends RuntimeFieldType {
    protected final Script script;
    private final TriFunction<String, Map<String, Object>, SearchLookup, LeafFactory> factory;

    AbstractScriptFieldType(String name, TriFunction<String, Map<String, Object>, SearchLookup, LeafFactory> factory, Builder builder) {
        super(name, builder);
        this.factory = factory;
        this.script = builder.getScript();
    }

    AbstractScriptFieldType(
        String name,
        TriFunction<String, Map<String, Object>, SearchLookup, LeafFactory> factory,
        Script script,
        Map<String, String> meta,
        CheckedBiConsumer<XContentBuilder, Boolean, IOException> toXContent
    ) {
        super(name, meta, toXContent);
        this.factory = factory;
        this.script = script;
    }

    /**
     * Create a script leaf factory.
     */
    protected final LeafFactory leafFactory(SearchLookup searchLookup) {
        return factory.apply(name(), script.getParams(), searchLookup);
    }

    /**
     * Create a script leaf factory for queries.
     */
    protected final LeafFactory leafFactory(SearchExecutionContext context) {
        /*
         * Forking here causes us to count this field in the field data loop
         * detection code as though we were resolving field data for this field.
         * We're not, but running the query is close enough.
         */
        return leafFactory(context.lookup().forkAndTrackFieldReferences(name()));
    }

    // Placeholder Script for source-only fields
    // TODO rework things so that we don't need this
    private static final Script DEFAULT_SCRIPT = new Script("");

    abstract static class Builder extends RuntimeFieldType.Builder {
        final FieldMapper.Parameter<Script> script = new FieldMapper.Parameter<>(
            "script",
            true,
            () -> null,
            Builder::parseScript,
            initializerNotSupported()
        ).setSerializerCheck((id, ic, v) -> ic);

        Builder(String name) {
            super(name);
        }

        @Override
        protected List<FieldMapper.Parameter<?>> getParameters() {
            List<FieldMapper.Parameter<?>> parameters = new ArrayList<>(super.getParameters());
            parameters.add(script);
            return Collections.unmodifiableList(parameters);
        }

        protected final Script getScript() {
            if (script.get() == null) {
                return DEFAULT_SCRIPT;
            }
            return script.get();
        }

        private static Script parseScript(String name, Mapper.TypeParser.ParserContext parserContext, Object scriptObject) {
            Script script = Script.parse(scriptObject);
            if (script.getType() == ScriptType.STORED) {
                throw new IllegalArgumentException("stored scripts are not supported for runtime field [" + name + "]");
            }
            return script;
        }
    }

    static <T> Function<FieldMapper, T> initializerNotSupported() {
        return mapper -> { throw new UnsupportedOperationException(); };
    }
}
