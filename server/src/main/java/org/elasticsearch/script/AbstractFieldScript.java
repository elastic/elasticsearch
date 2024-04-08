/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.core.TimeValue.timeValueMillis;

/**
 * Abstract base for scripts to execute to build scripted fields. Inspired by
 * {@link AggregationScript} but hopefully with less historical baggage.
 */
public abstract class AbstractFieldScript extends DocBasedScript {
    /**
     * The maximum number of values a script should be allowed to emit.
     */
    public static final int MAX_VALUES = 100;

    protected static <F> ScriptContext<F> newContext(String name, Class<F> factoryClass) {
        return new ScriptContext<>(
            name,
            factoryClass,
            /*
             * We rely on the script cache in two ways:
             * 1. It caches the "heavy" part of mappings generated at runtime.
             * 2. Mapping updates tend to try to compile the script twice. Not
             *    for any good reason. They just do.
             * Thus we use the default 100.
             */
            100,
            timeValueMillis(0),
            /*
             * Disable compilation rate limits for runtime fields so we
             * don't prevent mapping updates because we've performed too
             * many recently. That'd just be lame. We also compile these
             * scripts during search requests so this could totally be a
             * source of runaway script compilations. We think folks will
             * mostly reuse scripts though.
             */
            false,
            /*
             * Disable runtime fields scripts from being allowed
             * to be stored as part of the script meta data.
             */
            false
        );
    }

    @SuppressWarnings("unchecked")
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
        "_source",
        value -> ((Supplier<Source>) value).get().source()
    );

    protected final String fieldName;
    protected final Supplier<Source> source;
    private final Map<String, Object> params;
    private final OnScriptError onScriptError;

    public AbstractFieldScript(
        String fieldName,
        Map<String, Object> params,
        SearchLookup searchLookup,
        LeafReaderContext ctx,
        OnScriptError onScriptError
    ) {
        this(fieldName, params, new DocValuesDocReader(searchLookup, ctx), onScriptError);
    }

    private AbstractFieldScript(String fieldName, Map<String, Object> params, DocReader docReader, OnScriptError onScriptError) {
        super(docReader);
        this.fieldName = fieldName;
        this.source = docReader.source();
        params = new HashMap<>(params);
        params.put("_source", this.source);
        params.put("_fields", docReader.docAsMap().get("_fields"));
        this.params = new DynamicMap(params, PARAMS_FUNCTIONS);
        this.onScriptError = onScriptError;
    }

    /**
     * Expose the {@code params} of the script to the script itself.
     */
    public final Map<String, Object> getParams() {
        return params;
    }

    protected List<Object> extractFromSource(String path) {
        return XContentMapValues.extractRawValues(path, source.get().source());
    }

    protected final void emitFromCompositeScript(CompositeFieldScript compositeFieldScript) {
        List<Object> values = compositeFieldScript.getValues(fieldName);
        if (values == null) {
            return;
        }
        for (Object value : values) {
            emitValueFromCompositeScript(value);
        }
    }

    protected void emitValueFromCompositeScript(Object value) {
        emitFromObject(value);
    }

    protected abstract void emitFromObject(Object v);

    protected final void emitFromSource() {
        for (Object v : extractFromSource(fieldName)) {
            emitFromObject(v);
        }
    }

    /**
     * Check if the we can add another value to the list of values.
     * @param currentSize the current size of the list
     */
    protected final void checkMaxSize(int currentSize) {
        if (currentSize >= MAX_VALUES) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Runtime field [%s] is emitting [%s] values while the maximum number of values allowed is [%s]",
                    fieldName,
                    currentSize + 1,
                    MAX_VALUES
                )
            );
        }
    }

    protected abstract void prepareExecute();

    /**
     * Execute the script for the provided {@code docId}.
     */
    public final void runForDoc(int docId) {
        prepareExecute();
        setDocument(docId);
        try {
            execute();
        } catch (Exception e) {
            if (onScriptError == OnScriptError.CONTINUE) {
                // ignore
            } else {
                throw e;
            }
        }
    }

    public abstract void execute();
}
