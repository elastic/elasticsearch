/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.DynamicMap;
import org.elasticsearch.script.ScriptCache;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * Abstract base for scripts to execute to build scripted fields. Inspired by
 * {@link AggregationScript} but hopefully with less historical baggage.
 */
public abstract class AbstractScriptFieldScript {
    public static <F> ScriptContext<F> newContext(String name, Class<F> factoryClass) {
        return new ScriptContext<F>(
            name + "_script_field",
            factoryClass,
            /*
             * In an ideal world we wouldn't need the script cache at all
             * because we have a hard reference to the script. The trouble
             * is that we compile the scripts a few times when performing
             * a mapping update. This is unfortunate, but we rely on the
             * cache to speed this up.
             */
            100,
            timeValueMillis(0),
            /*
             * Disable compilation rate limits for scripted fields so we
             * don't prevent mapping updates because we've performed too
             * many recently. That'd just be lame.
             */
            ScriptCache.UNLIMITED_COMPILATION_RATE.asTuple()
        );
    }

    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
        "_source",
        value -> ((SourceLookup) value).loadSourceIfNeeded()
    );

    private final Map<String, Object> params;
    private final LeafSearchLookup leafSearchLookup;

    public AbstractScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        this.leafSearchLookup = searchLookup.getLeafSearchLookup(ctx);
        params = new HashMap<>(params);
        params.put("_source", leafSearchLookup.source());
        params.put("_fields", leafSearchLookup.fields());
        this.params = new DynamicMap(params, PARAMS_FUNCTIONS);
    }

    /**
     * Set the document to run the script against.
     */
    public final void setDocument(int docId) {
        this.leafSearchLookup.setDocument(docId);
    }

    /**
     * Expose the {@code params} of the script to the script itself.
     */
    public final Map<String, Object> getParams() {
        return params;
    }

    /**
     * Expose the {@code _source} to the script.
     */
    protected final Map<String, Object> getSource() {
        return leafSearchLookup.source();
    }

    /**
     * Expose field data to the script as {@code doc}.
     */
    public final Map<String, ScriptDocValues<?>> getDoc() {
        return leafSearchLookup.doc();
    }

    public abstract void execute();
}
