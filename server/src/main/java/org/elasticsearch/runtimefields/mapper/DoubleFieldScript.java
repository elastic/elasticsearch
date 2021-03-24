/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

public abstract class DoubleFieldScript extends AbstractFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("double_script_field", Factory.class);

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        DoubleFieldScript newInstance(LeafReaderContext ctx);
    }

    static final Factory PARSE_FROM_SOURCE = (field, params, lookup) -> (LeafFactory) ctx -> new DoubleFieldScript(
        field,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            for (Object v : extractFromSource(field)) {
                if (v instanceof Number) {
                    emit(((Number) v).doubleValue());
                } else if (v instanceof String) {
                    try {
                        emit(Double.parseDouble((String) v));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }
        }
    };

    private double[] values = new double[1];
    private int count;

    public DoubleFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
    }

    /**
     * Execute the script for the provided {@code docId}.
     */
    public final void runForDoc(int docId) {
        count = 0;
        setDocument(docId);
        execute();
    }

    /**
     * Values from the last time {@link #runForDoc(int)} was called. This array
     * is mutable and will change with the next call of {@link #runForDoc(int)}.
     * It is also oversized and will contain garbage at all indices at and
     * above {@link #count()}.
     */
    public final double[] values() {
        return values;
    }

    /**
     * The number of results produced the last time {@link #runForDoc(int)} was called.
     */
    public final int count() {
        return count;
    }

    protected final void emit(double v) {
        checkMaxSize(count);
        if (values.length < count + 1) {
            values = ArrayUtil.grow(values, count + 1);
        }
        values[count++] = v;
    }

    public static class Emit {
        private final DoubleFieldScript script;

        public Emit(DoubleFieldScript script) {
            this.script = script;
        }

        public void emit(double v) {
            script.emit(v);
        }
    }
}
