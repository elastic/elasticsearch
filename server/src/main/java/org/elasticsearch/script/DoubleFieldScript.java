/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Arrays;
import java.util.Map;
import java.util.function.DoubleConsumer;

public abstract class DoubleFieldScript extends AbstractFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("double_field", Factory.class);

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        DoubleFieldScript newInstance(LeafReaderContext ctx);
    }

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
     * Execute the script for the provided {@code docId}, passing results to the {@code consumer}
     */
    public final void runForDoc(int docId, DoubleConsumer consumer) {
        runForDoc(docId);
        for (int i = 0; i < count; i++) {
            consumer.accept(values[i]);
        }
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
     * Reorders the values from the last time {@link #values()} was called to
     * how this would appear in doc-values order. Truncates garbage values
     * based on {@link #count()}.
     */
    public final double[] asDocValues() {
        double[] truncated = Arrays.copyOf(values, count());
        Arrays.sort(truncated);
        return truncated;
    }

    /**
     * The number of results produced the last time {@link #runForDoc(int)} was called.
     */
    public final int count() {
        return count;
    }

    public final void emit(double v) {
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
