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
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.DoubleConsumer;
import java.util.function.Function;

public abstract class DoubleFieldScript extends AbstractFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("double_field", Factory.class);

    public static final Factory PARSE_FROM_SOURCE = new Factory() {
        @Override
        public LeafFactory newFactory(String field, Map<String, Object> params, SearchLookup lookup, OnScriptError onScriptError) {
            return ctx -> new DoubleFieldScript(field, params, lookup, OnScriptError.FAIL, ctx) {
                @Override
                public void execute() {
                    emitFromSource();
                }
            };
        }

        @Override
        public boolean isResultDeterministic() {
            return true;
        }
    };

    public static Factory leafAdapter(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentFactory) {
        return (leafFieldName, params, searchLookup, onScriptError) -> {
            CompositeFieldScript.LeafFactory parentLeafFactory = parentFactory.apply(searchLookup);
            return (LeafFactory) ctx -> {
                CompositeFieldScript compositeFieldScript = parentLeafFactory.newInstance(ctx);
                return new DoubleFieldScript(leafFieldName, params, searchLookup, onScriptError, ctx) {
                    @Override
                    public void setDocument(int docId) {
                        compositeFieldScript.setDocument(docId);
                    }

                    @Override
                    public void execute() {
                        emitFromCompositeScript(compositeFieldScript);
                    }
                };
            };
        };
    }

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup, OnScriptError onScriptError);
    }

    public interface LeafFactory {
        DoubleFieldScript newInstance(LeafReaderContext ctx);
    }

    private double[] values = new double[1];
    private int count;

    public DoubleFieldScript(
        String fieldName,
        Map<String, Object> params,
        SearchLookup searchLookup,
        OnScriptError onScriptError,
        LeafReaderContext ctx
    ) {
        super(fieldName, params, searchLookup, ctx, onScriptError);
    }

    @Override
    protected void prepareExecute() {
        count = 0;
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
     * The number of results produced the last time {@link #runForDoc(int)} was called.
     */
    public final int count() {
        return count;
    }

    @Override
    protected void emitFromObject(Object v) {
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

    public final void emit(double v) {
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
            script.checkMaxSize(script.count());
            script.emit(v);
        }
    }
}
