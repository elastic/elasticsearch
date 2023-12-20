/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.Function;

public abstract class LongFieldScript extends AbstractLongFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("long_field", Factory.class);

    public static final Factory PARSE_FROM_SOURCE = new Factory() {
        @Override
        public LeafFactory newFactory(String field, Map<String, Object> params, SearchLookup lookup, OnScriptError onScriptError) {
            return ctx -> new LongFieldScript(field, params, lookup, OnScriptError.FAIL, ctx) {
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
                return new LongFieldScript(leafFieldName, params, searchLookup, onScriptError, ctx) {
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
        LongFieldScript newInstance(LeafReaderContext ctx);
    }

    public LongFieldScript(
        String fieldName,
        Map<String, Object> params,
        SearchLookup searchLookup,
        OnScriptError onScriptError,
        LeafReaderContext ctx
    ) {
        super(fieldName, params, searchLookup, onScriptError, ctx);
    }

    @Override
    protected void emitFromObject(Object v) {
        try {
            emit(NumberFieldMapper.NumberType.objectToLong(v, true));
        } catch (Exception e) {
            // ignore;
        }
    }

    public static class Emit {
        private final LongFieldScript script;

        public Emit(LongFieldScript script) {
            this.script = script;
        }

        public void emit(long v) {
            script.checkMaxSize(script.count());
            script.emit(v);
        }
    }
}
