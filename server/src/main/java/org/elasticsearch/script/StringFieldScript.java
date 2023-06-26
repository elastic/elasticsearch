/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class StringFieldScript extends AbstractFieldScript {
    /**
     * The maximum number of chars a script should be allowed to emit.
     */
    public static final long MAX_CHARS = 1024 * 1024;

    public static final ScriptContext<Factory> CONTEXT = newContext("keyword_field", Factory.class);

    public static final StringFieldScript.Factory PARSE_FROM_SOURCE = new Factory() {
        @Override
        public LeafFactory newFactory(String field, Map<String, Object> params, SearchLookup lookup, OnScriptError onScriptError) {
            return ctx -> new StringFieldScript(field, params, lookup, OnScriptError.FAIL, ctx) {
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
                return new StringFieldScript(leafFieldName, params, searchLookup, onScriptError, ctx) {
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
        StringFieldScript newInstance(LeafReaderContext ctx);
    }

    private final List<String> results = new ArrayList<>();
    private long chars;

    public StringFieldScript(
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
        results.clear();
        chars = 0;
    }

    public final void runForDoc(int docId, Consumer<String> consumer) {
        runForDoc(docId);
        results.forEach(consumer);
    }

    /**
     * Values from the last time runForDoc(int) was called. This list is mutable and will change with the next call of runForDoc(int).
     */
    public List<String> getValues() {
        return results;
    }

    @Override
    protected void emitValueFromCompositeScript(Object value) {
        if (value != null) {
            String string = value.toString();
            checkMaxChars(string);
            emit(string);
        }
    }

    @Override
    protected void emitFromObject(Object v) {
        if (v != null) {
            emit(v.toString());
        }
    }

    public final void emit(String v) {
        results.add(v);
    }

    private void checkMaxChars(String v) {
        chars += v.length();
        if (chars > MAX_CHARS) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Runtime field [%s] is emitting [%s] characters while the maximum number of values allowed is [%s]",
                    fieldName,
                    chars,
                    MAX_CHARS
                )
            );
        }
    }

    public static class Emit {
        private final StringFieldScript script;

        public Emit(StringFieldScript script) {
            this.script = script;
        }

        public void emit(String v) {
            script.checkMaxSize(script.results.size());
            script.checkMaxChars(v);
            script.emit(v);
        }
    }
}
