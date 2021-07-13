/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;
import java.util.function.Consumer;

public abstract class BooleanFieldScript extends AbstractFieldScript {

    public static final ScriptContext<Factory> CONTEXT = newContext("boolean_field", Factory.class);

    public static final BooleanFieldScript.Factory PARSE_FROM_SOURCE
        = (field, params, lookup) -> (BooleanFieldScript.LeafFactory) ctx -> new BooleanFieldScript
        (
            field,
            params,
            lookup,
            ctx
        ) {
        @Override
        public void execute() {
            emitFromSource();
        }
    };

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        BooleanFieldScript newInstance(LeafReaderContext ctx);
    }

    private int trues;
    private int falses;

    public BooleanFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
    }

    /**
     * Execute the script for the provided {@code docId}.
     */
    public final void runForDoc(int docId) {
        trues = 0;
        falses = 0;
        setDocument(docId);
        execute();
    }

    public final void runForDoc(int docId, Consumer<Boolean> consumer) {
        runForDoc(docId);
        int count = trues + falses;
        for (int i = 0; i < count; i++) {
            consumer.accept(i < falses ? false : true);
        }
    }

    /**
     * How many {@code true} values were returned for this document.
     */
    public final int trues() {
        return trues;
    }

    /**
     * How many {@code false} values were returned for this document.
     */
    public final int falses() {
        return falses;
    }

    protected final void emitFromObject(Object v) {
        if (v instanceof Boolean) {
            emit((Boolean) v);
        } else if (v instanceof String) {
            try {
                emit(Booleans.parseBoolean((String) v));
            } catch (IllegalArgumentException e) {
                // ignore
            }
        }
    }

    public final void emit(boolean v) {
        if (v) {
            trues++;
        } else {
            falses++;
        }
    }

    public static class Emit {
        private final BooleanFieldScript script;

        public Emit(BooleanFieldScript script) {
            this.script = script;
        }

        public void value(boolean v) {
            script.emit(v);
        }
    }
}
