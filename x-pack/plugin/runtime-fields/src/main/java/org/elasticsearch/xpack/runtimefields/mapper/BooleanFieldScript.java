/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

public abstract class BooleanFieldScript extends AbstractFieldScript {

    public static final ScriptContext<Factory> CONTEXT = newContext("boolean_script_field", Factory.class);

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        BooleanFieldScript newInstance(LeafReaderContext ctx);
    }

    public static final Factory PARSE_FROM_SOURCE = (field, params, lookup) -> (LeafFactory) ctx -> new BooleanFieldScript(
        field,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            for (Object v : extractFromSource(field)) {
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
        }
    };

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

    protected final void emit(boolean v) {
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
