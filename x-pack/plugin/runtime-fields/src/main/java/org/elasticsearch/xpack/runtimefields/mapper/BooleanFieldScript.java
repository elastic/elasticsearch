/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;
import java.util.Map;

public abstract class BooleanFieldScript extends AbstractFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("boolean_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "boolean_whitelist.txt"));
    }

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
