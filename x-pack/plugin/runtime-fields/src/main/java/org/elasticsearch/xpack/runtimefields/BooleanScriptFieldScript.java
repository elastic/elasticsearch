/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class BooleanScriptFieldScript extends AbstractScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("boolean_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "boolean_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        BooleanScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    private int trues;
    private int falses;

    public BooleanScriptFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
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

    protected final void emitValue(boolean v) {
        if (v) {
            trues++;
        } else {
            falses++;
        }
    }

    public static boolean parse(Object str) {
        return Booleans.parseBoolean(str.toString());
    }

    public static class EmitValue {
        private final BooleanScriptFieldScript script;

        public EmitValue(BooleanScriptFieldScript script) {
            this.script = script;
        }

        public void value(boolean v) {
            script.emitValue(v);
        }
    }
}
