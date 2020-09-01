/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class StringScriptFieldScript extends AbstractScriptFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("string_script_field", Factory.class);

    /**
     * The maximum number of {@code char}s we support collecting in a string
     * runtime field.
     * <p>
     * Note: This is calculated with {@link String#length()} so it is a fairly
     * direct measure of memory usage but, because java uses utf-16 for these
     * chars it is fairly esoteric to talk about.
     */
    public static final long MAX_CHARS = 1024 * 1024;

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "string_whitelist.txt"));
    }

    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        StringScriptFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    private final List<String> results = new ArrayList<>();
    private long chars;

    public StringScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(params, searchLookup, ctx);
    }

    /**
     * Execute the script for the provided {@code docId}.
     * <p>
     * @return a mutable {@link List} that contains the results of the script
     * and will be modified the next time you call {@linkplain #resultsForDoc}.
     */
    public final List<String> resultsForDoc(int docId) {
        results.clear();
        chars = 0;
        setDocument(docId);
        execute();
        return results;
    }

    protected final void emitValue(String v) {
        if (results.size() >= MAX_VALUES) {
            throw new IllegalArgumentException("too many runtime values");
        }
        chars += v.length();
        if (chars >= MAX_CHARS) {
            throw new IllegalArgumentException("too many characters in runtime values [" + chars + "]");
        }
        results.add(v);
    }

    public static class EmitValue {
        private final StringScriptFieldScript script;

        public EmitValue(StringScriptFieldScript script) {
            this.script = script;
        }

        public void emitValue(String v) {
            script.emitValue(v);
        }
    }
}
