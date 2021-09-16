/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

public abstract class LongSortScript extends AbstractSortScript {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("long_sort", Factory.class);

    public LongSortScript(Map<String, Object> params, SearchLookup searchLookup, DocReader docReader) {
        // searchLookup is used taken in for compatibility with expressions.  See ExpressionScriptEngine.newScoreScript and
        // ExpressionScriptEngine.getDocValueSource for where it's used.
        super(params, docReader);
    }

    protected LongSortScript() {
        super();
    }

    public abstract long execute();

    /**
     * A factory to construct {@link LongSortScript} instances.
     */
    public interface LeafFactory {
        LongSortScript newInstance(DocReader reader) throws IOException;

        /**
         * Return {@code true} if the script needs {@code _score} calculated, or {@code false} otherwise.
         */
        boolean needs_score();
    }

    /**
     * A factory to construct stateful {@link LongSortScript} factories for a specific index.
     */
    public interface Factory extends ScriptFactory {
        // searchLookup is needed for **expressions-only** to look up bindings.  Painless callers should use the DocReader
        // in LeafFactory.newInstance to set fallbacks.
        LeafFactory newFactory(Map<String, Object> params, SearchLookup searchLookup);
    }
}
