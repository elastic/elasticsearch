/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.SearchLookup;

public abstract class NumberSortScript extends AbstractSortScript {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("number_sort", Factory.class);

    public NumberSortScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
    }

    protected NumberSortScript() {
        super();
    }

    public abstract double execute();

    /**
     * A factory to construct {@link NumberSortScript} instances.
     */
    public interface LeafFactory {
        NumberSortScript newInstance(LeafReaderContext ctx) throws IOException;

        /**
         * Return {@code true} if the script needs {@code _score} calculated, or {@code false} otherwise.
         */
        boolean needs_score();
    }

    /**
     * A factory to construct stateful {@link NumberSortScript} factories for a specific index.
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }

    public static class FieldAccess {
        private final NumberSortScript script;

        public FieldAccess(NumberSortScript script) {
            this.script = script;
        }

        public Field<?> field(String fieldName) {
            return script.field(fieldName);
        }
    }
}
