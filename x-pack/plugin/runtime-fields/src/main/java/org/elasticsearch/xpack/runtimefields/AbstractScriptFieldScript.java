/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Map;

/**
 * Abstract base for scripts to execute to build scripted fields. Inspired by
 * {@link AggregationScript} but hopefully with less historical baggage.
 */
public abstract class AbstractScriptFieldScript {
    private final Map<String, Object> params;
    private final LeafSearchLookup leafSearchLookup;

    public AbstractScriptFieldScript(Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        this.leafSearchLookup = searchLookup.getLeafSearchLookup(ctx);
        // TODO how do other scripts get stored fields exposed? Through asMap? I don't see any getters for them.
        this.params = params;
    }

    /**
     * Set the document to run the script against.
     */
    public final void setDocument(int docId) {
        this.leafSearchLookup.setDocument(docId);
    }

    /**
     * Expose the {@code params} of the script to the script itself.
     */
    public final Map<String, Object> getParams() {
        return params;
    }

    /**
     * Expose the {@code _source} to the script.
     */
    public final Map<String, Object> getSource() {
        return leafSearchLookup.source();
    }

    /**
     * Expose field data to the script as {@code doc}.
     */
    public final Map<String, ScriptDocValues<?>> getDoc() {
        return leafSearchLookup.doc();
    }

    public abstract void execute();
}
