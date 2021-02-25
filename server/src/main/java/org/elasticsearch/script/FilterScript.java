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
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

/**
 * A script implementation of a query filter.
 * See {@link org.elasticsearch.index.query.ScriptQueryBuilder}.
 */
public abstract class FilterScript {

    // no parameters for execute, but constant still required...
    public static final String[] PARAMETERS = {};

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /** A leaf lookup for the bound segment this script will operate on. */
    private final LeafSearchLookup leafLookup;

    public FilterScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        this.params = params;
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
    }

    /** Return {@code true} if the current document matches the filter, or {@code false} otherwise. */
    public abstract boolean execute();

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** The doc lookup for the Lucene segment this script was created for. */
    public final Map<String, ScriptDocValues<?>> getDoc() {
        return leafLookup.doc();
    }

    /** Set the current document to run the script on next. */
    public void setDocument(int docid) {
        leafLookup.setDocument(docid);
    }

    /** A factory to construct {@link FilterScript} instances. */
    public interface LeafFactory {
        FilterScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    /** A factory to construct stateful {@link FilterScript} factories for a specific index. */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }

    /** The context used to compile {@link FilterScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("filter", Factory.class);
}
