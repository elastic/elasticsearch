/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.script;

import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * A script implementation of a query filter.
 * See {@link org.elasticsearch.index.query.ScriptQueryBuilder}.
 */
public abstract class FilterScript extends DocBasedScript {

    // no parameters for execute, but constant still required...
    public static final String[] PARAMETERS = {};

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public FilterScript(Map<String, Object> params, SearchLookup lookup, DocReader docReader) {
        // searchLookup is taken in for compatibility with expressions. See ExpressionScriptEngine.newFilterScript and
        // ExpressionScriptEngine.getDocValueSource for where it's used.
        super(docReader);
        this.params = params;
    }

    /** Return {@code true} if the current document matches the filter, or {@code false} otherwise. */
    public abstract boolean execute();

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** Set the current document to run the script on next. */
    public void setDocument(int docid) {
        docReader.setDocument(docid);
    }

    /** A factory to construct {@link FilterScript} instances. */
    public interface LeafFactory {
        FilterScript newInstance(DocReader docReader) throws IOException;
    }

    /** A factory to construct stateful {@link FilterScript} factories for a specific index. */
    public interface Factory extends ScriptFactory {
        // searchLookup is taken in for compatibility with expressions. See ExpressionScriptEngine.newFilterScript and
        // ExpressionScriptEngine.getDocValueSource for where it's used.
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }

    /** The context used to compile {@link FilterScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("filter", Factory.class);
}
