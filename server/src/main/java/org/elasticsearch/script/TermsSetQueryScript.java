/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class TermsSetQueryScript {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("terms_set", Factory.class);

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
            "doc", value -> {
                deprecationLogger.deprecate(DeprecationCategory.SCRIPTING, "terms-set-query-script_doc",
                        "Accessing variable [doc] via [params.doc] from within an terms-set-query-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_doc", value -> {
                deprecationLogger.deprecate(DeprecationCategory.SCRIPTING, "terms-set-query-script__doc",
                        "Accessing variable [doc] via [params._doc] from within an terms-set-query-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_source", value -> ((SourceLookup)value).source()
    );

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    /**
     * A leaf lookup for the bound segment this script will operate on.
     */
    private final LeafSearchLookup leafLookup;

    public TermsSetQueryScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        Map<String, Object> parameters = new HashMap<>(params);
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        parameters.putAll(leafLookup.asMap());
        this.params = new DynamicMap(parameters, PARAMS_FUNCTIONS);
    }

    protected TermsSetQueryScript() {
        params = null;
        leafLookup = null;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * The doc lookup for the Lucene segment this script was created for.
     */
    public Map<String, ScriptDocValues<?>> getDoc() {
        return leafLookup.doc();
    }

    /**
     * Set the current document to run the script on next.
     */
    public void setDocument(int docid) {
        leafLookup.setDocument(docid);
    }

    /**
     * Return the result as a long. This is used by aggregation scripts over long fields.
     */
    public long runAsLong() {
        return execute().longValue();
    }

    public abstract Number execute();

    /**
     * A factory to construct {@link TermsSetQueryScript} instances.
     */
    public interface LeafFactory {
        TermsSetQueryScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    /**
     * A factory to construct stateful {@link TermsSetQueryScript} factories for a specific index.
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }
}
