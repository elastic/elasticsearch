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

/**
 * A script to produce dynamic values for return fields.
 */
public abstract class FieldScript {

    public static final String[] PARAMETERS = {};

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
            "doc", value -> {
                deprecationLogger.deprecate(DeprecationCategory.SCRIPTING, "field-script_doc",
                        "Accessing variable [doc] via [params.doc] from within an field-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_doc", value -> {
                deprecationLogger.deprecate(DeprecationCategory.SCRIPTING, "field-script__doc",
                        "Accessing variable [doc] via [params._doc] from within an field-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_source", value -> ((SourceLookup)value).source()
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /** A leaf lookup for the bound segment this script will operate on. */
    private final LeafSearchLookup leafLookup;

    private final FieldProxy fieldProxy;

    public FieldScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        params = new HashMap<>(params);
        params.putAll(leafLookup.asMap());
        this.params = new DynamicMap(params, PARAMS_FUNCTIONS);
        this.fieldProxy = new ReadDocValuesFieldProxy(this.leafLookup);
    }

    // for expression engine
    protected FieldScript() {
        params = null;
        leafLookup = null;
        fieldProxy = null;
    }

    public abstract Object execute();

    /** The leaf lookup for the Lucene segment this script was created for. */
    protected final LeafSearchLookup getLeafLookup() {
        return leafLookup;
    }

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

    /** A factory to construct {@link FieldScript} instances. */
    public interface LeafFactory {
        FieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }

    /** The context used to compile {@link FieldScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("field", Factory.class);
}
