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
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A script to produce dynamic values for return fields.
 */
public abstract class FieldScript extends DocBasedScript {

    public static final String[] PARAMETERS = {};

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);

    @SuppressWarnings("unchecked")
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of("doc", value -> {
        deprecationLogger.warn(
            DeprecationCategory.SCRIPTING,
            "field-script_doc",
            "Accessing variable [doc] via [params.doc] from within an field-script " + "is deprecated in favor of directly accessing [doc]."
        );
        return value;
    }, "_doc", value -> {
        deprecationLogger.warn(
            DeprecationCategory.SCRIPTING,
            "field-script__doc",
            "Accessing variable [doc] via [params._doc] from within an field-script "
                + "is deprecated in favor of directly accessing [doc]."
        );
        return value;
    }, "_source", value -> ((Supplier<Source>) value).get().source());

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public FieldScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        this(params, new DocValuesDocReader(lookup, leafContext));
    }

    private FieldScript(Map<String, Object> params, DocReader docReader) {
        super(docReader);
        params = new HashMap<>(params);
        params.putAll(docReader.docAsMap());
        this.params = new DynamicMap(params, PARAMS_FUNCTIONS);
    }

    // for expression engine
    protected FieldScript() {
        super(null);
        params = null;
    }

    public abstract Object execute();

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
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
