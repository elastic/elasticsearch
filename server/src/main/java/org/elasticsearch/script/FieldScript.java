/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.LeafReaderContext;
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

    private static final DeprecationLogger deprecationLogger =
            new DeprecationLogger(LogManager.getLogger(DynamicMap.class));
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
            "doc", value -> {
                deprecationLogger.deprecatedAndMaybeLog("field-script_doc",
                        "Accessing variable [doc] via [params.doc] from within an field-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_doc", value -> {
                deprecationLogger.deprecatedAndMaybeLog("field-script__doc",
                        "Accessing variable [doc] via [params._doc] from within an field-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_source", value -> ((SourceLookup)value).loadSourceIfNeeded()
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /** A leaf lookup for the bound segment this script will operate on. */
    private final LeafSearchLookup leafLookup;

    public FieldScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        params = new HashMap<>(params);
        params.putAll(leafLookup.asMap());
        this.params = new DynamicMap(params, PARAMS_FUNCTIONS);
    }

    // for expression engine
    protected FieldScript() {
        params = null;
        leafLookup = null;
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
