/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

abstract class AbstractSortScript extends DocBasedScript implements ScorerAware {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
            "doc", value -> {
                deprecationLogger.deprecate(DeprecationCategory.SCRIPTING, "sort-script_doc",
                        "Accessing variable [doc] via [params.doc] from within an sort-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_doc", value -> {
                deprecationLogger.deprecate(DeprecationCategory.SCRIPTING, "sort-script__doc",
                        "Accessing variable [doc] via [params._doc] from within an sort-script "
                                + "is deprecated in favor of directly accessing [doc].");
                return value;
            },
            "_source", value -> ((SourceLookup)value).source()
    );

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    /** A scorer that will return the score for the current document when the script is run. */
    private Scorable scorer;

    AbstractSortScript(Map<String, Object> params, DocReader docReader) {
        super(docReader);
        Map<String, Object> parameters = new HashMap<>(params);
        parameters.putAll(docReader.docAsMap());
        this.params = new DynamicMap(parameters, PARAMS_FUNCTIONS);
    }

    protected AbstractSortScript() {
        super(null);
        this.params = null;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public void setScorer(Scorable scorer) {
        this.scorer = scorer;
    }

    /** Return the score of the current document. */
    public double get_score() {
        try {
            return scorer.score();
        } catch (IOException e) {
            throw new ElasticsearchException("couldn't lookup score", e);
        }
    }
}
