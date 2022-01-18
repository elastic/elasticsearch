/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.WeightedSpanTerm;
import org.apache.lucene.search.highlight.WeightedSpanTermExtractor;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;

import java.io.IOException;
import java.util.Map;

public final class CustomQueryScorer extends QueryScorer {

    public CustomQueryScorer(Query query, IndexReader reader, String field, String defaultField) {
        super(query, reader, field, defaultField);
    }

    public CustomQueryScorer(Query query, IndexReader reader, String field) {
        super(query, reader, field);
    }

    public CustomQueryScorer(Query query, String field, String defaultField) {
        super(query, field, defaultField);
    }

    public CustomQueryScorer(Query query, String field) {
        super(query, field);
    }

    public CustomQueryScorer(Query query) {
        super(query);
    }

    public CustomQueryScorer(WeightedSpanTerm[] weightedTerms) {
        super(weightedTerms);
    }

    @Override
    protected WeightedSpanTermExtractor newTermExtractor(String defaultField) {
        return defaultField == null ? new CustomWeightedSpanTermExtractor() : new CustomWeightedSpanTermExtractor(defaultField);
    }

    private static class CustomWeightedSpanTermExtractor extends WeightedSpanTermExtractor {

        CustomWeightedSpanTermExtractor() {
            super();
        }

        CustomWeightedSpanTermExtractor(String defaultField) {
            super(defaultField);
        }

        @Override
        protected void extractUnknownQuery(Query query, Map<String, WeightedSpanTerm> terms) throws IOException {
            extractWeightedTerms(terms, query, 1F);
        }

        protected void extract(Query query, float boost, Map<String, WeightedSpanTerm> terms) throws IOException {
            if (isChildOrParentQuery(query.getClass())) {
                // skip has_child or has_parent queries, see: https://github.com/elastic/elasticsearch/issues/14999
                return;
            } else if (query instanceof FunctionScoreQuery) {
                super.extract(((FunctionScoreQuery) query).getSubQuery(), boost, terms);
            } else if (query instanceof ESToParentBlockJoinQuery) {
                super.extract(((ESToParentBlockJoinQuery) query).getChildQuery(), boost, terms);
            } else {
                super.extract(query, boost, terms);
            }
        }

        /**
         * Workaround to detect parent/child query
         */
        private static final String PARENT_CHILD_QUERY_NAME = "LateParsingQuery";

        private static boolean isChildOrParentQuery(Class<?> clazz) {
            return clazz.getName().endsWith(PARENT_CHILD_QUERY_NAME);
        }
    }
}
