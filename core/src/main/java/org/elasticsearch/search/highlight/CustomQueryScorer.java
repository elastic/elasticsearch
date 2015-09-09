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

package org.elasticsearch.search.highlight;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.WeightedSpanTerm;
import org.apache.lucene.search.highlight.WeightedSpanTermExtractor;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;

import java.io.IOException;
import java.util.Map;

public final class CustomQueryScorer extends QueryScorer {

    public CustomQueryScorer(Query query, IndexReader reader, String field,
                             String defaultField) {
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
        return defaultField == null ? new CustomWeightedSpanTermExtractor()
                : new CustomWeightedSpanTermExtractor(defaultField);
    }

    private static class CustomWeightedSpanTermExtractor extends WeightedSpanTermExtractor {

        public CustomWeightedSpanTermExtractor() {
            super();
        }

        public CustomWeightedSpanTermExtractor(String defaultField) {
            super(defaultField);
        }

        @Override
        protected void extractUnknownQuery(Query query,
                                           Map<String, WeightedSpanTerm> terms) throws IOException {
            if (query instanceof FunctionScoreQuery) {
                query = ((FunctionScoreQuery) query).getSubQuery();
                extract(query, query.getBoost(), terms);
            } else if (query instanceof FiltersFunctionScoreQuery) {
                query = ((FiltersFunctionScoreQuery) query).getSubQuery();
                extract(query, query.getBoost(), terms);
            } else if (query instanceof FilteredQuery) {
                query = ((FilteredQuery) query).getQuery();
                extract(query, 1F, terms);
            } else {
                extractWeightedTerms(terms, query, query.getBoost());
            }
        }

    }

}
