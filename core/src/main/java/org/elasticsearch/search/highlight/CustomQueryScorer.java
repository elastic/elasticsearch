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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.WeightedSpanTerm;
import org.apache.lucene.search.highlight.WeightedSpanTermExtractor;
import org.apache.lucene.spatial.geopoint.search.GeoPointInBBoxQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.HasChildQueryParser;

import java.io.IOException;
import java.util.Map;

public final class CustomQueryScorer extends QueryScorer {

    private static final Class<?> unsupportedGeoQuery;

    static {
        try {
            // in extract() we need to check for GeoPointMultiTermQuery and skip extraction for queries that inherit from it.
            // But GeoPointMultiTermQuerythat is package private in Lucene hence we cannot use an instanceof check. This is why
            // we use this rather ugly workaround to get a Class and later be able to compare with isAssignableFrom().
            unsupportedGeoQuery = Class.forName("org.apache.lucene.spatial.geopoint.search.GeoPointMultiTermQuery");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }

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
            } else if (terms.isEmpty()) {
                extractWeightedTerms(terms, query, query.getBoost());
            }
        }

        protected void extract(Query query, float boost, Map<String, WeightedSpanTerm> terms) throws IOException {
            if (query instanceof GeoPointInBBoxQuery || unsupportedGeoQuery.isAssignableFrom(query.getClass())) {
                // skip all geo queries, see https://issues.apache.org/jira/browse/LUCENE-7293 and
                // https://github.com/elastic/elasticsearch/issues/17537
                return;
            } else if (query instanceof HasChildQueryParser.LateParsingQuery) {
                // skip has_child or has_parent queries, see: https://github.com/elastic/elasticsearch/issues/14999
                return;
            } else if (query instanceof FunctionScoreQuery) {
                super.extract(((FunctionScoreQuery) query).getSubQuery(), boost, terms);
            } else {
                super.extract(query, boost, terms);
            }
        }
    }
}
