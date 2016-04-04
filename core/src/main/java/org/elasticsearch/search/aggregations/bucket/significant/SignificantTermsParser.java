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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParserMapper;
import org.elasticsearch.search.aggregations.bucket.terms.AbstractTermsParser;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class SignificantTermsParser extends AbstractTermsParser {
    private final SignificanceHeuristicParserMapper significanceHeuristicParserMapper;
    private final IndicesQueriesRegistry queriesRegistry;

    public SignificantTermsParser(SignificanceHeuristicParserMapper significanceHeuristicParserMapper,
            IndicesQueriesRegistry queriesRegistry) {
        this.significanceHeuristicParserMapper = significanceHeuristicParserMapper;
        this.queriesRegistry = queriesRegistry;
    }

    @Override
    protected SignificantTermsAggregatorBuilder doCreateFactory(String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, BucketCountThresholds bucketCountThresholds, SubAggCollectionMode collectMode, String executionHint,
            IncludeExclude incExc, Map<ParseField, Object> otherOptions) {
        SignificantTermsAggregatorBuilder factory = new SignificantTermsAggregatorBuilder(aggregationName, targetValueType);
        if (bucketCountThresholds != null) {
            factory.bucketCountThresholds(bucketCountThresholds);
        }
        if (executionHint != null) {
            factory.executionHint(executionHint);
        }
        if (incExc != null) {
            factory.includeExclude(incExc);
        }
        QueryBuilder<?> backgroundFilter = (QueryBuilder<?>) otherOptions.get(SignificantTermsAggregatorBuilder.BACKGROUND_FILTER);
        if (backgroundFilter != null) {
            factory.backgroundFilter(backgroundFilter);
        }
        SignificanceHeuristic significanceHeuristic = (SignificanceHeuristic) otherOptions.get(SignificantTermsAggregatorBuilder.HEURISTIC);
        if (significanceHeuristic != null) {
            factory.significanceHeuristic(significanceHeuristic);
        }
        return factory;
    }

    @Override
    public boolean parseSpecial(String aggregationName, XContentParser parser, ParseFieldMatcher parseFieldMatcher, Token token,
            String currentFieldName, Map<ParseField, Object> otherOptions) throws IOException {
        if (token == XContentParser.Token.START_OBJECT) {
            SignificanceHeuristicParser significanceHeuristicParser = significanceHeuristicParserMapper.get(currentFieldName);
            if (significanceHeuristicParser != null) {
                SignificanceHeuristic significanceHeuristic = significanceHeuristicParser.parse(parser, parseFieldMatcher);
                otherOptions.put(SignificantTermsAggregatorBuilder.HEURISTIC, significanceHeuristic);
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, SignificantTermsAggregatorBuilder.BACKGROUND_FILTER)) {
                QueryParseContext queryParseContext = new QueryParseContext(queriesRegistry);
                queryParseContext.reset(parser);
                queryParseContext.parseFieldMatcher(parseFieldMatcher);
                QueryBuilder<?> filter = queryParseContext.parseInnerQueryBuilder();
                otherOptions.put(SignificantTermsAggregatorBuilder.BACKGROUND_FILTER, filter);
                return true;
            }
        }
        return false;
    }

    @Override
    protected BucketCountThresholds getDefaultBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(SignificantTermsAggregatorBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS);
    }
}
