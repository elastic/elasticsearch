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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParserMapper;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SignificantTermsParser implements Aggregator.Parser {

    private final SignificanceHeuristicParserMapper significanceHeuristicParserMapper;

    @Inject
    public SignificantTermsParser(SignificanceHeuristicParserMapper significanceHeuristicParserMapper) {
        this.significanceHeuristicParserMapper = significanceHeuristicParserMapper;
    }

    @Override
    public String type() {
        return SignificantStringTerms.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        SignificantTermsParametersParser aggParser = new SignificantTermsParametersParser(significanceHeuristicParserMapper);
        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, SignificantStringTerms.TYPE, context)
                .scriptable(false)
                .formattable(true)
                .build();
        IncludeExclude.Parser incExcParser = new IncludeExclude.Parser();
        aggParser.parse(aggregationName, parser, context, vsParser, incExcParser);

        TermsAggregator.BucketCountThresholds bucketCountThresholds = aggParser.getBucketCountThresholds();
        if (bucketCountThresholds.getShardSize() == aggParser.getDefaultBucketCountThresholds().getShardSize()) {
            //The user has not made a shardSize selection .
            //Use default heuristic to avoid any wrong-ranking caused by distributed counting
            //but request double the usual amount.
            //We typically need more than the number of "top" terms requested by other aggregations
            //as the significance algorithm is in less of a position to down-select at shard-level -
            //some of the things we want to find have only one occurrence on each shard and as
            // such are impossible to differentiate from non-significant terms at that early stage.
            bucketCountThresholds.setShardSize(2 * BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize(), context.numberOfShards()));
        }

        bucketCountThresholds.ensureValidity();
        SignificanceHeuristic significanceHeuristic = aggParser.getSignificanceHeuristic();
        if (significanceHeuristic == null) {
            significanceHeuristic = JLHScore.INSTANCE;
        }
        return new SignificantTermsAggregatorFactory(aggregationName, vsParser.config(), bucketCountThresholds, aggParser.getIncludeExclude(), aggParser.getExecutionHint(), aggParser.getFilter(), significanceHeuristic);
    }
}
