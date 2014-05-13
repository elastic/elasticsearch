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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SignificantTermsParser implements Aggregator.Parser {

    @Override
    public String type() {
        return SignificantStringTerms.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        SignificantTermsParametersParser aggParser = new SignificantTermsParametersParser();
        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, SignificantStringTerms.TYPE, context)
                .scriptable(false)
                .formattable(true)
                .requiresSortedValues(true)
                .requiresUniqueValues(true)
                .build();
        IncludeExclude.Parser incExcParser = new IncludeExclude.Parser(aggregationName, SignificantStringTerms.TYPE, context);
        aggParser.parse(aggregationName, parser, context, vsParser, incExcParser);

        TermsAggregator.BucketCountThresholds bucketCountThresholds = aggParser.getBucketCountThresholds();
        if ( bucketCountThresholds.shardSize == aggParser.getDefaultBucketCountThresholds().shardSize) {
            //The user has not made a shardSize selection .
            //Use default heuristic to avoid any wrong-ranking caused by distributed counting
            //but request double the usual amount.
            //We typically need more than the number of "top" terms requested by other aggregations
            //as the significance algorithm is in less of a position to down-select at shard-level -
            //some of the things we want to find have only one occurrence on each shard and as
            // such are impossible to differentiate from non-significant terms at that early stage.
            bucketCountThresholds.shardSize = 2 * BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.requiredSize, context.numberOfShards());

        }

        // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
        if (bucketCountThresholds.shardSize < bucketCountThresholds.requiredSize) {
            bucketCountThresholds.shardSize = bucketCountThresholds.requiredSize;
        }

        // shard_min_doc_count should not be larger than min_doc_count because this can cause buckets to be removed that would match the min_doc_count criteria
        if (bucketCountThresholds.shardMinDocCount > bucketCountThresholds.minDocCount) {
            bucketCountThresholds.shardMinDocCount = bucketCountThresholds.minDocCount;
        }
        return new SignificantTermsAggregatorFactory(aggregationName, vsParser.config(), bucketCountThresholds, aggParser.getIncludeExclude(), aggParser.getExecutionHint(), aggParser.getFilter());
    }
}
