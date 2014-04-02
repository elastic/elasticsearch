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

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SignificantTermsParser implements Aggregator.Parser {

    public static final int DEFAULT_REQUIRED_SIZE = 10;
    public static final int DEFAULT_SHARD_SIZE = 0;

    //Typically need more than one occurrence of something for it to be statistically significant
    public static final int DEFAULT_MIN_DOC_COUNT = 3;
    
    static final ParseField BACKGROUND_FILTER = new ParseField("background_filter");

    static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final int DEFAULT_SHARD_MIN_DOC_COUNT = 1;

    @Override
    public String type() {
        return SignificantStringTerms.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, SignificantStringTerms.TYPE, context)
                .scriptable(false)
                .formattable(true)
                .requiresSortedValues(true)
                .requiresUniqueValues(true)
                .build();

        IncludeExclude.Parser incExcParser = new IncludeExclude.Parser(aggregationName, SignificantStringTerms.TYPE, context);

        Filter filter = null;
        int requiredSize = DEFAULT_REQUIRED_SIZE;
        int shardSize = DEFAULT_SHARD_SIZE;
        long minDocCount = DEFAULT_MIN_DOC_COUNT;
        long shardMinDocCount = DEFAULT_SHARD_MIN_DOC_COUNT;

        String executionHint = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (incExcParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("execution_hint".equals(currentFieldName) || "executionHint".equals(currentFieldName)) {
                    executionHint = parser.text();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                } else if ("shard_size".equals(currentFieldName) || "shardSize".equals(currentFieldName)) {
                    shardSize = parser.intValue();
                } else if ("min_doc_count".equals(currentFieldName) || "minDocCount".equals(currentFieldName)) {
                    minDocCount = parser.intValue();
                } else if (SHARD_MIN_DOC_COUNT_FIELD_NAME.match(currentFieldName)){
                    shardMinDocCount = parser.longValue();
                } else {
                        throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");

                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (BACKGROUND_FILTER.match(currentFieldName)) {
                    filter = context.queryParserService().parseInnerFilter(parser).filter();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");                    
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (shardSize == DEFAULT_SHARD_SIZE) {
            //The user has not made a shardSize selection .
            //Use default heuristic to avoid any wrong-ranking caused by distributed counting
            //but request double the usual amount.
            //We typically need more than the number of "top" terms requested by other aggregations
            //as the significance algorithm is in less of a position to down-select at shard-level -
            //some of the things we want to find have only one occurrence on each shard and as
            // such are impossible to differentiate from non-significant terms at that early stage.
            shardSize = 2 * BucketUtils.suggestShardSideQueueSize(requiredSize, context.numberOfShards());

        }

        // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
        if (shardSize < requiredSize) {
            shardSize = requiredSize;
        }

        // shard_min_doc_count should not be larger than min_doc_count because this can cause buckets to be removed that would match the min_doc_count criteria
        if (shardMinDocCount > minDocCount) {
            shardMinDocCount = minDocCount;
        }

        IncludeExclude includeExclude = incExcParser.includeExclude();
        return new SignificantTermsAggregatorFactory(aggregationName, vsParser.config(), requiredSize, shardSize, minDocCount, shardMinDocCount, includeExclude, executionHint, filter);
    }

}
