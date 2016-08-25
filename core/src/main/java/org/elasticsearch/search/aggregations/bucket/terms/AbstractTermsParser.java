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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AbstractValuesSourceParser.AnyValuesSourceParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractTermsParser extends AnyValuesSourceParser {

    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");

    public IncludeExclude.Parser incExcParser = new IncludeExclude.Parser();

    protected AbstractTermsParser() {
        super(true, true);
    }

    @Override
    protected final ValuesSourceAggregationBuilder<ValuesSource, ?> createFactory(String aggregationName,
                                                                                  ValuesSourceType valuesSourceType,
                                                                                  ValueType targetValueType,
                                                                                  Map<ParseField, Object> otherOptions) {
        BucketCountThresholds bucketCountThresholds = getDefaultBucketCountThresholds();
        Integer requiredSize = (Integer) otherOptions.get(REQUIRED_SIZE_FIELD_NAME);
        if (requiredSize != null && requiredSize != -1) {
            bucketCountThresholds.setRequiredSize(requiredSize);
        }
        Integer shardSize = (Integer) otherOptions.get(SHARD_SIZE_FIELD_NAME);
        if (shardSize != null && shardSize != -1) {
            bucketCountThresholds.setShardSize(shardSize);
        }
        Long minDocCount = (Long) otherOptions.get(MIN_DOC_COUNT_FIELD_NAME);
        if (minDocCount != null && minDocCount != -1) {
            bucketCountThresholds.setMinDocCount(minDocCount);
        }
        Long shardMinDocCount = (Long) otherOptions.get(SHARD_MIN_DOC_COUNT_FIELD_NAME);
        if (shardMinDocCount != null && shardMinDocCount != -1) {
            bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        }
        SubAggCollectionMode collectMode = (SubAggCollectionMode) otherOptions.get(SubAggCollectionMode.KEY);
        String executionHint = (String) otherOptions.get(EXECUTION_HINT_FIELD_NAME);
        IncludeExclude incExc = incExcParser.createIncludeExclude(otherOptions);
        return doCreateFactory(aggregationName, valuesSourceType, targetValueType, bucketCountThresholds, collectMode, executionHint,
                incExc,
                otherOptions);
    }

    protected abstract ValuesSourceAggregationBuilder<ValuesSource, ?> doCreateFactory(String aggregationName,
                                                                                       ValuesSourceType valuesSourceType,
                                                                                       ValueType targetValueType,
                                                                                       BucketCountThresholds bucketCountThresholds,
                                                                                       SubAggCollectionMode collectMode,
                                                                                       String executionHint,
                                                                                       IncludeExclude incExc,
                                                                                       Map<ParseField, Object> otherOptions);

    @Override
    protected boolean token(String aggregationName, String currentFieldName, Token token, XContentParser parser,
            ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (incExcParser.token(currentFieldName, token, parser, parseFieldMatcher, otherOptions)) {
            return true;
        } else if (token == XContentParser.Token.VALUE_STRING) {
            if (parseFieldMatcher.match(currentFieldName, EXECUTION_HINT_FIELD_NAME)) {
                otherOptions.put(EXECUTION_HINT_FIELD_NAME, parser.text());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, SubAggCollectionMode.KEY)) {
                otherOptions.put(SubAggCollectionMode.KEY, SubAggCollectionMode.parse(parser.text(), parseFieldMatcher));
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, REQUIRED_SIZE_FIELD_NAME)) {
                otherOptions.put(REQUIRED_SIZE_FIELD_NAME, parser.intValue());
                return true;
            } else if (parseSpecial(aggregationName, parser, parseFieldMatcher, token, currentFieldName, otherOptions)) {
                return true;
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            if (parseFieldMatcher.match(currentFieldName, REQUIRED_SIZE_FIELD_NAME)) {
                otherOptions.put(REQUIRED_SIZE_FIELD_NAME, parser.intValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, SHARD_SIZE_FIELD_NAME)) {
                otherOptions.put(SHARD_SIZE_FIELD_NAME, parser.intValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, MIN_DOC_COUNT_FIELD_NAME)) {
                otherOptions.put(MIN_DOC_COUNT_FIELD_NAME, parser.longValue());
                return true;
            } else if (parseFieldMatcher.match(currentFieldName, SHARD_MIN_DOC_COUNT_FIELD_NAME)) {
                otherOptions.put(SHARD_MIN_DOC_COUNT_FIELD_NAME, parser.longValue());
                return true;
            } else if (parseSpecial(aggregationName, parser, parseFieldMatcher, token, currentFieldName, otherOptions)) {
                return true;
            }
        } else if (parseSpecial(aggregationName, parser, parseFieldMatcher, token, currentFieldName, otherOptions)) {
            return true;
        }
        return false;
    }

    public abstract boolean parseSpecial(String aggregationName, XContentParser parser, ParseFieldMatcher parseFieldMatcher,
            XContentParser.Token token, String currentFieldName, Map<ParseField, Object> otherOptions) throws IOException;

    protected abstract TermsAggregator.BucketCountThresholds getDefaultBucketCountThresholds();

}
