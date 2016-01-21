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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public abstract class AbstractTermsParametersParser {

    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");
    public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");
    

    //These are the results of the parsing.
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds();

    private String executionHint = null;
    
    private SubAggCollectionMode collectMode = SubAggCollectionMode.DEPTH_FIRST;


    IncludeExclude includeExclude;

    public TermsAggregator.BucketCountThresholds getBucketCountThresholds() {return bucketCountThresholds;}

    //These are the results of the parsing.

    public String getExecutionHint() {
        return executionHint;
    }

    public IncludeExclude getIncludeExclude() {
        return includeExclude;
    }
    
    public SubAggCollectionMode getCollectionMode() {
        return collectMode;
    }

    public void parse(String aggregationName, XContentParser parser, SearchContext context, ValuesSourceParser vsParser, IncludeExclude.Parser incExcParser) throws IOException {
        bucketCountThresholds = getDefaultBucketCountThresholds();
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
                if (context.parseFieldMatcher().match(currentFieldName, EXECUTION_HINT_FIELD_NAME)) {
                    executionHint = parser.text();
                } else if(context.parseFieldMatcher().match(currentFieldName, SubAggCollectionMode.KEY)){
                    collectMode = SubAggCollectionMode.parse(parser.text(), context.parseFieldMatcher());
                } else if (context.parseFieldMatcher().match(currentFieldName, REQUIRED_SIZE_FIELD_NAME)) {
                    bucketCountThresholds.setRequiredSize(parser.intValue());
                } else {
                    parseSpecial(aggregationName, parser, context, token, currentFieldName);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (context.parseFieldMatcher().match(currentFieldName, REQUIRED_SIZE_FIELD_NAME)) {
                    bucketCountThresholds.setRequiredSize(parser.intValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, SHARD_SIZE_FIELD_NAME)) {
                    bucketCountThresholds.setShardSize(parser.intValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, MIN_DOC_COUNT_FIELD_NAME)) {
                    bucketCountThresholds.setMinDocCount(parser.intValue());
                } else if (context.parseFieldMatcher().match(currentFieldName, SHARD_MIN_DOC_COUNT_FIELD_NAME)) {
                    bucketCountThresholds.setShardMinDocCount(parser.longValue());
                } else {
                    parseSpecial(aggregationName, parser, context, token, currentFieldName);
                }
            } else {
                parseSpecial(aggregationName, parser, context, token, currentFieldName);
            }
        }
        includeExclude = incExcParser.includeExclude();
    }

    public abstract void parseSpecial(String aggregationName, XContentParser parser, SearchContext context, XContentParser.Token token, String currentFieldName) throws IOException;

    protected abstract TermsAggregator.BucketCountThresholds getDefaultBucketCountThresholds();
}
