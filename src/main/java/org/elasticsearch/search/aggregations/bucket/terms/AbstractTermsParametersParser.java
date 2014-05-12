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
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

public abstract class AbstractTermsParametersParser {

    public static final int DEFAULT_REQUIRED_SIZE = 10;
    public static final int DEFAULT_SHARD_SIZE = -1;

    //Typically need more than one occurrence of something for it to be statistically significant
    public static final int DEFAULT_MIN_DOC_COUNT = 1;
    public static final int DEFAULT_SHARD_MIN_DOC_COUNT = 1;

    static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");

    public int getRequiredSize() {
        return requiredSize;
    }

    public int getShardSize() {
        return shardSize;
    }

    public void setMinDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
    }

    public long getMinDocCount() {
        return minDocCount;
    }

    public long getShardMinDocCount() {
        return shardMinDocCount;
    }

    //These are the results of the parsing.

    public String getExecutionHint() {
        return executionHint;
    }

    public IncludeExclude getIncludeExclude() {
        return includeExclude;
    }

    private int requiredSize = DEFAULT_REQUIRED_SIZE;
    private int shardSize = DEFAULT_SHARD_SIZE;
    private long minDocCount = DEFAULT_MIN_DOC_COUNT;
    private long shardMinDocCount = DEFAULT_SHARD_MIN_DOC_COUNT;
    private String executionHint = null;
    IncludeExclude includeExclude;

    public void parse(String aggregationName, XContentParser parser, SearchContext context, ValuesSourceParser vsParser, IncludeExclude.Parser incExcParser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        setDefaults();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (incExcParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (EXECUTION_HINT_FIELD_NAME.match(currentFieldName)) {
                    executionHint = parser.text();
                } else {
                    parseSpecial(aggregationName, parser, context, token, currentFieldName);
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("size".equals(currentFieldName)) {
                    requiredSize = parser.intValue();
                } else if (SHARD_SIZE_FIELD_NAME.match(currentFieldName)) {
                    shardSize = parser.intValue();
                } else if (MIN_DOC_COUNT_FIELD_NAME.match(currentFieldName)) {
                    minDocCount = parser.intValue();
                } else if (SHARD_MIN_DOC_COUNT_FIELD_NAME.match(currentFieldName)) {
                    shardMinDocCount = parser.longValue();
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

    public abstract void setDefaults();
}
