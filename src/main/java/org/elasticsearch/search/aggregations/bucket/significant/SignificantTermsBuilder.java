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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

/**
 * Creates an aggregation that finds interesting or unusual occurrences of terms in a result set.
 * <p/>
 * This feature is marked as experimental, and may be subject to change in the future.  If you
 * use this feature, please let us know your experience with it!
 */
public class SignificantTermsBuilder extends AggregationBuilder<SignificantTermsBuilder> {

    private String field;
    private int requiredSize = SignificantTermsParser.DEFAULT_REQUIRED_SIZE;
    private int shardSize = SignificantTermsParser.DEFAULT_SHARD_SIZE;
    private int minDocCount = SignificantTermsParser.DEFAULT_MIN_DOC_COUNT;
    private int shardMinDocCount = SignificantTermsParser.DEFAULT_SHARD_MIN_DOC_COUNT;
    private String executionHint;
    private String includePattern;
    private int includeFlags;
    private String excludePattern;
    private int excludeFlags;

    public SignificantTermsBuilder(String name) {
        super(name, SignificantStringTerms.TYPE.name());
    }

    public SignificantTermsBuilder field(String field) {
        this.field = field;
        return this;
    }

    public SignificantTermsBuilder size(int requiredSize) {
        this.requiredSize = requiredSize;
        return this;
    }

    public SignificantTermsBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }

    public SignificantTermsBuilder minDocCount(int minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    public SignificantTermsBuilder shardMinDocCount(int shardMinDocCount) {
        this.shardMinDocCount = shardMinDocCount;
        return this;
    }

    public SignificantTermsBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Define a regular expression that will determine what terms should be aggregated. The regular expression is based
     * on the {@link java.util.regex.Pattern} class.
     *
     * @see #include(String, int)
     */
    public SignificantTermsBuilder include(String regex) {
        return include(regex, 0);
    }

    /**
     * Define a regular expression that will determine what terms should be aggregated. The regular expression is based
     * on the {@link java.util.regex.Pattern} class.
     *
     * @see java.util.regex.Pattern#compile(String, int)
     */
    public SignificantTermsBuilder include(String regex, int flags) {
        this.includePattern = regex;
        this.includeFlags = flags;
        return this;
    }

    /**
     * Define a regular expression that will filter out terms that should be excluded from the aggregation. The regular
     * expression is based on the {@link java.util.regex.Pattern} class.
     *
     * @see #exclude(String, int)
     */
    public SignificantTermsBuilder exclude(String regex) {
        return exclude(regex, 0);
    }

    /**
     * Define a regular expression that will filter out terms that should be excluded from the aggregation. The regular
     * expression is based on the {@link java.util.regex.Pattern} class.
     *
     * @see java.util.regex.Pattern#compile(String, int)
     */
    public SignificantTermsBuilder exclude(String regex, int flags) {
        this.excludePattern = regex;
        this.excludeFlags = flags;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (minDocCount != SignificantTermsParser.DEFAULT_MIN_DOC_COUNT) {
            builder.field("minDocCount", minDocCount);
        }
        if (shardMinDocCount != SignificantTermsParser.DEFAULT_SHARD_MIN_DOC_COUNT) {
            builder.field("shardMinDocCount", shardMinDocCount);
        }
        if (requiredSize != SignificantTermsParser.DEFAULT_REQUIRED_SIZE) {
            builder.field("size", requiredSize);
        }
        if (shardSize != SignificantTermsParser.DEFAULT_SHARD_SIZE) {
            builder.field("shard_size", shardSize);
        }
        if (executionHint != null) {
            builder.field("execution_hint", executionHint);
        }
        if (includePattern != null) {
            if (includeFlags == 0) {
                builder.field("include", includePattern);
            } else {
                builder.startObject("include")
                        .field("pattern", includePattern)
                        .field("flags", includeFlags)
                        .endObject();
            }
        }
        if (excludePattern != null) {
            if (excludeFlags == 0) {
                builder.field("exclude", excludePattern);
            } else {
                builder.startObject("exclude")
                        .field("pattern", excludePattern)
                        .field("flags", excludeFlags)
                        .endObject();
            }
        }

        return builder.endObject();
    }

}
