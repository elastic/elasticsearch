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
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.AbstractTermsParametersParser;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;

import java.io.IOException;

/**
 * Creates an aggregation that finds interesting or unusual occurrences of terms in a result set.
 * <p/>
 * This feature is marked as experimental, and may be subject to change in the future.  If you
 * use this feature, please let us know your experience with it!
 */
public class SignificantTermsBuilder extends AggregationBuilder<SignificantTermsBuilder> {

    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(-1, -1, -1, -1);

    private String field;
    private String executionHint;
    private String includePattern;
    private int includeFlags;
    private String excludePattern;
    private int excludeFlags;
    private FilterBuilder filterBuilder;
    private SignificanceHeuristicBuilder significanceHeuristicBuilder;


    public SignificantTermsBuilder(String name) {
        super(name, SignificantStringTerms.TYPE.name());
    }

    public SignificantTermsBuilder field(String field) {
        this.field = field;
        return this;
    }

    public SignificantTermsBuilder size(int requiredSize) {
        bucketCountThresholds.setRequiredSize(requiredSize);
        return this;
    }

    public SignificantTermsBuilder shardSize(int shardSize) {
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    public SignificantTermsBuilder minDocCount(int minDocCount) {
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }
    
    public SignificantTermsBuilder backgroundFilter(FilterBuilder filter) {
        this.filterBuilder = filter;
        return this;
    }
    

    public SignificantTermsBuilder shardMinDocCount(int shardMinDocCount) {
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
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
        bucketCountThresholds.toXContent(builder);
        if (executionHint != null) {
            builder.field(AbstractTermsParametersParser.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
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
        
        if (filterBuilder != null) {
            builder.field(SignificantTermsParametersParser.BACKGROUND_FILTER.getPreferredName());
            filterBuilder.toXContent(builder, params); 
        }
        if (significanceHeuristicBuilder != null) {
            significanceHeuristicBuilder.toXContent(builder);
        }

        return builder.endObject();
    }

    public SignificantTermsBuilder significanceHeuristic(SignificanceHeuristicBuilder significanceHeuristicBuilder) {
        this.significanceHeuristicBuilder = significanceHeuristicBuilder;
        return this;
    }
}
