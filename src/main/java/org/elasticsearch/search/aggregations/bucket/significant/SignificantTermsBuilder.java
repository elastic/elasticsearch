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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
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
    private String[] includeTerms = null;
    private String[] excludeTerms = null;
    private FilterBuilder filterBuilder;
    private SignificanceHeuristicBuilder significanceHeuristicBuilder;

    /**
     * Sole constructor.
     */
    public SignificantTermsBuilder(String name) {
        super(name, SignificantStringTerms.TYPE.name());
    }

    /**
     * Set the field to fetch significant terms from.
     */
    public SignificantTermsBuilder field(String field) {
        this.field = field;
        return this;
    }

    /**
     * Set the number of significant terms to retrieve.
     */
    public SignificantTermsBuilder size(int requiredSize) {
        bucketCountThresholds.setRequiredSize(requiredSize);
        return this;
    }

    /**
     * Expert: Set the number of significant terms to retrieve on each shard.
     */
    public SignificantTermsBuilder shardSize(int shardSize) {
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Only return significant terms that belong to at least <code>minDocCount</code> documents.
     */
    public SignificantTermsBuilder minDocCount(int minDocCount) {
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }
    
    /**
     * Set the background filter to compare to. Defaults to the whole index.
     */
    public SignificantTermsBuilder backgroundFilter(FilterBuilder filter) {
        this.filterBuilder = filter;
        return this;
    }
    
    /**
     * Expert: set the minimum number of documents that a term should match to
     * be retrieved from a shard.
     */
    public SignificantTermsBuilder shardMinDocCount(int shardMinDocCount) {
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Expert: give an execution hint to this aggregation.
     */
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
        if (includeTerms != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of strings or a regex, not both");
        }
        this.includePattern = regex;
        this.includeFlags = flags;
        return this;
    }
    
    /**
     * Define a set of terms that should be aggregated.
     */
    public SignificantTermsBuilder include(String [] terms) {
        if (includePattern != null) {
            throw new ElasticsearchIllegalArgumentException("include clause must be an array of exact values or a regex, not both");
        }
        this.includeTerms = terms;
        return this;
    }    
    
    /**
     * Define a set of terms that should be aggregated.
     */
    public SignificantTermsBuilder include(long [] terms) {
        if (includePattern != null) {
            throw new ElasticsearchIllegalArgumentException("include clause must be an array of exact values or a regex, not both");
        }
        this.includeTerms = longsArrToStringArr(terms);
        return this;
    }     
    
    private String[] longsArrToStringArr(long[] terms) {
        String[] termsAsString = new String[terms.length];
        for (int i = 0; i < terms.length; i++) {
            termsAsString[i] = Long.toString(terms[i]);
        }
        return termsAsString;
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
        if (excludeTerms != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of strings or a regex, not both");
        }
        this.excludePattern = regex;
        this.excludeFlags = flags;
        return this;
    }
    
    /**
     * Define a set of terms that should not be aggregated.
     */
    public SignificantTermsBuilder exclude(String [] terms) {
        if (excludePattern != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of strings or a regex, not both");
        }
        this.excludeTerms = terms;
        return this;
    }    
    
    
    /**
     * Define a set of terms that should not be aggregated.
     */
    public SignificantTermsBuilder exclude(long [] terms) {
        if (excludePattern != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of longs or a regex, not both");
        }
        this.excludeTerms = longsArrToStringArr(terms);
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
        if (includeTerms != null) {
            builder.array("include", includeTerms);
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
        if (excludeTerms != null) {
            builder.array("exclude", excludeTerms);
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

    /**
     * Expert: set the {@link SignificanceHeuristic} to use.
     */
    public SignificantTermsBuilder significanceHeuristic(SignificanceHeuristicBuilder significanceHeuristicBuilder) {
        this.significanceHeuristicBuilder = significanceHeuristicBuilder;
        return this;
    }
}
