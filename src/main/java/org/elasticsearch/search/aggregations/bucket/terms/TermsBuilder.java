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

import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Builder for the {@link Terms} aggregation.
 */
public class TermsBuilder extends ValuesSourceAggregationBuilder<TermsBuilder> {

    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(-1, -1, -1, -1);

    private Terms.ValueType valueType;
    private Terms.Order order;
    private String includePattern;
    private String excludePattern;
    private String executionHint;
    private SubAggCollectionMode collectionMode;
    private Boolean showTermDocCountError;
    private String[] includeTerms = null;
    private String[] excludeTerms = null;

    /**
     * Sole constructor.
     */
    public TermsBuilder(String name) {
        super(name, "terms");
    }

    /**
     * Sets the size - indicating how many term buckets should be returned (defaults to 10)
     */
    public TermsBuilder size(int size) {
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard will return to the coordinating node (the
     * node that coordinates the search execution). The higher the shard size is, the more accurate the results are.
     */
    public TermsBuilder shardSize(int shardSize) {
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Set the minimum document count terms should have in order to appear in the response.
     */
    public TermsBuilder minDocCount(long minDocCount) {
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Set the minimum document count terms should have on the shard in order to appear in the response.
     */
    public TermsBuilder shardMinDocCount(long shardMinDocCount) {
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Define a regular expression that will determine what terms should be aggregated. The regular expression is based
     * on the {@link RegExp} class.
     *
     * @see {@link RegExp#RegExp(String)}
     */
    public TermsBuilder include(String regex) {
        if (includeTerms != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of strings or a regex, not both");
        }
        this.includePattern = regex;
        return this;
    }
    
    /**
     * Define a set of terms that should be aggregated.
     */
    public TermsBuilder include(String [] terms) {
        if (includePattern != null) {
            throw new ElasticsearchIllegalArgumentException("include clause must be an array of exact values or a regex, not both");
        }
        this.includeTerms = terms;
        return this;
    }    
    
    /**
     * Define a set of terms that should be aggregated.
     */
    public TermsBuilder include(long [] terms) {
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
     * Define a set of terms that should be aggregated.
     */
    public TermsBuilder include(double [] terms) {
        if (includePattern != null) {
            throw new ElasticsearchIllegalArgumentException("include clause must be an array of exact values or a regex, not both");
        }
        this.includeTerms = doubleArrToStringArr(terms);
        return this;
    }

    private String[] doubleArrToStringArr(double[] terms) {
        String[] termsAsString = new String[terms.length];
        for (int i = 0; i < terms.length; i++) {
            termsAsString[i] = Double.toString(terms[i]);
        }
        return termsAsString;
    }    

    /**
     * Define a regular expression that will filter out terms that should be excluded from the aggregation. The regular
     * expression is based on the {@link RegExp} class.
     *
     * @see {@link RegExp#RegExp(String)}
     */
    public TermsBuilder exclude(String regex) {
        if (excludeTerms != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of exact values or a regex, not both");
        }
        this.excludePattern = regex;
        return this;
    }
    
    /**
     * Define a set of terms that should not be aggregated.
     */
    public TermsBuilder exclude(String [] terms) {
        if (excludePattern != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of exact values or a regex, not both");
        }
        this.excludeTerms = terms;
        return this;
    }    
    
    
    /**
     * Define a set of terms that should not be aggregated.
     */
    public TermsBuilder exclude(long [] terms) {
        if (excludePattern != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of exact values or a regex, not both");
        }
        this.excludeTerms = longsArrToStringArr(terms);
        return this;
    }

    /**
     * Define a set of terms that should not be aggregated.
     */
    public TermsBuilder exclude(double [] terms) {
        if (excludePattern != null) {
            throw new ElasticsearchIllegalArgumentException("exclude clause must be an array of exact values or a regex, not both");
        }
        this.excludeTerms = doubleArrToStringArr(terms);
        return this;
    }    
    
    

    /**
     * When using scripts, the value type indicates the types of the values the script is generating.
     */
    public TermsBuilder valueType(Terms.ValueType valueType) {
        this.valueType = valueType;
        return this;
    }

    /**
     * Defines the order in which the buckets will be returned.
     */
    public TermsBuilder order(Terms.Order order) {
        this.order = order;
        return this;
    }

    /**
     * Expert: provide an execution hint to the aggregation.
     */
    public TermsBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Expert: set the collection mode.
     */
    public TermsBuilder collectMode(SubAggCollectionMode mode) {
        this.collectionMode = mode;
        return this;
    }

    /**
     * Expert: return document count errors per term in the response.
     */
    public TermsBuilder showTermDocCountError(boolean showTermDocCountError) {
        this.showTermDocCountError = showTermDocCountError;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {

        bucketCountThresholds.toXContent(builder);

        if (showTermDocCountError != null) {
            builder.field(AbstractTermsParametersParser.SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
        }
        if (executionHint != null) {
            builder.field(AbstractTermsParametersParser.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        if (valueType != null) {
            builder.field("value_type", valueType.name().toLowerCase(Locale.ROOT));
        }
        if (order != null) {
            builder.field("order");
            order.toXContent(builder, params);
        }
        if (collectionMode != null) {
            builder.field(SubAggCollectionMode.KEY.getPreferredName(), collectionMode.parseField().getPreferredName());
        }
        if (includeTerms != null) {
            builder.array("include", includeTerms);
        }
        if (includePattern != null) {
            builder.field("include", includePattern);
        }
        if (excludeTerms != null) {
            builder.array("exclude", excludeTerms);
        }
        if (excludePattern != null) {
            builder.field("exclude", excludePattern);
        }
        return builder;
    }
}
