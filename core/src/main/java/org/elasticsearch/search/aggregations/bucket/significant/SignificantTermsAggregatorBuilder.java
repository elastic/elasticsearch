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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicStreams;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public class SignificantTermsAggregatorBuilder extends ValuesSourceAggregatorBuilder<ValuesSource, SignificantTermsAggregatorBuilder> {

    static final ParseField BACKGROUND_FILTER = new ParseField("background_filter");
    static final ParseField HEURISTIC = new ParseField("significance_heuristic");

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(
            3, 0, 10, -1);

    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private String indexedFieldName;
    private MappedFieldType fieldType;
    private FilterableTermsEnum termsEnum;
    private int numberOfAggregatorsCreated = 0;
    private QueryBuilder<?> filterBuilder = null;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    private SignificanceHeuristic significanceHeuristic = JLHScore.PROTOTYPE;

    protected TermsAggregator.BucketCountThresholds getBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(bucketCountThresholds);
    }

    public SignificantTermsAggregatorBuilder(String name, ValueType valueType) {
        super(name, SignificantStringTerms.TYPE, ValuesSourceType.ANY, valueType);
    }

    public TermsAggregator.BucketCountThresholds bucketCountThresholds() {
        return bucketCountThresholds;
    }

    public SignificantTermsAggregatorBuilder bucketCountThresholds(TermsAggregator.BucketCountThresholds bucketCountThresholds) {
        this.bucketCountThresholds = bucketCountThresholds;
        return this;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public SignificantTermsAggregatorBuilder size(int size) {
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public SignificantTermsAggregatorBuilder shardSize(int shardSize) {
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     */
    public SignificantTermsAggregatorBuilder minDocCount(long minDocCount) {
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     */
    public SignificantTermsAggregatorBuilder shardMinDocCount(long shardMinDocCount) {
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Expert: sets an execution hint to the aggregation.
     */
    public SignificantTermsAggregatorBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Expert: gets an execution hint to the aggregation.
     */
    public String executionHint() {
        return executionHint;
    }

    public SignificantTermsAggregatorBuilder backgroundFilter(QueryBuilder<?> filterBuilder) {
        this.filterBuilder = filterBuilder;
        return this;
    }

    public QueryBuilder<?> backgroundFilter() {
        return filterBuilder;
    }

    /**
     * Set terms to include and exclude from the aggregation results
     */
    public SignificantTermsAggregatorBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     */
    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    public SignificantTermsAggregatorBuilder significanceHeuristic(SignificanceHeuristic significanceHeuristic) {
        this.significanceHeuristic = significanceHeuristic;
        return this;
    }

    public SignificanceHeuristic significanceHeuristic() {
        return significanceHeuristic;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource, ?> innerBuild(AggregationContext context, ValuesSourceConfig<ValuesSource> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new SignificantTermsAggregatorFactory(name, type, config, includeExclude, executionHint, filterBuilder,
                bucketCountThresholds, significanceHeuristic, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        bucketCountThresholds.toXContent(builder, params);
        if (executionHint != null) {
            builder.field(TermsAggregatorBuilder.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        if (filterBuilder != null) {
            builder.field(BACKGROUND_FILTER.getPreferredName(), filterBuilder);
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        significanceHeuristic.toXContent(builder, params);
        return builder;
    }

    @Override
    protected SignificantTermsAggregatorBuilder innerReadFrom(String name, ValuesSourceType valuesSourceType,
            ValueType targetValueType, StreamInput in) throws IOException {
        SignificantTermsAggregatorBuilder factory = new SignificantTermsAggregatorBuilder(name, targetValueType);
        factory.bucketCountThresholds = BucketCountThresholds.readFromStream(in);
        factory.executionHint = in.readOptionalString();
        if (in.readBoolean()) {
            factory.filterBuilder = in.readQuery();
        }
        if (in.readBoolean()) {
            factory.includeExclude = IncludeExclude.readFromStream(in);
        }
        factory.significanceHeuristic = SignificanceHeuristicStreams.read(in);
        return factory;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeOptionalString(executionHint);
        boolean hasfilterBuilder = filterBuilder != null;
        out.writeBoolean(hasfilterBuilder);
        if (hasfilterBuilder) {
            out.writeQuery(filterBuilder);
        }
        boolean hasIncExc = includeExclude != null;
        out.writeBoolean(hasIncExc);
        if (hasIncExc) {
            includeExclude.writeTo(out);
        }
        SignificanceHeuristicStreams.writeTo(significanceHeuristic, out);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(bucketCountThresholds, executionHint, filterBuilder, includeExclude, significanceHeuristic);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        SignificantTermsAggregatorBuilder other = (SignificantTermsAggregatorBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
                && Objects.equals(executionHint, other.executionHint)
                && Objects.equals(filterBuilder, other.filterBuilder)
                && Objects.equals(includeExclude, other.includeExclude)
                && Objects.equals(significanceHeuristic, other.significanceHeuristic);
    }
}
