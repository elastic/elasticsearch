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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 *
 */
/**
 *
 */
public class TermsAggregatorBuilder extends ValuesSourceAggregatorBuilder<ValuesSource, TermsAggregatorBuilder> {

    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(1, 0, 10,
            -1);
    public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");
    public static final ParseField ORDER_FIELD = new ParseField("order");

    static final TermsAggregatorBuilder PROTOTYPE = new TermsAggregatorBuilder("", null);

    private Terms.Order order = Terms.Order.compound(Terms.Order.count(false), Terms.Order.term(true));
    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private SubAggCollectionMode collectMode = SubAggCollectionMode.DEPTH_FIRST;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
            DEFAULT_BUCKET_COUNT_THRESHOLDS);
    private boolean showTermDocCountError = false;

    public TermsAggregatorBuilder(String name, ValueType valueType) {
        super(name, StringTerms.TYPE, ValuesSourceType.ANY, valueType);
    }

    public TermsAggregator.BucketCountThresholds bucketCountThresholds() {
        return bucketCountThresholds;
    }

    public TermsAggregatorBuilder bucketCountThresholds(TermsAggregator.BucketCountThresholds bucketCountThresholds) {
        if (bucketCountThresholds == null) {
            throw new IllegalArgumentException("[bucketCountThresholds] must not be null: [" + name + "]");
        }
        this.bucketCountThresholds = bucketCountThresholds;
        return this;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public TermsAggregatorBuilder size(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("[size] must be greater than or equal to 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public TermsAggregatorBuilder shardSize(int shardSize) {
        if (shardSize < 0) {
            throw new IllegalArgumentException(
                    "[shardSize] must be greater than or equal to 0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     */
    public TermsAggregatorBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     */
    public TermsAggregatorBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                    "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Sets the order in which the buckets will be returned.
     */
    public TermsAggregatorBuilder order(Terms.Order order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        this.order = order;
        return this;
    }

    /**
     * Sets the order in which the buckets will be returned.
     */
    public TermsAggregatorBuilder order(List<Terms.Order> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        order(Terms.Order.compound(orders));
        return this;
    }

    /**
     * Gets the order in which the buckets will be returned.
     */
    public Terms.Order order() {
        return order;
    }

    /**
     * Expert: sets an execution hint to the aggregation.
     */
    public TermsAggregatorBuilder executionHint(String executionHint) {
        this.executionHint = executionHint;
        return this;
    }

    /**
     * Expert: gets an execution hint to the aggregation.
     */
    public String executionHint() {
        return executionHint;
    }

    /**
     * Expert: set the collection mode.
     */
    public TermsAggregatorBuilder collectMode(SubAggCollectionMode collectMode) {
        if (collectMode == null) {
            throw new IllegalArgumentException("[collectMode] must not be null: [" + name + "]");
        }
        this.collectMode = collectMode;
        return this;
    }

    /**
     * Expert: get the collection mode.
     */
    public SubAggCollectionMode collectMode() {
        return collectMode;
    }

    /**
     * Set terms to include and exclude from the aggregation results
     */
    public TermsAggregatorBuilder includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
        return this;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     */
    public IncludeExclude includeExclude() {
        return includeExclude;
    }

    /**
     * Get whether doc count error will be return for individual terms
     */
    public boolean showTermDocCountError() {
        return showTermDocCountError;
    }

    /**
     * Set whether doc count error will be return for individual terms
     */
    public TermsAggregatorBuilder showTermDocCountError(boolean showTermDocCountError) {
        this.showTermDocCountError = showTermDocCountError;
        return this;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource, ?> innerBuild(AggregationContext context, ValuesSourceConfig<ValuesSource> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new TermsAggregatorFactory(name, type, config, order, includeExclude, executionHint, collectMode,
 bucketCountThresholds,
                showTermDocCountError, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        bucketCountThresholds.toXContent(builder, params);
        builder.field(SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
        if (executionHint != null) {
            builder.field(TermsAggregatorBuilder.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        builder.field(ORDER_FIELD.getPreferredName());
        order.toXContent(builder, params);
        builder.field(SubAggCollectionMode.KEY.getPreferredName(), collectMode.parseField().getPreferredName());
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    protected TermsAggregatorBuilder innerReadFrom(String name, ValuesSourceType valuesSourceType,
            ValueType targetValueType, StreamInput in) throws IOException {
        TermsAggregatorBuilder factory = new TermsAggregatorBuilder(name, targetValueType);
        factory.bucketCountThresholds = BucketCountThresholds.readFromStream(in);
        factory.collectMode = SubAggCollectionMode.BREADTH_FIRST.readFrom(in);
        factory.executionHint = in.readOptionalString();
        if (in.readBoolean()) {
            factory.includeExclude = IncludeExclude.readFromStream(in);
        }
        factory.order = InternalOrder.Streams.readOrder(in);
        factory.showTermDocCountError = in.readBoolean();
        return factory;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        collectMode.writeTo(out);
        out.writeOptionalString(executionHint);
        boolean hasIncExc = includeExclude != null;
        out.writeBoolean(hasIncExc);
        if (hasIncExc) {
            includeExclude.writeTo(out);
        }
        InternalOrder.Streams.writeOrder(order, out);
        out.writeBoolean(showTermDocCountError);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(bucketCountThresholds, collectMode, executionHint, includeExclude, order, showTermDocCountError);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        TermsAggregatorBuilder other = (TermsAggregatorBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
                && Objects.equals(collectMode, other.collectMode)
                && Objects.equals(executionHint, other.executionHint)
                && Objects.equals(includeExclude, other.includeExclude)
                && Objects.equals(order, other.order)
                && Objects.equals(showTermDocCountError, other.showTermDocCountError);
    }

}
