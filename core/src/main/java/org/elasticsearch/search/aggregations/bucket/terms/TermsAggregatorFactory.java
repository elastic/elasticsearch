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

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
/**
 *
 */
public class TermsAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(1, 0, 10,
            -1);
    public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");
    public static final ParseField ORDER_FIELD = new ParseField("order");

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                    boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter();
                return new StringTermsAggregator(name, factories, valuesSource, order, bucketCountThresholds, filter, aggregationContext,
                        parent, subAggCollectMode, showTermDocCountError, pipelineAggregators, metaData);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                    boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
                final IncludeExclude.OrdinalsFilter filter = includeExclude == null ? null : includeExclude.convertToOrdinalsFilter();
                return new GlobalOrdinalsStringTermsAggregator(name, factories, (ValuesSource.Bytes.WithOrdinals) valuesSource, order,
                        bucketCountThresholds, filter, aggregationContext, parent, subAggCollectMode, showTermDocCountError,
                        pipelineAggregators, metaData);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }

        },
        GLOBAL_ORDINALS_HASH(new ParseField("global_ordinals_hash")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                    boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
                final IncludeExclude.OrdinalsFilter filter = includeExclude == null ? null : includeExclude.convertToOrdinalsFilter();
                return new GlobalOrdinalsStringTermsAggregator.WithHash(name, factories,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, order, bucketCountThresholds, filter, aggregationContext,
                        parent, subAggCollectMode, showTermDocCountError, pipelineAggregators, metaData);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }
        },
        GLOBAL_ORDINALS_LOW_CARDINALITY(new ParseField("global_ordinals_low_cardinality")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                    boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
                if (includeExclude != null || factories.count() > 0) {
                    return GLOBAL_ORDINALS.create(name, factories, valuesSource, order, bucketCountThresholds, includeExclude,
                            aggregationContext, parent, subAggCollectMode, showTermDocCountError, pipelineAggregators, metaData);
                }
                return new GlobalOrdinalsStringTermsAggregator.LowCardinality(name, factories,
                        (ValuesSource.Bytes.WithOrdinals) valuesSource, order, bucketCountThresholds, aggregationContext, parent,
                        subAggCollectMode, showTermDocCountError, pipelineAggregators, metaData);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }
        };

        public static ExecutionMode fromString(String value, ParseFieldMatcher parseFieldMatcher) {
            for (ExecutionMode mode : values()) {
                if (parseFieldMatcher.match(value, mode.parseField)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of " + values());
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

    private List<Terms.Order> orders = Collections.singletonList(Terms.Order.count(false));
    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private SubAggCollectionMode collectMode = SubAggCollectionMode.DEPTH_FIRST;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
            DEFAULT_BUCKET_COUNT_THRESHOLDS);
    private boolean showTermDocCountError = false;

    public TermsAggregatorFactory(String name, ValuesSourceType valuesSourceType, ValueType valueType) {
        super(name, StringTerms.TYPE, valuesSourceType, valueType);
    }

    public TermsAggregator.BucketCountThresholds bucketCountThresholds() {
        return bucketCountThresholds;
    }

    public void bucketCountThresholds(TermsAggregator.BucketCountThresholds bucketCountThresholds) {
        this.bucketCountThresholds = bucketCountThresholds;
    }

    /**
     * Sets the order in which the buckets will be returned.
     */
    public void order(List<Terms.Order> order) {
        this.orders = order;
    }

    /**
     * Gets the order in which the buckets will be returned.
     */
    public List<Terms.Order> order() {
        return orders;
    }

    /**
     * Expert: sets an execution hint to the aggregation.
     */
    public void executionHint(String executionHint) {
        this.executionHint = executionHint;
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
    public void collectMode(SubAggCollectionMode mode) {
        this.collectMode = mode;
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
    public void includeExclude(IncludeExclude includeExclude) {
        this.includeExclude = includeExclude;
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
    public void showTermDocCountError(boolean showTermDocCountError) {
        this.showTermDocCountError = showTermDocCountError;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        Terms.Order order = resolveOrder(orders);
        final InternalAggregation aggregation = new UnmappedTerms(name, order, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), bucketCountThresholds.getMinDocCount(), pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, aggregationContext, parent, factories, pipelineAggregators, metaData) {
            {
                // even in the case of an unmapped aggregator, validate the order
                InternalOrder.validate(order, this);
            }

            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    private Order resolveOrder(List<Order> orders) {
        Terms.Order order;
        if (orders.size() == 1 && (orders.get(0) == InternalOrder.TERM_ASC || orders.get(0) == InternalOrder.TERM_DESC)) {
            // If order is only terms order then we don't need compound
            // ordering
            order = orders.get(0);
        } else {
            // for all other cases we need compound order so term order asc
            // can be added to make the order deterministic
            order = Order.compound(orders);
        }
        return order;
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        Terms.Order order = resolveOrder(orders);
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, aggregationContext, parent);
        }
        BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
        if (!(order == InternalOrder.TERM_ASC || order == InternalOrder.TERM_DESC)
                && bucketCountThresholds.getShardSize() == DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize(),
                    aggregationContext.searchContext().numberOfShards()));
        }
        bucketCountThresholds.ensureValidity();
        if (valuesSource instanceof ValuesSource.Bytes) {
            ExecutionMode execution = null;
            if (executionHint != null) {
                execution = ExecutionMode.fromString(executionHint, aggregationContext.searchContext().parseFieldMatcher());
            }

            // In some cases, using ordinals is just not supported: override it
            if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
                execution = ExecutionMode.MAP;
            }

            final long maxOrd;
            final double ratio;
            if (execution == null || execution.needsGlobalOrdinals()) {
                ValuesSource.Bytes.WithOrdinals valueSourceWithOrdinals = (ValuesSource.Bytes.WithOrdinals) valuesSource;
                IndexSearcher indexSearcher = aggregationContext.searchContext().searcher();
                maxOrd = valueSourceWithOrdinals.globalMaxOrd(indexSearcher);
                ratio = maxOrd / ((double) indexSearcher.getIndexReader().numDocs());
            } else {
                maxOrd = -1;
                ratio = -1;
            }

            // Let's try to use a good default
            if (execution == null) {
                // if there is a parent bucket aggregator the number of instances of this aggregator is going
                // to be unbounded and most instances may only aggregate few documents, so use hashed based
                // global ordinals to keep the bucket ords dense.
                if (Aggregator.descendsFromBucketAggregator(parent)) {
                    execution = ExecutionMode.GLOBAL_ORDINALS_HASH;
                } else {
                    if (factories == AggregatorFactories.EMPTY) {
                        if (ratio <= 0.5 && maxOrd <= 2048) {
                            // 0.5: At least we need reduce the number of global ordinals look-ups by half
                            // 2048: GLOBAL_ORDINALS_LOW_CARDINALITY has additional memory usage, which directly linked to maxOrd, so we need to limit.
                            execution = ExecutionMode.GLOBAL_ORDINALS_LOW_CARDINALITY;
                        } else {
                            execution = ExecutionMode.GLOBAL_ORDINALS;
                        }
                    } else {
                        execution = ExecutionMode.GLOBAL_ORDINALS;
                    }
                }
            }

            return execution.create(name, factories, valuesSource, order, bucketCountThresholds, includeExclude, aggregationContext,
                    parent, collectMode, showTermDocCountError, pipelineAggregators, metaData);
        }

        if ((includeExclude != null) && (includeExclude.isRegexBased())) {
            throw new AggregationExecutionException(
                    "Aggregation ["
                            + name
                            + "] cannot support regular expression style include/exclude "
                            + "settings as they can only be applied to string fields. Use an array of numeric values for include/exclude clauses used to filter numeric fields");
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            IncludeExclude.LongFilter longFilter = null;
            if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToDoubleFilter();
                }
                return new DoubleTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(), order,
                        bucketCountThresholds, aggregationContext, parent, collectMode, showTermDocCountError, longFilter,
                        pipelineAggregators, metaData);
            }
            if (includeExclude != null) {
                longFilter = includeExclude.convertToLongFilter();
            }
            return new LongTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(), order,
                    bucketCountThresholds, aggregationContext, parent, collectMode, showTermDocCountError, longFilter, pipelineAggregators,
                    metaData);
        }

        throw new AggregationExecutionException("terms aggregation cannot be applied to field [" + config.fieldContext().field()
                + "]. It can only be applied to numeric or string fields.");
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        bucketCountThresholds.toXContent(builder, params);
        builder.field(SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
        if (executionHint != null) {
            builder.field(TermsAggregatorFactory.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        builder.startArray(ORDER_FIELD.getPreferredName());
        for (Terms.Order order : orders) {
            order.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(SubAggCollectionMode.KEY.getPreferredName(), collectMode.parseField().getPreferredName());
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource> innerReadFrom(String name, ValuesSourceType valuesSourceType,
            ValueType targetValueType, StreamInput in) throws IOException {
        TermsAggregatorFactory factory = new TermsAggregatorFactory(name, valuesSourceType, targetValueType);
        factory.bucketCountThresholds = BucketCountThresholds.readFromStream(in);
        factory.collectMode = SubAggCollectionMode.BREADTH_FIRST.readFrom(in);
        factory.executionHint = in.readOptionalString();
        if (in.readBoolean()) {
            factory.includeExclude = IncludeExclude.readFromStream(in);
        }
        int numOrders = in.readVInt();
        List<Terms.Order> orders = new ArrayList<>(numOrders);
        for (int i = 0; i < numOrders; i++) {
            orders.add(InternalOrder.Streams.readOrder(in));
        }
        factory.orders = orders;
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
        out.writeVInt(orders.size());
        for (Terms.Order order : orders) {
            InternalOrder.Streams.writeOrder(order, out);
        }
        out.writeBoolean(showTermDocCountError);
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(bucketCountThresholds, collectMode, executionHint, includeExclude, orders, showTermDocCountError);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        TermsAggregatorFactory other = (TermsAggregatorFactory) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
                && Objects.equals(collectMode, other.collectMode)
                && Objects.equals(executionHint, other.executionHint)
                && Objects.equals(includeExclude, other.includeExclude)
                && Objects.equals(orders, other.orders)
                && Objects.equals(showTermDocCountError, other.showTermDocCountError);
    }

}
