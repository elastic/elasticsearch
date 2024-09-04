/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToLongFunction;

public class TermsAggregationBuilder extends ValuesSourceAggregationBuilder<TermsAggregationBuilder> {
    public static final int KEY_ORDER_CONCURRENCY_THRESHOLD = 50;

    public static final String NAME = "terms";
    public static final ValuesSourceRegistry.RegistryKey<TermsAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        TermsAggregatorSupplier.class
    );

    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");

    static final TermsAggregator.ConstantBucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS =
        new TermsAggregator.ConstantBucketCountThresholds(1, 0, 10, -1);
    public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");
    public static final ParseField ORDER_FIELD = new ParseField("order");

    public static final ObjectParser<TermsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(NAME, TermsAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);

        PARSER.declareBoolean(TermsAggregationBuilder::showTermDocCountError, TermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR);

        PARSER.declareInt(TermsAggregationBuilder::shardSize, SHARD_SIZE_FIELD_NAME);

        PARSER.declareLong(TermsAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareLong(TermsAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareInt(TermsAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);

        PARSER.declareString(TermsAggregationBuilder::executionHint, EXECUTION_HINT_FIELD_NAME);

        PARSER.declareField(
            TermsAggregationBuilder::collectMode,
            (p, c) -> SubAggCollectionMode.parse(p.text(), LoggingDeprecationHandler.INSTANCE),
            SubAggCollectionMode.KEY,
            ObjectParser.ValueType.STRING
        );

        PARSER.declareObjectArray(
            TermsAggregationBuilder::order,
            (p, c) -> InternalOrder.Parser.parseOrderParam(p),
            TermsAggregationBuilder.ORDER_FIELD
        );

        PARSER.declareField(
            (b, v) -> b.includeExclude(IncludeExclude.merge(v, b.includeExclude())),
            IncludeExclude::parseInclude,
            IncludeExclude.INCLUDE_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING
        );

        PARSER.declareField(
            (b, v) -> b.includeExclude(IncludeExclude.merge(b.includeExclude(), v)),
            IncludeExclude::parseExclude,
            IncludeExclude.EXCLUDE_FIELD,
            ObjectParser.ValueType.STRING_ARRAY
        );
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        TermsAggregatorFactory.registerAggregators(builder);
    }

    private BucketOrder order = BucketOrder.compound(BucketOrder.count(false)); // automatically adds tie-breaker key asc order
    private IncludeExclude includeExclude = null;
    private String executionHint = null;
    private SubAggCollectionMode collectMode = null;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;

    private boolean showTermDocCountError = false;
    private boolean excludeDeletedDocs = false;

    public TermsAggregationBuilder(String name) {
        super(name);
        this.bucketCountThresholds = new TermsAggregator.BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    }

    protected TermsAggregationBuilder(
        TermsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.order = clone.order;
        this.executionHint = clone.executionHint;
        this.includeExclude = clone.includeExclude;
        this.collectMode = clone.collectMode;
        this.bucketCountThresholds = new BucketCountThresholds(clone.bucketCountThresholds);
        this.showTermDocCountError = clone.showTermDocCountError;
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    public boolean supportsParallelCollection(ToLongFunction<String> fieldCardinalityResolver) {
        if (minDocCount() == 0) {
            // if minDocCount os zero, we collect the zero buckets looking into all segments in the index. to avoid
            // looking into the same segment for each thread we disable concurrency
            return false;
        }
        /*
         * we parallelize only if the cardinality of the field is lower than shard size, this is to minimize precision issues.
         * When ordered by term, we still take cardinality into account to avoid overhead that concurrency may cause against
         * high cardinality fields.
         */
        if (script() == null
            && (executionHint == null || executionHint.equals(TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS.toString()))) {
            long cardinality = fieldCardinalityResolver.applyAsLong(field());
            if (supportsParallelCollection(cardinality, order, bucketCountThresholds)) {
                return super.supportsParallelCollection(fieldCardinalityResolver);
            }
        }
        return false;
    }

    /**
     * Whether a terms aggregation with the provided order and bucket count thresholds against a field
     * with the given cardinality should be executed concurrency.
     */
    public static boolean supportsParallelCollection(long cardinality, BucketOrder order, BucketCountThresholds bucketCountThresholds) {
        if (cardinality != -1) {
            if (InternalOrder.isKeyOrder(order)) {
                return cardinality <= KEY_ORDER_CONCURRENCY_THRESHOLD;
            }
            BucketCountThresholds adjusted = TermsAggregatorFactory.adjustBucketCountThresholds(bucketCountThresholds, order);
            // for cardinality equal to shard size, we don't know if there were more terms when merging.
            return cardinality < adjusted.getShardSize();
        }
        return false;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.KEYWORD;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TermsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public TermsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        bucketCountThresholds = new BucketCountThresholds(in);
        collectMode = in.readOptionalWriteable(SubAggCollectionMode::readFromStream);
        executionHint = in.readOptionalString();
        includeExclude = in.readOptionalWriteable(IncludeExclude::new);
        order = InternalOrder.Streams.readOrder(in);
        showTermDocCountError = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            excludeDeletedDocs = in.readBoolean();
        }
    }

    @Override
    protected boolean serializeTargetValueType(TransportVersion version) {
        return true;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeOptionalWriteable(collectMode);
        out.writeOptionalString(executionHint);
        out.writeOptionalWriteable(includeExclude);
        order.writeTo(out);
        out.writeBoolean(showTermDocCountError);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeBoolean(excludeDeletedDocs);
        }
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public TermsAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /**
     * Returns the number of term buckets currently configured
     */
    public int size() {
        return bucketCountThresholds.getRequiredSize();
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public TermsAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException("[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Returns the number of term buckets per shard that are currently configured
     */
    public int shardSize() {
        return bucketCountThresholds.getShardSize();
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     */
    public TermsAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Returns the minimum document count required per term
     */
    public long minDocCount() {
        return bucketCountThresholds.getMinDocCount();
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     */
    public TermsAggregationBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Returns the minimum document count required per term, per shard
     */
    public long shardMinDocCount() {
        return bucketCountThresholds.getShardMinDocCount();
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
    public TermsAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        if (order instanceof CompoundOrder || InternalOrder.isKeyOrder(order)) {
            this.order = order; // if order already contains a tie-breaker we are good to go
        } else { // otherwise add a tie-breaker by using a compound order
            this.order = BucketOrder.compound(order);
        }
        return this;
    }

    /**
     * Sets the order in which the buckets will be returned. A tie-breaker may be added to avoid non-deterministic
     * ordering.
     */
    public TermsAggregationBuilder order(List<BucketOrder> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        // if the list only contains one order use that to avoid inconsistent xcontent
        order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
        return this;
    }

    /**
     * Gets the order in which the buckets will be returned.
     */
    public BucketOrder order() {
        return order;
    }

    /**
     * Expert: sets an execution hint to the aggregation.
     */
    public TermsAggregationBuilder executionHint(String executionHint) {
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
    public TermsAggregationBuilder collectMode(SubAggCollectionMode collectMode) {
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
    public TermsAggregationBuilder includeExclude(IncludeExclude includeExclude) {
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
    public TermsAggregationBuilder showTermDocCountError(boolean showTermDocCountError) {
        this.showTermDocCountError = showTermDocCountError;
        return this;
    }

    /**
     * Set whether deleted documents should be explicitly excluded from the aggregation results
     */
    public TermsAggregationBuilder excludeDeletedDocs(boolean excludeDeletedDocs) {
        this.excludeDeletedDocs = excludeDeletedDocs;
        return this;
    }

    public boolean excludeDeletedDocs() {
        return excludeDeletedDocs;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        TermsAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new TermsAggregatorFactory(
            name,
            config,
            order,
            includeExclude,
            executionHint,
            collectMode,
            bucketCountThresholds,
            showTermDocCountError,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier,
            excludeDeletedDocs
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        bucketCountThresholds.toXContent(builder, params);
        builder.field(SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
        if (executionHint != null) {
            builder.field(TermsAggregationBuilder.EXECUTION_HINT_FIELD_NAME.getPreferredName(), executionHint);
        }
        builder.field(ORDER_FIELD.getPreferredName());
        order.toXContent(builder, params);
        if (collectMode != null) {
            builder.field(SubAggCollectionMode.KEY.getPreferredName(), collectMode.parseField().getPreferredName());
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            bucketCountThresholds,
            collectMode,
            executionHint,
            includeExclude,
            order,
            showTermDocCountError,
            excludeDeletedDocs
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        TermsAggregationBuilder other = (TermsAggregationBuilder) obj;
        return Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
            && Objects.equals(collectMode, other.collectMode)
            && Objects.equals(executionHint, other.executionHint)
            && Objects.equals(includeExclude, other.includeExclude)
            && Objects.equals(order, other.order)
            && Objects.equals(showTermDocCountError, other.showTermDocCountError)
            && Objects.equals(excludeDeletedDocs, other.excludeDeletedDocs);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
