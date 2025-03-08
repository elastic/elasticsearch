/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public class MultiTermsAggregationBuilder extends AbstractAggregationBuilder<MultiTermsAggregationBuilder> {
    public static final String NAME = "multi_terms";
    public static final ParseField TERMS_FIELD = new ParseField("terms");
    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField SHARD_SIZE_FIELD_NAME = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD_NAME = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD_NAME = new ParseField("shard_min_doc_count");
    public static final ParseField REQUIRED_SIZE_FIELD_NAME = new ParseField("size");
    public static final ParseField SHOW_TERM_DOC_COUNT_ERROR = new ParseField("show_term_doc_count_error");

    static final TermsAggregator.ConstantBucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS =
        new TermsAggregator.ConstantBucketCountThresholds(1, 0, 10, -1);

    public static final ObjectParser<MultiTermsAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        MultiTermsAggregationBuilder::new
    );

    static {
        ContextParser<Void, MultiValuesSourceFieldConfig.Builder> termsParser = MultiValuesSourceFieldConfig.parserBuilder(
            true,
            true,
            false,
            true,
            false
        );

        PARSER.declareBoolean(MultiTermsAggregationBuilder::showTermDocCountError, MultiTermsAggregationBuilder.SHOW_TERM_DOC_COUNT_ERROR);

        PARSER.declareObjectArray(MultiTermsAggregationBuilder::terms, (p, n) -> termsParser.parse(p, null).build(), TERMS_FIELD);

        PARSER.declareInt(MultiTermsAggregationBuilder::shardSize, SHARD_SIZE_FIELD_NAME);

        PARSER.declareLong(MultiTermsAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareLong(MultiTermsAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD_NAME);

        PARSER.declareInt(MultiTermsAggregationBuilder::size, REQUIRED_SIZE_FIELD_NAME);

        PARSER.declareObjectArray(MultiTermsAggregationBuilder::order, (p, c) -> InternalOrder.Parser.parseOrderParam(p), ORDER_FIELD);

        PARSER.declareField(
            MultiTermsAggregationBuilder::collectMode,
            (p, c) -> Aggregator.SubAggCollectionMode.parse(p.text(), LoggingDeprecationHandler.INSTANCE),
            Aggregator.SubAggCollectionMode.KEY,
            ObjectParser.ValueType.STRING
        );
    }

    private List<MultiValuesSourceFieldConfig> terms = Collections.emptyList();

    private BucketOrder order = BucketOrder.compound(BucketOrder.count(false));

    boolean showTermDocCountError = false;

    private Aggregator.SubAggCollectionMode collectMode = null;

    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
        DEFAULT_BUCKET_COUNT_THRESHOLDS
    );

    @FunctionalInterface
    interface MultiTermValuesSupplier {
        MultiTermsAggregator.TermValuesSource build(ValuesSourceConfig config);
    }

    static final ValuesSourceRegistry.RegistryKey<MultiTermValuesSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        NAME,
        MultiTermValuesSupplier.class
    );

    public static void registerAggregators(ValuesSourceRegistry.Builder registry) {
        registry.registerUsage(NAME);
        registry.register(REGISTRY_KEY, List.of(CoreValuesSourceType.NUMERIC), MultiTermsAggregator::buildNumericTermValues, false);
        registry.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.DATE),
            MultiTermsAggregator.LongTermValuesSource::new,
            false
        );
        registry.register(REGISTRY_KEY, List.of(CoreValuesSourceType.KEYWORD), MultiTermsAggregator.StringTermValuesSource::new, false);
        registry.register(REGISTRY_KEY, List.of(CoreValuesSourceType.IP), MultiTermsAggregator.IPTermValuesSource::new, false);
    }

    public MultiTermsAggregationBuilder(String name) {
        super(name);
    }

    public MultiTermsAggregationBuilder(
        MultiTermsAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.terms = new ArrayList<>(clone.terms);
        this.order = clone.order;
        this.collectMode = clone.collectMode;
        this.showTermDocCountError = clone.showTermDocCountError;
        this.bucketCountThresholds = new TermsAggregator.BucketCountThresholds(clone.bucketCountThresholds);
    }

    public MultiTermsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        terms = in.readCollectionAsList(MultiValuesSourceFieldConfig::new);
        order = InternalOrder.Streams.readOrder(in);
        collectMode = in.readOptionalWriteable(Aggregator.SubAggCollectionMode::readFromStream);
        bucketCountThresholds = new TermsAggregator.BucketCountThresholds(in);
        showTermDocCountError = in.readBoolean();
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    public boolean supportsParallelCollection(ToLongFunction<String> fieldCardinalityResolver) {
        for (MultiValuesSourceFieldConfig sourceFieldConfig : terms) {
            if (sourceFieldConfig.getScript() != null) {
                return false;
            }
            long cardinality = fieldCardinalityResolver.applyAsLong(sourceFieldConfig.getFieldName());
            if (TermsAggregationBuilder.supportsParallelCollection(cardinality, order, bucketCountThresholds) == false) {
                return false;
            }
        }
        return super.supportsParallelCollection(fieldCardinalityResolver);
    }

    /**
     * Sets the field to use for this aggregation.
     */
    public MultiTermsAggregationBuilder terms(List<MultiValuesSourceFieldConfig> terms) {
        if (terms == null) {
            throw new IllegalArgumentException("[terms] must not be null: [" + name + "]");
        }
        if (terms.size() < 2) {
            throw new IllegalArgumentException(
                "The [terms] parameter in the aggregation ["
                    + name
                    + "] must be present and have at least "
                    + "2 fields or scripts."
                    + (terms.size() == 1 ? " For a single field user terms aggregation." : "")
            );
        }
        this.terms = terms;
        return this;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new MultiTermsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeCollection(terms);
        order.writeTo(out);
        out.writeOptionalWriteable(collectMode);
        bucketCountThresholds.writeTo(out);
        out.writeBoolean(showTermDocCountError);
    }

    /**
     * Set whether doc count error will be return for individual terms
     */
    public MultiTermsAggregationBuilder showTermDocCountError(boolean showTermDocCountError) {
        this.showTermDocCountError = showTermDocCountError;
        return this;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public MultiTermsAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
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
    public MultiTermsAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException("[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Set the minimum document count terms should have in order to appear in
     * the response.
     */
    public MultiTermsAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 1) {
            throw new IllegalArgumentException(
                "[minDocCount] must be greater than or equal to 1. Found [" + minDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setMinDocCount(minDocCount);
        return this;
    }

    /**
     * Set the minimum document count terms should have on the shard in order to
     * appear in the response.
     */
    public MultiTermsAggregationBuilder shardMinDocCount(long shardMinDocCount) {
        if (shardMinDocCount < 0) {
            throw new IllegalArgumentException(
                "[shardMinDocCount] must be greater than or equal to 0. Found [" + shardMinDocCount + "] in [" + name + "]"
            );
        }
        bucketCountThresholds.setShardMinDocCount(shardMinDocCount);
        return this;
    }

    /**
     * Set a new order on this builder and return the builder so that calls
     * can be chained. A tie-breaker may be added to avoid non-deterministic ordering.
     */
    public MultiTermsAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        if (order instanceof InternalOrder.CompoundOrder || InternalOrder.isKeyOrder(order)) {
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
    public MultiTermsAggregationBuilder order(List<BucketOrder> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        // if the list only contains one order use that to avoid inconsistent xcontent
        order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
        return this;
    }

    /**
     * Expert: set the collection mode.
     */
    public MultiTermsAggregationBuilder collectMode(Aggregator.SubAggCollectionMode collectMode) {
        if (collectMode == null) {
            throw new IllegalArgumentException("[collectMode] must not be null: [" + name + "]");
        }
        this.collectMode = collectMode;
        return this;
    }

    @Override
    protected final MultiTermsAggregationFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        List<ValuesSourceConfig> configs = resolveConfig(context);
        return new MultiTermsAggregationFactory(
            name,
            configs,
            configs.stream().map(ValuesSourceConfig::format).collect(Collectors.toList()),
            showTermDocCountError,
            order,
            collectMode,
            bucketCountThresholds,
            context,
            parent,
            subFactoriesBuilder,
            metadata
        );
    }

    protected List<ValuesSourceConfig> resolveConfig(AggregationContext context) {
        List<ValuesSourceConfig> configs = new ArrayList<>();
        for (MultiValuesSourceFieldConfig field : terms) {
            configs.add(
                ValuesSourceConfig.resolveUnregistered(
                    context,
                    field.getUserValueTypeHint(),
                    field.getFieldName(),
                    field.getScript(),
                    field.getMissing(),
                    field.getTimeZone(),
                    field.getFormat(),
                    CoreValuesSourceType.KEYWORD
                )
            );

        }
        return configs;
    }

    @Override
    public final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        bucketCountThresholds.toXContent(builder, params);
        builder.field(SHOW_TERM_DOC_COUNT_ERROR.getPreferredName(), showTermDocCountError);
        builder.field(ORDER_FIELD.getPreferredName());
        order.toXContent(builder, params);
        if (collectMode != null) {
            builder.field(Aggregator.SubAggCollectionMode.KEY.getPreferredName(), collectMode.parseField().getPreferredName());
        }
        if (terms != null) {
            builder.field(TERMS_FIELD.getPreferredName(), terms);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), terms, order, collectMode, bucketCountThresholds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        MultiTermsAggregationBuilder other = (MultiTermsAggregationBuilder) obj;
        return Objects.equals(this.terms, other.terms)
            && Objects.equals(this.order, other.order)
            && Objects.equals(this.collectMode, other.collectMode)
            && Objects.equals(this.bucketCountThresholds, other.bucketCountThresholds);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
