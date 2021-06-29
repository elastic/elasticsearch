/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A builder for histograms on numeric fields.  This builder can operate on either base numeric fields, or numeric range fields.  IP range
 * fields are unsupported, and will throw at the factory layer.
 */
public class HistogramAggregationBuilder extends ValuesSourceAggregationBuilder<HistogramAggregationBuilder> {
    public static final String NAME = "histogram";
    public static final ValuesSourceRegistry.RegistryKey<HistogramAggregatorSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, HistogramAggregatorSupplier.class);

    private static final ObjectParser<double[], Void> EXTENDED_BOUNDS_PARSER = new ObjectParser<>(
            Histogram.EXTENDED_BOUNDS_FIELD.getPreferredName(),
            () -> new double[]{ Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY });
    static {
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[0] = d, new ParseField("min"));
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[1] = d, new ParseField("max"));
    }

    public static final ObjectParser<HistogramAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, HistogramAggregationBuilder::new);
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);

        PARSER.declareDouble(HistogramAggregationBuilder::interval, Histogram.INTERVAL_FIELD);

        PARSER.declareDouble(HistogramAggregationBuilder::offset, Histogram.OFFSET_FIELD);

        PARSER.declareBoolean(HistogramAggregationBuilder::keyed, Histogram.KEYED_FIELD);

        PARSER.declareLong(HistogramAggregationBuilder::minDocCount, Histogram.MIN_DOC_COUNT_FIELD);

        PARSER.declareField(HistogramAggregationBuilder::extendedBounds, parser -> DoubleBounds.PARSER.apply(parser, null),
            Histogram.EXTENDED_BOUNDS_FIELD, ObjectParser.ValueType.OBJECT);

        PARSER.declareField(HistogramAggregationBuilder::hardBounds, parser -> DoubleBounds.PARSER.apply(parser, null),
            Histogram.HARD_BOUNDS_FIELD, ObjectParser.ValueType.OBJECT);

        PARSER.declareObjectArray(HistogramAggregationBuilder::order, (p, c) -> InternalOrder.Parser.parseOrderParam(p),
            Histogram.ORDER_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        HistogramAggregatorFactory.registerAggregators(builder);
    }

    private double interval;
    private double offset = 0;
    private DoubleBounds extendedBounds;
    private DoubleBounds hardBounds;
    private BucketOrder order = BucketOrder.key(true);
    private boolean keyed = false;
    private long minDocCount = 0;

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    /** Create a new builder with the given name. */
    public HistogramAggregationBuilder(String name) {
        super(name);
    }

    protected HistogramAggregationBuilder(HistogramAggregationBuilder clone,
                                          AggregatorFactories.Builder factoriesBuilder,
                                          Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.interval = clone.interval;
        this.offset = clone.offset;
        this.extendedBounds = clone.extendedBounds;
        this.hardBounds = clone.hardBounds;
        this.order = clone.order;
        this.keyed = clone.keyed;
        this.minDocCount = clone.minDocCount;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new HistogramAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /** Read from a stream, for internal use only. */
    public HistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readHistogramOrder(in);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        interval = in.readDouble();
        offset = in.readDouble();
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            extendedBounds = in.readOptionalWriteable(DoubleBounds::new);
            hardBounds = in.readOptionalWriteable(DoubleBounds::new);
        } else {
            double minBound = in.readDouble();
            double maxBound = in.readDouble();
            if (minBound ==  Double.POSITIVE_INFINITY && maxBound == Double.NEGATIVE_INFINITY) {
                extendedBounds = null;
            } else {
                extendedBounds = new DoubleBounds(minBound, maxBound);
            }
        }
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out);
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        out.writeDouble(interval);
        out.writeDouble(offset);
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeOptionalWriteable(extendedBounds);
            out.writeOptionalWriteable(hardBounds);
        } else {
            if (extendedBounds != null) {
                out.writeDouble(extendedBounds.getMin());
                out.writeDouble(extendedBounds.getMax());
            } else {
                out.writeDouble(Double.POSITIVE_INFINITY);
                out.writeDouble(Double.NEGATIVE_INFINITY);
            }
        }
    }

    /** Get the current interval that is set on this builder. */
    public double interval() {
        return interval;
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained. */
    public HistogramAggregationBuilder interval(double interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be >0 for histogram aggregation [" + name + "]");
        }
        this.interval = interval;
        return this;
    }

    /** Get the current offset that is set on this builder. */
    public double offset() {
        return offset;
    }

    /** Set the offset on this builder, and return the builder so that calls can be chained. */
    public HistogramAggregationBuilder offset(double offset) {
        this.offset = offset;
        return this;
    }

    /** Get the current minimum bound that is set on this builder. */
    public double minBound() {
        return DoubleBounds.getEffectiveMin(extendedBounds);
    }

    /** Get the current maximum bound that is set on this builder. */
    public double maxBound() {
        return DoubleBounds.getEffectiveMax(extendedBounds);
    }

    protected DoubleBounds extendedBounds() {
        return extendedBounds;
    }

    /**
     * Set extended bounds on this builder: buckets between {@code minBound} and
     * {@code maxBound} will be created even if no documents fell into these
     * buckets.
     *
     * @throws IllegalArgumentException
     *             if maxBound is less that minBound, or if either of the bounds
     *             are not finite.
     */
    public HistogramAggregationBuilder extendedBounds(double minBound, double maxBound) {
        return extendedBounds(new DoubleBounds(minBound, maxBound));
    }

    /**
     * Set extended bounds on this builder: buckets between {@code minBound} and
     * {@code maxBound} will be created even if no documents fell into these
     * buckets.
     *
     * @throws IllegalArgumentException
     *             if maxBound is less that minBound, or if either of the bounds
     *             are not finite.
     */
    public HistogramAggregationBuilder extendedBounds(DoubleBounds extendedBounds) {
        if (extendedBounds == null) {
            throw new IllegalArgumentException("[extended_bounds] must not be null: [" + name + "]");
        }
        this.extendedBounds = extendedBounds;
        return this;
    }

    /**
     * Set hard bounds on this histogram, specifying boundaries outside which buckets cannot be created.
     */
    public HistogramAggregationBuilder hardBounds(DoubleBounds hardBounds) {
        if (hardBounds == null) {
            throw new IllegalArgumentException("[hardBounds] must not be null: [" + name + "]");
        }
        this.hardBounds = hardBounds;
        return this;
    }

    /** Return the order to use to sort buckets of this histogram. */
    public BucketOrder order() {
        return order;
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
    public HistogramAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        if(order instanceof CompoundOrder || InternalOrder.isKeyOrder(order)) {
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
    public HistogramAggregationBuilder order(List<BucketOrder> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        // if the list only contains one order use that to avoid inconsistent xcontent
        order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
        return this;
    }

    /** Return whether buckets should be returned as a hash. In case
     *  {@code keyed} is false, buckets will be returned as an array. */
    public boolean keyed() {
        return keyed;
    }

    /** Set whether to return buckets as a hash or as an array, and return the
     *  builder so that calls can be chained. */
    public HistogramAggregationBuilder keyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /** Return the minimum count of documents that buckets need to have in order
     *  to be included in the response. */
    public long minDocCount() {
        return minDocCount;
    }

    /** Set the minimum count of matching documents that buckets need to have
     *  and return this builder so that calls can be chained. */
    public HistogramAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        this.minDocCount = minDocCount;
        return this;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

        builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
        builder.field(Histogram.OFFSET_FIELD.getPreferredName(), offset);

        if (order != null) {
            builder.field(Histogram.ORDER_FIELD.getPreferredName());
            order.toXContent(builder, params);
        }

        builder.field(Histogram.KEYED_FIELD.getPreferredName(), keyed);

        builder.field(Histogram.MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

        if (extendedBounds != null) {
            builder.startObject(Histogram.EXTENDED_BOUNDS_FIELD.getPreferredName());
            extendedBounds.toXContent(builder, params);
            builder.endObject();
        }

        if (hardBounds != null) {
            builder.startObject(Histogram.HARD_BOUNDS_FIELD.getPreferredName());
            hardBounds.toXContent(builder, params);
            builder.endObject();
        }

        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(AggregationContext context,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        HistogramAggregatorSupplier aggregatorSupplier =
            context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);

        if (hardBounds != null && extendedBounds != null) {
            if (hardBounds.getMax() != null && extendedBounds.getMax() != null && hardBounds.getMax() < extendedBounds.getMax()) {
                throw new IllegalArgumentException("Extended bounds have to be inside hard bounds, hard bounds: [" +
                    hardBounds + "], extended bounds: [" + extendedBounds.getMin() + "--" + extendedBounds.getMax() + "]");
            }
            if (hardBounds.getMin() != null && extendedBounds.getMin() != null && hardBounds.getMin() > extendedBounds.getMin()) {
                throw new IllegalArgumentException("Extended bounds have to be inside hard bounds, hard bounds: [" +
                    hardBounds + "], extended bounds: [" + extendedBounds.getMin() + "--" + extendedBounds.getMax() + "]");
            }
        }

        return new HistogramAggregatorFactory(name, config, interval, offset, order, keyed, minDocCount, extendedBounds,
            hardBounds, context, parent, subFactoriesBuilder, metadata, aggregatorSupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, keyed, minDocCount, interval, offset, extendedBounds, hardBounds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        HistogramAggregationBuilder other = (HistogramAggregationBuilder) obj;
        return Objects.equals(order, other.order)
            && Objects.equals(keyed, other.keyed)
            && Objects.equals(minDocCount, other.minDocCount)
            && Objects.equals(interval, other.interval)
            && Objects.equals(offset, other.offset)
            && Objects.equals(extendedBounds, other.extendedBounds)
            && Objects.equals(hardBounds, other.hardBounds);
    }
}
