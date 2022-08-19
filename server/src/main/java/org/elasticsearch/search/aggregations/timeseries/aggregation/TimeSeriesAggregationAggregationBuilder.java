/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry.RegistryKey;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TimeSeriesAggregationAggregationBuilder extends ValuesSourceAggregationBuilder<TimeSeriesAggregationAggregationBuilder> {
    public static final String NAME = "time_series_aggregation";
    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    public static final ParseField GROUP_FIELD = new ParseField("group");
    public static final ParseField WITHOUT_FIELD = new ParseField("without");
    public static final ParseField INTERVAL_FIELD = new ParseField("interval");
    public static final ParseField OFFSET_FIELD = new ParseField("offset");
    public static final ParseField AGGREGATOR_FIELD = new ParseField("aggregator");
    public static final ParseField AGGREGATOR_PARAMS_FIELD = new ParseField("aggregator_params");
    public static final ParseField DOWNSAMPLE_FIELD = new ParseField("downsample");
    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD = new ParseField("min_doc_count");
    public static final ParseField SHARD_MIN_DOC_COUNT_FIELD = new ParseField("shard_min_doc_count");
    public static final ParseField START_TIME_FIELD = new ParseField("start_time");
    public static final ParseField END_TIME_FIELD = new ParseField("end_time");

    static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(
        1,
        0,
        10,
        -1
    );

    public static final RegistryKey<TimeSeriesAggregationAggregatorSupplier> REGISTRY_KEY = new RegistryKey<>(
        NAME,
        TimeSeriesAggregationAggregatorSupplier.class
    );

    public static final ObjectParser<TimeSeriesAggregationAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        TimeSeriesAggregationAggregationBuilder::new
    );

    private boolean keyed;
    private List<String> group;
    private List<String> without;
    private DateHistogramInterval interval;
    private DateHistogramInterval offset;
    private Aggregator aggregator;
    private Map<String, Object> aggregatorParams;
    private Downsample downsample;
    private TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(
        DEFAULT_BUCKET_COUNT_THRESHOLDS
    );
    private BucketOrder order = BucketOrder.key(true);
    private Long startTime;
    private Long endTime;

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, false, true, false);

        PARSER.declareField(
            TimeSeriesAggregationAggregationBuilder::interval,
            p -> new DateHistogramInterval(p.text()),
            INTERVAL_FIELD,
            ObjectParser.ValueType.STRING
        );

        PARSER.declareField(
            TimeSeriesAggregationAggregationBuilder::offset,
            p -> new DateHistogramInterval(p.text()),
            OFFSET_FIELD,
            ObjectParser.ValueType.STRING
        );

        PARSER.declareBoolean(TimeSeriesAggregationAggregationBuilder::setKeyed, KEYED_FIELD);
        PARSER.declareStringArray(TimeSeriesAggregationAggregationBuilder::group, GROUP_FIELD);
        PARSER.declareStringArray(TimeSeriesAggregationAggregationBuilder::without, WITHOUT_FIELD);
        PARSER.declareString(TimeSeriesAggregationAggregationBuilder::aggregator, AGGREGATOR_FIELD);
        PARSER.declareObject(
            TimeSeriesAggregationAggregationBuilder::aggregatorParams,
            (parser, c) -> parser.map(),
            AGGREGATOR_PARAMS_FIELD
        );
        PARSER.declareObject(TimeSeriesAggregationAggregationBuilder::downsample, (p, c) -> Downsample.fromXContent(p), DOWNSAMPLE_FIELD);
        PARSER.declareObjectArray(
            TimeSeriesAggregationAggregationBuilder::order,
            (p, c) -> InternalOrder.Parser.parseOrderParam(p),
            ORDER_FIELD
        );
        PARSER.declareInt(TimeSeriesAggregationAggregationBuilder::size, SIZE_FIELD);
        PARSER.declareInt(TimeSeriesAggregationAggregationBuilder::shardSize, SHARD_SIZE_FIELD);
        PARSER.declareInt(TimeSeriesAggregationAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD);
        PARSER.declareInt(TimeSeriesAggregationAggregationBuilder::shardMinDocCount, SHARD_MIN_DOC_COUNT_FIELD);
        PARSER.declareLong(TimeSeriesAggregationAggregationBuilder::startTime, START_TIME_FIELD);
        PARSER.declareLong(TimeSeriesAggregationAggregationBuilder::endTime, END_TIME_FIELD);
    }

    public TimeSeriesAggregationAggregationBuilder(String name) {
        super(name);
    }

    protected TimeSeriesAggregationAggregationBuilder(
        TimeSeriesAggregationAggregationBuilder clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.keyed = clone.keyed;
        this.group = clone.group;
        this.without = clone.group;
        this.interval = clone.interval;
        this.offset = clone.offset;
        this.aggregator = clone.aggregator;
        this.aggregatorParams = clone.aggregatorParams;
        this.downsample = clone.downsample;
        this.order = clone.order;
        this.bucketCountThresholds = clone.bucketCountThresholds;
        this.startTime = clone.startTime;
        this.endTime = clone.endTime;
    }

    public TimeSeriesAggregationAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        group = in.readOptionalStringList();
        without = in.readOptionalStringList();
        interval = in.readOptionalWriteable(DateHistogramInterval::new);
        offset = in.readOptionalWriteable(DateHistogramInterval::new);
        aggregator = in.readOptionalEnum(Aggregator.class);
        aggregatorParams = in.readMap();
        downsample = in.readOptionalWriteable(Downsample::new);
        order = InternalOrder.Streams.readOrder(in);
        bucketCountThresholds = new TermsAggregator.BucketCountThresholds(in);
        startTime = in.readOptionalLong();
        endTime = in.readOptionalLong();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeOptionalStringCollection(group);
        out.writeOptionalStringCollection(without);
        out.writeOptionalWriteable(interval);
        out.writeOptionalWriteable(offset);
        out.writeOptionalEnum(aggregator);
        out.writeGenericMap(aggregatorParams);
        out.writeOptionalWriteable(downsample);
        order.writeTo(out);
        bucketCountThresholds.writeTo(out);
        out.writeOptionalLong(startTime);
        out.writeOptionalLong(endTime);
    }

    @Override
    protected RegistryKey<?> getRegistryKey() {
        return REGISTRY_KEY;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        TimeSeriesAggregationAggregationFactory.registerAggregators(builder);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        Builder subFactoriesBuilder
    ) throws IOException {
        TimeSeriesAggregationAggregatorSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new TimeSeriesAggregationAggregationFactory(
            name,
            keyed,
            group,
            without,
            interval,
            offset,
            aggregator,
            aggregatorParams,
            downsample,
            bucketCountThresholds,
            order,
            startTime != null ? startTime : -1,
            endTime != null ? endTime : -1,
            config,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEYED_FIELD.getPreferredName(), keyed);
        if (group != null) {
            builder.field(GROUP_FIELD.getPreferredName(), group);
        }
        if (without != null) {
            builder.field(WITHOUT_FIELD.getPreferredName(), without);
        }
        if (interval != null) {
            builder.field(INTERVAL_FIELD.getPreferredName(), interval.toString());
        }
        if (offset != null) {
            builder.field(OFFSET_FIELD.getPreferredName(), offset.toString());
        }
        if (aggregator != null) {
            builder.field(AGGREGATOR_FIELD.getPreferredName(), aggregator);
        }
        if (aggregatorParams != null) {
            builder.field(AGGREGATOR_PARAMS_FIELD.getPreferredName(), aggregatorParams);
        }
        if (downsample != null) {
            builder.field(DOWNSAMPLE_FIELD.getPreferredName(), downsample);
        }
        bucketCountThresholds.toXContent(builder, params);
        builder.field(ORDER_FIELD.getPreferredName());
        order.toXContent(builder, params);
        if (startTime != null) {
            builder.field(START_TIME_FIELD.getPreferredName(), startTime);
        }
        if (endTime != null) {
            builder.field(END_TIME_FIELD.getPreferredName(), endTime);
        }
        return builder;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TimeSeriesAggregationAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public boolean isInSortOrderExecutionRequired() {
        return false;
    }

    /**
     * Returns the keyed value
     */
    public boolean isKeyed() {
        return keyed;
    }

    /**
     * Sets if keyed for the results.
     */
    public void setKeyed(boolean keyed) {
        this.keyed = keyed;
    }

    /**
     * Returns the group values
     */
    public List<String> getGroup() {
        return group;
    }

    /**
     * Sets the group values, it used to include dimension fields
     */
    public TimeSeriesAggregationAggregationBuilder group(List<String> group) {
        this.group = group;
        return this;
    }

    /**
     * Returns the without values
     */
    public List<String> getWithout() {
        return without;
    }

    /**
     * Sets the without values, it used to exclude dimension fields
     */
    public TimeSeriesAggregationAggregationBuilder without(List<String> without) {
        this.without = without;
        return this;
    }

    /**
     * Return the interval value
     */
    public DateHistogramInterval getInterval() {
        return interval;
    }

    /**
     * Sets the interval value
     */
    public TimeSeriesAggregationAggregationBuilder interval(DateHistogramInterval interval) {
        this.interval = interval;
        return this;
    }

    /**
     * Return the offset value
     */
    public DateHistogramInterval getOffset() {
        return offset;
    }

    /**
     * Sets the offset value
     */
    public TimeSeriesAggregationAggregationBuilder offset(DateHistogramInterval offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Returns the aggregator function
     */
    public Aggregator getAggregator() {
        return aggregator;
    }

    /**
     * Sets the aggregator function, it used to aggregator time series lines to one time serie line
     */
    public TimeSeriesAggregationAggregationBuilder aggregator(String aggregator) {
        this.aggregator = Aggregator.resolve(aggregator);
        return this;
    }

    public Map<String, Object> getAggregatorParams() {
        return aggregatorParams;
    }

    public TimeSeriesAggregationAggregationBuilder aggregatorParams(Map<String, Object> aggregatorParams) {
        this.aggregatorParams = aggregatorParams;
        return this;
    }

    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public TimeSeriesAggregationAggregationBuilder size(int size) {
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
    public TimeSeriesAggregationAggregationBuilder shardSize(int shardSize) {
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
    public TimeSeriesAggregationAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 1) {
            throw new IllegalArgumentException(
                "[minDocCount] must be greater than or equal to 1. Found [" + minDocCount + "] in [" + name + "]"
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
    public TimeSeriesAggregationAggregationBuilder shardMinDocCount(long shardMinDocCount) {
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

    /**
     * Set a new order on this builder and return the builder so that calls
     * can be chained. A tie-breaker may be added to avoid non-deterministic ordering.
     */
    public TimeSeriesAggregationAggregationBuilder order(BucketOrder order) {
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
    public TimeSeriesAggregationAggregationBuilder order(List<BucketOrder> orders) {
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
     * Returns the downsample value
     */
    public Downsample getDownsample() {
        return downsample;
    }

    /**
     * Sets the downsample value
     */
    public TimeSeriesAggregationAggregationBuilder downsample(Downsample downsample) {
        this.downsample = downsample;
        return this;
    }

    /**
     * Sets the downsample value
     */
    public TimeSeriesAggregationAggregationBuilder downsample(DateHistogramInterval range, Function function, Map<String, Object> params) {
        this.downsample = new Downsample(range, function, params);
        return this;
    }

    public Long startTime() {
        return startTime;
    }

    public TimeSeriesAggregationAggregationBuilder startTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public Long endTime() {
        return endTime;
    }

    public TimeSeriesAggregationAggregationBuilder endTime(long endTime) {
        this.endTime = endTime;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (false == super.equals(o)) {
            return false;
        }
        TimeSeriesAggregationAggregationBuilder that = (TimeSeriesAggregationAggregationBuilder) o;
        return keyed == that.keyed
            && Objects.equals(group, that.group)
            && Objects.equals(without, that.without)
            && Objects.equals(interval, that.interval)
            && Objects.equals(offset, that.offset)
            && aggregator == that.aggregator
            && Objects.equals(aggregatorParams, that.aggregatorParams)
            && Objects.equals(downsample, that.downsample)
            && Objects.equals(bucketCountThresholds, that.bucketCountThresholds)
            && Objects.equals(order, that.order)
            && Objects.equals(startTime, that.startTime)
            && Objects.equals(endTime, that.endTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            keyed,
            group,
            without,
            interval,
            offset,
            aggregator,
            aggregatorParams,
            downsample,
            bucketCountThresholds,
            order,
            startTime,
            endTime
        );
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_3_0;
    }
}
