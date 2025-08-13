/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SimpleTimeZone;
import java.util.function.Consumer;

import static java.util.Map.entry;

/**
 * A builder for histograms on date fields.
 */
public class DateHistogramAggregationBuilder extends ValuesSourceAggregationBuilder<DateHistogramAggregationBuilder>
    implements
        DateIntervalConsumer<DateHistogramAggregationBuilder> {

    public static final String NAME = "date_histogram";
    public static final ValuesSourceRegistry.RegistryKey<DateHistogramAggregationSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, DateHistogramAggregationSupplier.class);

    public static final Map<String, Rounding.DateTimeUnit> DATE_FIELD_UNITS = Map.ofEntries(
        entry("year", Rounding.DateTimeUnit.YEAR_OF_CENTURY),
        entry("1y", Rounding.DateTimeUnit.YEAR_OF_CENTURY),
        entry("quarter", Rounding.DateTimeUnit.QUARTER_OF_YEAR),
        entry("1q", Rounding.DateTimeUnit.QUARTER_OF_YEAR),
        entry("month", Rounding.DateTimeUnit.MONTH_OF_YEAR),
        entry("1M", Rounding.DateTimeUnit.MONTH_OF_YEAR),
        entry("week", Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR),
        entry("1w", Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR),
        entry("day", Rounding.DateTimeUnit.DAY_OF_MONTH),
        entry("1d", Rounding.DateTimeUnit.DAY_OF_MONTH),
        entry("hour", Rounding.DateTimeUnit.HOUR_OF_DAY),
        entry("1h", Rounding.DateTimeUnit.HOUR_OF_DAY),
        entry("minute", Rounding.DateTimeUnit.MINUTE_OF_HOUR),
        entry("1m", Rounding.DateTimeUnit.MINUTE_OF_HOUR),
        entry("second", Rounding.DateTimeUnit.SECOND_OF_MINUTE),
        entry("1s", Rounding.DateTimeUnit.SECOND_OF_MINUTE),
        entry("years", Rounding.DateTimeUnit.YEARS_OF_CENTURY),
        entry("months", Rounding.DateTimeUnit.MONTHS_OF_YEAR)
    );

    public static final ObjectParser<DateHistogramAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
        NAME,
        DateHistogramAggregationBuilder::new
    );
    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, true);
        DateIntervalWrapper.declareIntervalFields(PARSER);

        PARSER.declareField(DateHistogramAggregationBuilder::offset, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            } else {
                return DateHistogramAggregationBuilder.parseStringOffset(p.text());
            }
        }, Histogram.OFFSET_FIELD, ObjectParser.ValueType.LONG);

        PARSER.declareBoolean(DateHistogramAggregationBuilder::keyed, Histogram.KEYED_FIELD);

        PARSER.declareLong(DateHistogramAggregationBuilder::minDocCount, Histogram.MIN_DOC_COUNT_FIELD);

        PARSER.declareField(
            DateHistogramAggregationBuilder::extendedBounds,
            parser -> LongBounds.PARSER.apply(parser, null),
            Histogram.EXTENDED_BOUNDS_FIELD,
            ObjectParser.ValueType.OBJECT
        );

        PARSER.declareField(
            DateHistogramAggregationBuilder::hardBounds,
            parser -> LongBounds.PARSER.apply(parser, null),
            Histogram.HARD_BOUNDS_FIELD,
            ObjectParser.ValueType.OBJECT
        );

        PARSER.declareObjectArray(
            DateHistogramAggregationBuilder::order,
            (p, c) -> InternalOrder.Parser.parseOrderParam(p),
            Histogram.ORDER_FIELD
        );
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        DateHistogramAggregatorFactory.registerAggregators(builder);
    }

    private DateIntervalWrapper dateHistogramInterval = new DateIntervalWrapper();
    private long offset = 0;
    private LongBounds extendedBounds;
    private LongBounds hardBounds;
    private BucketOrder order = BucketOrder.key(true);
    private boolean keyed = false;
    private long minDocCount = 0;

    /** Create a new builder with the given name. */
    public DateHistogramAggregationBuilder(String name) {
        super(name);
    }

    protected DateHistogramAggregationBuilder(
        DateHistogramAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.dateHistogramInterval = clone.dateHistogramInterval;
        this.offset = clone.offset;
        this.extendedBounds = clone.extendedBounds;
        this.hardBounds = clone.hardBounds;
        this.order = clone.order;
        this.keyed = clone.keyed;
        this.minDocCount = clone.minDocCount;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new DateHistogramAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /** Read from a stream, for internal use only. */
    public DateHistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readHistogramOrder(in);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        dateHistogramInterval = new DateIntervalWrapper(in);
        offset = in.readLong();
        extendedBounds = in.readOptionalWriteable(LongBounds::new);
        hardBounds = in.readOptionalWriteable(LongBounds::new);
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.DATE;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out);
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        dateHistogramInterval.writeTo(out);
        out.writeLong(offset);
        out.writeOptionalWriteable(extendedBounds);
        out.writeOptionalWriteable(hardBounds);
    }

    /**
     * Sets the interval of the DateHistogram using calendar units (`1d`, `1w`, `1M`, etc).  These units
     * are calendar-aware, meaning they respect leap additions, variable days per month, etc.
     *
     * This is mutually exclusive with {@link DateHistogramAggregationBuilder#fixedInterval(DateHistogramInterval)}
     *
     * @param interval The calendar interval to use with the aggregation
     */
    @Override
    public DateHistogramAggregationBuilder calendarInterval(DateHistogramInterval interval) {
        dateHistogramInterval.calendarInterval(interval);
        return this;
    }

    /**
     * Sets the interval of the DateHistogram using fixed units (`1ms`, `1s`, `10m`, `4h`, etc).  These are
     * not calendar aware and are simply multiples of fixed, SI units.
     *
     * This is mutually exclusive with {@link DateHistogramAggregationBuilder#calendarInterval(DateHistogramInterval)}
     *
     * @param interval The fixed interval to use with the aggregation
     */
    @Override
    public DateHistogramAggregationBuilder fixedInterval(DateHistogramInterval interval) {
        dateHistogramInterval.fixedInterval(interval);
        return this;
    }

    /**
     * Returns the interval as a date time unit if and only if it was configured as a calendar interval originally.
     * Returns null otherwise.
     */
    public DateHistogramInterval getCalendarInterval() {
        if (dateHistogramInterval.getIntervalType().equals(DateIntervalWrapper.IntervalTypeEnum.CALENDAR)) {
            return dateHistogramInterval.getAsCalendarInterval();
        }
        return null;
    }

    /**
     * Returns the interval as a fixed time unit if and only if it was configured as a fixed interval originally.
     * Returns null otherwise.
     */
    public DateHistogramInterval getFixedInterval() {
        if (dateHistogramInterval.getIntervalType().equals(DateIntervalWrapper.IntervalTypeEnum.FIXED)) {
            return dateHistogramInterval.getAsFixedInterval();
        }
        return null;
    }

    /** Get the offset to use when rounding, which is a number of milliseconds. */
    public long offset() {
        return offset;
    }

    /** Set the offset on this builder, which is a number of milliseconds, and
     *  return the builder so that calls can be chained. */
    public DateHistogramAggregationBuilder offset(long offset) {
        this.offset = offset;
        return this;
    }

    /** Set the offset on this builder, as a time value, and
     *  return the builder so that calls can be chained. */
    public DateHistogramAggregationBuilder offset(String offset) {
        if (offset == null) {
            throw new IllegalArgumentException("[offset] must not be null: [" + name + "]");
        }
        return offset(parseStringOffset(offset));
    }

    /**
     * Parse the string specification of an offset.
     */
    public static long parseStringOffset(String offset) {
        if (offset.charAt(0) == '-') {
            return -TimeValue.parseTimeValue(
                offset.substring(1),
                null,
                DateHistogramAggregationBuilder.class.getSimpleName() + ".parseOffset"
            ).millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue.parseTimeValue(
            offset.substring(beginIndex),
            null,
            DateHistogramAggregationBuilder.class.getSimpleName() + ".parseOffset"
        ).millis();
    }

    /** Return extended bounds for this histogram, or {@code null} if none are set. */
    public LongBounds extendedBounds() {
        return extendedBounds;
    }

    /** Set extended bounds on this histogram, so that buckets would also be
     *  generated on intervals that did not match any documents. */
    public DateHistogramAggregationBuilder extendedBounds(LongBounds extendedBounds) {
        if (extendedBounds == null) {
            throw new IllegalArgumentException("[extendedBounds] must not be null: [" + name + "]");
        }
        this.extendedBounds = extendedBounds;
        return this;
    }

    /** Set hard bounds on this histogram, specifying boundaries outside which buckets cannot be created. */
    public DateHistogramAggregationBuilder hardBounds(LongBounds hardBounds) {
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
    public DateHistogramAggregationBuilder order(BucketOrder order) {
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
    public DateHistogramAggregationBuilder order(List<BucketOrder> orders) {
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
    public DateHistogramAggregationBuilder keyed(boolean keyed) {
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
    public DateHistogramAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]"
            );
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

        dateHistogramInterval.toXContent(builder, params);
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
    protected ValuesSourceAggregatorFactory innerBuild(
        AggregationContext context,
        ValuesSourceConfig config,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        final DateIntervalWrapper.IntervalTypeEnum dateHistogramIntervalType = dateHistogramInterval.getIntervalType();

        boolean downsampledResultsOffset = false;
        final ZoneId tz = timeZone();

        String downsamplingInterval = context.getIndexSettings().getIndexMetadata().getDownsamplingInterval();
        if (downsamplingInterval != null) {
            if (DateIntervalWrapper.IntervalTypeEnum.CALENDAR.equals(dateHistogramIntervalType)) {
                throw new IllegalArgumentException(
                    config.getDescription()
                        + " is not supported for aggregation ["
                        + getName()
                        + "] with interval type ["
                        + dateHistogramIntervalType.getPreferredName()
                        + "]"
                );
            }

            // Downsampled data in time-series indexes contain aggregated values that get calculated over UTC-based intervals.
            // When they get aggregated using a different timezone, the resulting buckets may need to be offset to account for
            // the difference between UTC (where stored data refers to) and the requested timezone. For instance:
            // a. A TZ shifted by -01:15 over hourly downsampled data will lead to buckets with times XX:45, instead of XX:00
            // b. A TZ shifted by +07:00 over daily downsampled data will lead to buckets with times 07:00, instead of 00:00
            // c. Intervals over DST are approximate, not including gaps in time buckets. This applies to date histogram aggregation in
            // general.
            if (tz != null && ZoneId.of("UTC").equals(tz) == false && field().equals(DataStreamTimestampFieldMapper.DEFAULT_PATH)) {

                // Get the downsampling interval.
                DateHistogramInterval interval = new DateHistogramInterval(downsamplingInterval);
                long downsamplingResolution = interval.estimateMillis();
                long aggregationResolution = dateHistogramInterval.getAsFixedInterval().estimateMillis();

                // If the aggregation resolution is not a multiple of the downsampling resolution, the reported time for each
                // bucket needs to be shifted by the mod - in addition to rounding that's applied as usual.
                // Note that the aggregation resolution gets shifted to match the specified timezone. Timezone.getOffset() normally expects
                // a date but it can also process an offset (interval) in milliseconds as it uses the Unix epoch for reference.
                long aggregationOffset = SimpleTimeZone.getTimeZone(tz).getOffset(aggregationResolution) % downsamplingResolution;
                if (aggregationOffset != 0) {
                    downsampledResultsOffset = true;
                    offset += aggregationOffset;
                }
            }
        }

        DateHistogramAggregationSupplier aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        final Rounding rounding = dateHistogramInterval.createRounding(tz, offset);

        LongBounds roundedBounds = null;
        if (this.extendedBounds != null) {
            // parse any string bounds to longs and round
            roundedBounds = this.extendedBounds.parseAndValidate(name, "extended_bounds", context::nowInMillis, config.format())
                .round(rounding);
        }

        LongBounds roundedHardBounds = null;
        if (this.hardBounds != null) {
            // parse any string bounds to longs and round
            roundedHardBounds = this.hardBounds.parseAndValidate(name, "hard_bounds", context::nowInMillis, config.format())
                .round(rounding);
        }

        if (roundedBounds != null && roundedHardBounds != null) {
            if (roundedBounds.getMax() != null
                && roundedHardBounds.getMax() != null
                && roundedBounds.getMax() > roundedHardBounds.getMax()) {
                throw new IllegalArgumentException(
                    "Extended bounds have to be inside hard bounds, hard bounds: ["
                        + hardBounds
                        + "], extended bounds: ["
                        + extendedBounds
                        + "]"
                );
            }
            if (roundedBounds.getMin() != null
                && roundedHardBounds.getMin() != null
                && roundedBounds.getMin() < roundedHardBounds.getMin()) {
                throw new IllegalArgumentException(
                    "Extended bounds have to be inside hard bounds, hard bounds: ["
                        + hardBounds
                        + "], extended bounds: ["
                        + extendedBounds
                        + "]"
                );
            }
        }

        return new DateHistogramAggregatorFactory(
            name,
            config,
            order,
            keyed,
            minDocCount,
            downsampledResultsOffset,
            rounding,
            roundedBounds,
            roundedHardBounds,
            context,
            parent,
            subFactoriesBuilder,
            metadata,
            aggregatorSupplier
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, keyed, minDocCount, dateHistogramInterval, minDocCount, extendedBounds, hardBounds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        DateHistogramAggregationBuilder other = (DateHistogramAggregationBuilder) obj;
        return Objects.equals(order, other.order)
            && Objects.equals(keyed, other.keyed)
            && Objects.equals(minDocCount, other.minDocCount)
            && Objects.equals(dateHistogramInterval, other.dateHistogramInterval)
            && Objects.equals(offset, other.offset)
            && Objects.equals(extendedBounds, other.extendedBounds)
            && Objects.equals(hardBounds, other.hardBounds);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    @Override
    protected void validateSequentiallyOrdered(String type, String name, Consumer<String> addValidationError) {}

    @Override
    protected void validateSequentiallyOrderedWithoutGaps(String type, String name, Consumer<String> addValidationError) {
        if (minDocCount != 0) {
            addValidationError.accept("parent histogram of " + type + " aggregation [" + name + "] must have min_doc_count of 0");
        }
    }
}
