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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Map.entry;

/**
 * A builder for histograms on date fields.
 */
public class DateHistogramAggregationBuilder extends ValuesSourceAggregationBuilder<DateHistogramAggregationBuilder>
        implements DateIntervalConsumer {

    public static final String NAME = "date_histogram";

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
            entry("minute", Rounding.DateTimeUnit.MINUTES_OF_HOUR),
            entry("1m", Rounding.DateTimeUnit.MINUTES_OF_HOUR),
            entry("second", Rounding.DateTimeUnit.SECOND_OF_MINUTE),
            entry("1s", Rounding.DateTimeUnit.SECOND_OF_MINUTE));

    public static final ObjectParser<DateHistogramAggregationBuilder, String> PARSER =
            ObjectParser.fromBuilder(NAME, DateHistogramAggregationBuilder::new);
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

        PARSER.declareField(DateHistogramAggregationBuilder::extendedBounds, parser -> ExtendedBounds.PARSER.apply(parser, null),
                ExtendedBounds.EXTENDED_BOUNDS_FIELD, ObjectParser.ValueType.OBJECT);

        PARSER.declareObjectArray(DateHistogramAggregationBuilder::order, (p, c) -> InternalOrder.Parser.parseOrderParam(p),
                Histogram.ORDER_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        DateHistogramAggregatorFactory.registerAggregators(builder);
    }

    private DateIntervalWrapper dateHistogramInterval = new DateIntervalWrapper();
    private long offset = 0;
    private ExtendedBounds extendedBounds;
    private BucketOrder order = BucketOrder.key(true);
    private boolean keyed = false;
    private long minDocCount = 0;

    /** Create a new builder with the given name. */
    public DateHistogramAggregationBuilder(String name) {
        super(name);
    }

    protected DateHistogramAggregationBuilder(DateHistogramAggregationBuilder clone,
                                              AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.dateHistogramInterval = clone.dateHistogramInterval;
        this.offset = clone.offset;
        this.extendedBounds = clone.extendedBounds;
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
        extendedBounds = in.readOptionalWriteable(ExtendedBounds::new);
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
    }

    /** Get the current interval in milliseconds that is set on this builder. */
    @Deprecated
    public long interval() {
        return dateHistogramInterval.interval();
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained.
     *  If both {@link #interval()} and {@link #dateHistogramInterval()} are set, then the
     *  {@link #dateHistogramInterval()} wins.
     *
     *  @deprecated use {@link #fixedInterval(DateHistogramInterval)} or {@link #calendarInterval(DateHistogramInterval)} instead
     *  @since 7.2.0
     */
    @Deprecated
    public DateHistogramAggregationBuilder interval(long interval) {
        dateHistogramInterval.interval(interval);
        return this;
    }

    /** Get the current date interval that is set on this builder. */
    @Deprecated
    public DateHistogramInterval dateHistogramInterval() {
       return dateHistogramInterval.dateHistogramInterval();
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained.
     *  If both {@link #interval()} and {@link #dateHistogramInterval()} are set, then the
     *  {@link #dateHistogramInterval()} wins.
     *
     *  @deprecated use {@link #fixedInterval(DateHistogramInterval)} or {@link #calendarInterval(DateHistogramInterval)} instead
     *  @since 7.2.0
     */
    @Deprecated
    public DateHistogramAggregationBuilder dateHistogramInterval(DateHistogramInterval interval) {
        dateHistogramInterval.dateHistogramInterval(interval);
        return this;
    }

    /**
     * Sets the interval of the DateHistogram using calendar units (`1d`, `1w`, `1M`, etc).  These units
     * are calendar-aware, meaning they respect leap additions, variable days per month, etc.
     *
     * This is mutually exclusive with {@link DateHistogramAggregationBuilder#fixedInterval(DateHistogramInterval)}
     *
     * @param interval The calendar interval to use with the aggregation
     */
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
            return -TimeValue
                    .parseTimeValue(offset.substring(1), null, DateHistogramAggregationBuilder.class.getSimpleName() + ".parseOffset")
                    .millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue
                .parseTimeValue(offset.substring(beginIndex), null, DateHistogramAggregationBuilder.class.getSimpleName() + ".parseOffset")
                .millis();
    }

    /** Return extended bounds for this histogram, or {@code null} if none are set. */
    public ExtendedBounds extendedBounds() {
        return extendedBounds;
    }

    /** Set extended bounds on this histogram, so that buckets would also be
     *  generated on intervals that did not match any documents. */
    public DateHistogramAggregationBuilder extendedBounds(ExtendedBounds extendedBounds) {
        if (extendedBounds == null) {
            throw new IllegalArgumentException("[extendedBounds] must not be null: [" + name + "]");
        }
        this.extendedBounds = extendedBounds;
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

        dateHistogramInterval.toXContent(builder, params);
        builder.field(Histogram.OFFSET_FIELD.getPreferredName(), offset);

        if (order != null) {
            builder.field(Histogram.ORDER_FIELD.getPreferredName());
            order.toXContent(builder, params);
        }

        builder.field(Histogram.KEYED_FIELD.getPreferredName(), keyed);

        builder.field(Histogram.MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

        if (extendedBounds != null) {
            extendedBounds.toXContent(builder, params);
        }

        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Returns a {@linkplain ZoneId} that functions the same as
     * {@link #timeZone()} on the data in the shard referred to by
     * {@code context}. It <strong>attempts</strong> to convert zones that
     * have non-fixed offsets into fixed offset zones that produce the
     * same results on all data in the shard.
     * <p>
     * We go about this in three phases:
     * <ol>
     * <li>A bunch of preflight checks to see if we *can* optimize it
     * <li>Find the any Instant in shard
     * <li>Find the DST transition before and after that Instant
     * <li>Round those into the interval
     * <li>Check if the rounded value include all values within shard
     * <li>If they do then return a fixed offset time zone because it
     *     will return the same values for all time in the shard as the
     *     original time zone, but faster
     * <li>Otherwise return the original time zone. It'll be slower, but
     *     correct.
     * </ol>
     * <p>
     * NOTE: this can't be done in rewrite() because the timezone is then also used on the
     * coordinating node in order to generate missing buckets, which may cross a transition
     * even though data on the shards doesn't.
     */
    ZoneId rewriteTimeZone(QueryShardContext context) throws IOException {
        final ZoneId tz = timeZone();
        if (tz == null || tz.getRules().isFixedOffset()) {
            // This time zone is already as fast as it is going to get.
            return tz;
        }
        if (script() != null) {
            // We can't be sure what dates the script will return so we don't attempt to optimize anything
            return tz;
        }
        if (field() == null) {
            // Without a field we're not going to be able to look anything up.
            return tz;
        }
        MappedFieldType ft = context.fieldMapper(field());
        if (ft == null || false == ft instanceof DateFieldMapper.DateFieldType) {
            // If the field is unmapped or not a date then we can't get its range.
            return tz;
        }
        DateFieldMapper.DateFieldType dft = (DateFieldMapper.DateFieldType) ft;
        final IndexReader reader = context.getIndexReader();
        if (reader == null) {
            return tz;
        }

        Instant instant = null;
        final IndexNumericFieldData fieldData = context.getForField(ft);
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafNumericFieldData leafFD = fieldData.load(ctx);
            SortedNumericDocValues values = leafFD.getLongValues();
            if (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                instant = Instant.ofEpochMilli(values.nextValue());
                break;
            }
        }
        if (instant == null) {
            return tz;
        }

        ZoneOffsetTransition prevOffsetTransition = tz.getRules().previousTransition(instant);
        final long prevTransition;
        if (prevOffsetTransition != null) {
            prevTransition = prevOffsetTransition.getInstant().toEpochMilli();
        } else {
            prevTransition = instant.toEpochMilli();
        }
        ZoneOffsetTransition nextOffsetTransition = tz.getRules().nextTransition(instant);
        final long nextTransition;
        if (nextOffsetTransition != null) {
            nextTransition = nextOffsetTransition.getInstant().toEpochMilli();
        } else {
            nextTransition = Long.MAX_VALUE; // fixed time-zone after prevTransition
        }

        // We need all not only values but also rounded values to be within
        // [prevTransition, nextTransition].
        final long low;

        DateIntervalWrapper.IntervalTypeEnum intervalType = dateHistogramInterval.getIntervalType();
        if (intervalType.equals(DateIntervalWrapper.IntervalTypeEnum.FIXED)) {
            low = Math.addExact(prevTransition, dateHistogramInterval.tryIntervalAsFixedUnit().millis());
        } else if (intervalType.equals(DateIntervalWrapper.IntervalTypeEnum.CALENDAR)) {
            final Rounding.DateTimeUnit intervalAsUnit = dateHistogramInterval.tryIntervalAsCalendarUnit();
            final Rounding rounding = Rounding.builder(intervalAsUnit).timeZone(timeZone()).build();
            low = rounding.nextRoundingValue(prevTransition);
        } else {
            // We're not sure what the interval was originally (legacy) so use old behavior of assuming
            // calendar first, then fixed. Required because fixed/cal overlap in places ("1h")
            Rounding.DateTimeUnit intervalAsUnit = dateHistogramInterval.tryIntervalAsCalendarUnit();
            if (intervalAsUnit != null) {
                final Rounding rounding = Rounding.builder(intervalAsUnit).timeZone(timeZone()).build();
                low = rounding.nextRoundingValue(prevTransition);
            } else {
                final TimeValue intervalAsMillis =  dateHistogramInterval.tryIntervalAsFixedUnit();
                low = Math.addExact(prevTransition, intervalAsMillis.millis());
            }
        }
        // rounding rounds down, so 'nextTransition' is a good upper bound
        final long high = nextTransition;

        if (dft.isFieldWithinRange(
                        reader, Instant.ofEpochMilli(low), Instant.ofEpochMilli(high - 1)) == Relation.WITHIN) {
            // All values in this reader have the same offset despite daylight saving times.
            // This is very common for location-based timezones such as Europe/Paris in
            // combination with time-based indices.
            return ZoneOffset.ofTotalSeconds(tz.getRules().getOffset(instant).getTotalSeconds());
        }
        return tz;
    }

    @Override
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        final ZoneId tz = timeZone();
        final Rounding rounding = dateHistogramInterval.createRounding(tz, offset);
        // TODO once we optimize TimeIntervalRounding we won't need to rewrite the time zone 
        final ZoneId rewrittenTimeZone = rewriteTimeZone(queryShardContext);
        final Rounding shardRounding;
        if (tz == rewrittenTimeZone) {
            shardRounding = rounding;
        } else {
            shardRounding = dateHistogramInterval.createRounding(rewrittenTimeZone, offset);
        }

        ExtendedBounds roundedBounds = null;
        if (this.extendedBounds != null) {
            // parse any string bounds to longs and round
            roundedBounds = this.extendedBounds.parseAndValidate(name, queryShardContext, config.format()).round(rounding);
        }
        return new DateHistogramAggregatorFactory(name, config, order, keyed, minDocCount,
                rounding, shardRounding, roundedBounds, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order, keyed, minDocCount, dateHistogramInterval, minDocCount, extendedBounds);
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
                && Objects.equals(extendedBounds, other.extendedBounds);
    }
}
