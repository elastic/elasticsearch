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
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Relation;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
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
public class DateHistogramAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource, DateHistogramAggregationBuilder>
        implements MultiBucketAggregationBuilder, DateIntervalConsumer {

    public static final String NAME = "date_histogram";
    private static final DateMathParser EPOCH_MILLIS_PARSER = DateFormatter.forPattern("epoch_millis").toDateMathParser();

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

    private static final ObjectParser<DateHistogramAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(DateHistogramAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareAnyFields(PARSER, true, true, true);

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

    public static DateHistogramAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new DateHistogramAggregationBuilder(aggregationName), null);
    }

    private DateIntervalWrapper dateHistogramInterval = new DateIntervalWrapper();
    private long offset = 0;
    private ExtendedBounds extendedBounds;
    private BucketOrder order = BucketOrder.key(true);
    private boolean keyed = false;
    private long minDocCount = 0;

    /** Create a new builder with the given name. */
    public DateHistogramAggregationBuilder(String name) {
        super(name, CoreValuesSourceType.ANY, ValueType.DATE);
    }

    protected DateHistogramAggregationBuilder(DateHistogramAggregationBuilder clone,
                                              Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.dateHistogramInterval = clone.dateHistogramInterval;
        this.offset = clone.offset;
        this.extendedBounds = clone.extendedBounds;
        this.order = clone.order;
        this.keyed = clone.keyed;
        this.minDocCount = clone.minDocCount;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new DateHistogramAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /** Read from a stream, for internal use only. */
    public DateHistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in, CoreValuesSourceType.ANY, ValueType.DATE);
        order = InternalOrder.Streams.readHistogramOrder(in);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        dateHistogramInterval = new DateIntervalWrapper(in);
        offset = in.readLong();
        extendedBounds = in.readOptionalWriteable(ExtendedBounds::new);
    }

    @Override
    protected ValuesSourceType resolveScriptAny(Script script) {
        // TODO: No idea how we'd support Range scripts here.
        return CoreValuesSourceType.NUMERIC;
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

    /*
     * NOTE: this can't be done in rewrite() because the timezone is then also used on the
     * coordinating node in order to generate missing buckets, which may cross a transition
     * even though data on the shards doesn't.
     */
    ZoneId rewriteTimeZone(QueryShardContext context) throws IOException {
        final ZoneId tz = timeZone();
        if (field() != null &&
                tz != null &&
                tz.getRules().isFixedOffset() == false &&
                field() != null &&
                script() == null) {
            final MappedFieldType ft = context.fieldMapper(field());
            final IndexReader reader = context.getIndexReader();
            if (ft != null && reader != null) {
                Long anyInstant = null;
                final IndexNumericFieldData fieldData = context.getForField(ft);
                for (LeafReaderContext ctx : reader.leaves()) {
                    AtomicNumericFieldData leafFD = fieldData.load(ctx);
                    SortedNumericDocValues values = leafFD.getLongValues();
                    if (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        anyInstant = values.nextValue();
                        break;
                    }
                }

                if (anyInstant != null) {
                    Instant instant = Instant.ofEpochMilli(anyInstant);
                    ZoneOffsetTransition prevOffsetTransition = tz.getRules().previousTransition(instant);
                    final long prevTransition;
                    if (prevOffsetTransition  != null) {
                        prevTransition = prevOffsetTransition.getInstant().toEpochMilli();
                    } else {
                        prevTransition = instant.toEpochMilli();
                    }
                    ZoneOffsetTransition nextOffsetTransition = tz.getRules().nextTransition(instant);
                    final long nextTransition;
                    if (nextOffsetTransition != null) {
                        nextTransition = nextOffsetTransition.getInstant().toEpochMilli();
                    } else {
                        nextTransition = instant.toEpochMilli();
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

                    if (ft.isFieldWithinQuery(reader, low, high, true, false, ZoneOffset.UTC, EPOCH_MILLIS_PARSER,
                            context) == Relation.WITHIN) {
                        // All values in this reader have the same offset despite daylight saving times.
                        // This is very common for location-based timezones such as Europe/Paris in
                        // combination with time-based indices.
                        return ZoneOffset.ofTotalSeconds(tz.getRules().getOffset(instant).getTotalSeconds());
                    }
                }
            }
        }
        return tz;
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource> innerBuild(QueryShardContext queryShardContext,
                                                                        ValuesSourceConfig<ValuesSource> config,
                                                                        AggregatorFactory parent,
                                                                        Builder subFactoriesBuilder) throws IOException {
        final ZoneId tz = timeZone();
        // TODO use offset here rather than explicitly in the aggregation
        final Rounding rounding = dateHistogramInterval.createRounding(tz, offset);
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
                rounding, shardRounding, roundedBounds, queryShardContext, parent, subFactoriesBuilder, metaData);
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
