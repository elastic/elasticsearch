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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * A builder for histograms on date fields.
 */
public class DateHistogramAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.Numeric, DateHistogramAggregationBuilder>
        implements MultiBucketAggregationBuilder {
    public static final String NAME = "date_histogram";
    private static DateMathParser EPOCH_MILLIS_PARSER = Joda.forPattern("epoch_millis", Locale.ROOT).toDateMathParser();

    public static final Map<String, DateTimeUnit> DATE_FIELD_UNITS;

    static {
        Map<String, DateTimeUnit> dateFieldUnits = new HashMap<>();
        dateFieldUnits.put("year", DateTimeUnit.YEAR_OF_CENTURY);
        dateFieldUnits.put("1y", DateTimeUnit.YEAR_OF_CENTURY);
        dateFieldUnits.put("quarter", DateTimeUnit.QUARTER);
        dateFieldUnits.put("1q", DateTimeUnit.QUARTER);
        dateFieldUnits.put("month", DateTimeUnit.MONTH_OF_YEAR);
        dateFieldUnits.put("1M", DateTimeUnit.MONTH_OF_YEAR);
        dateFieldUnits.put("week", DateTimeUnit.WEEK_OF_WEEKYEAR);
        dateFieldUnits.put("1w", DateTimeUnit.WEEK_OF_WEEKYEAR);
        dateFieldUnits.put("day", DateTimeUnit.DAY_OF_MONTH);
        dateFieldUnits.put("1d", DateTimeUnit.DAY_OF_MONTH);
        dateFieldUnits.put("hour", DateTimeUnit.HOUR_OF_DAY);
        dateFieldUnits.put("1h", DateTimeUnit.HOUR_OF_DAY);
        dateFieldUnits.put("minute", DateTimeUnit.MINUTES_OF_HOUR);
        dateFieldUnits.put("1m", DateTimeUnit.MINUTES_OF_HOUR);
        dateFieldUnits.put("second", DateTimeUnit.SECOND_OF_MINUTE);
        dateFieldUnits.put("1s", DateTimeUnit.SECOND_OF_MINUTE);
        DATE_FIELD_UNITS = unmodifiableMap(dateFieldUnits);
    }

    private static final ObjectParser<DateHistogramAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(DateHistogramAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareNumericFields(PARSER, true, true, true);

        PARSER.declareField((histogram, interval) -> {
            if (interval instanceof Long) {
                histogram.interval((long) interval);
            } else {
                histogram.dateHistogramInterval((DateHistogramInterval) interval);
            }
        }, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            } else {
                return new DateHistogramInterval(p.text());
            }
        }, Histogram.INTERVAL_FIELD, ObjectParser.ValueType.LONG);

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

    private long interval;
    private DateHistogramInterval dateHistogramInterval;
    private long offset = 0;
    private ExtendedBounds extendedBounds;
    private BucketOrder order = BucketOrder.key(true);
    private boolean keyed = false;
    private long minDocCount = 0;

    /** Create a new builder with the given name. */
    public DateHistogramAggregationBuilder(String name) {
        super(name, ValuesSourceType.NUMERIC, ValueType.DATE);
    }

    protected DateHistogramAggregationBuilder(DateHistogramAggregationBuilder clone,
                                              Builder factoriesBuilder, Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        this.interval = clone.interval;
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
        super(in, ValuesSourceType.NUMERIC, ValueType.DATE);
        order = InternalOrder.Streams.readHistogramOrder(in, true);
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        interval = in.readLong();
        dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        offset = in.readLong();
        extendedBounds = in.readOptionalWriteable(ExtendedBounds::new);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out, true);
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        out.writeLong(interval);
        out.writeOptionalWriteable(dateHistogramInterval);
        out.writeLong(offset);
        out.writeOptionalWriteable(extendedBounds);
    }

    /** Get the current interval in milliseconds that is set on this builder. */
    public long interval() {
        return interval;
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained.
     *  If both {@link #interval()} and {@link #dateHistogramInterval()} are set, then the
     *  {@link #dateHistogramInterval()} wins. */
    public DateHistogramAggregationBuilder interval(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException("[interval] must be 1 or greater for histogram aggregation [" + name + "]");
        }
        this.interval = interval;
        return this;
    }

    /** Get the current date interval that is set on this builder. */
    public DateHistogramInterval dateHistogramInterval() {
        return dateHistogramInterval;
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained.
     *  If both {@link #interval()} and {@link #dateHistogramInterval()} are set, then the
     *  {@link #dateHistogramInterval()} wins. */
    public DateHistogramAggregationBuilder dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null: [" + name + "]");
        }
        this.dateHistogramInterval = dateHistogramInterval;
        return this;
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

    static long parseStringOffset(String offset) {
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

        if (dateHistogramInterval == null) {
            builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
        } else {
            builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), dateHistogramInterval.toString());
        }
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
    DateTimeZone rewriteTimeZone(QueryShardContext context) throws IOException {
        final DateTimeZone tz = timeZone();
        if (field() != null &&
                tz != null &&
                tz.isFixed() == false &&
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
                    final long prevTransition = tz.previousTransition(anyInstant);
                    final long nextTransition = tz.nextTransition(anyInstant);

                    // We need all not only values but also rounded values to be within
                    // [prevTransition, nextTransition].
                    final long low;
                    DateTimeUnit intervalAsUnit = getIntervalAsDateTimeUnit();
                    if (intervalAsUnit != null) {
                        final DateTimeField dateTimeField = intervalAsUnit.field(tz);
                        low = dateTimeField.roundCeiling(prevTransition);
                    } else {
                        final TimeValue intervalAsMillis = getIntervalAsTimeValue();
                        low = Math.addExact(prevTransition, intervalAsMillis.millis());
                    }
                    // rounding rounds down, so 'nextTransition' is a good upper bound
                    final long high = nextTransition;

                    if (ft.isFieldWithinQuery(reader, low, high, true, false, DateTimeZone.UTC, EPOCH_MILLIS_PARSER,
                            context) == Relation.WITHIN) {
                        // All values in this reader have the same offset despite daylight saving times.
                        // This is very common for location-based timezones such as Europe/Paris in
                        // combination with time-based indices.
                        return DateTimeZone.forOffsetMillis(tz.getOffset(anyInstant));
                    }
                }
            }
        }
        return tz;
    }

    @Override
    protected ValuesSourceAggregatorFactory<Numeric, ?> innerBuild(SearchContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        final DateTimeZone tz = timeZone();
        final Rounding rounding = createRounding(tz);
        final DateTimeZone rewrittenTimeZone = rewriteTimeZone(context.getQueryShardContext());
        final Rounding shardRounding;
        if (tz == rewrittenTimeZone) {
            shardRounding = rounding;
        } else {
            shardRounding = createRounding(rewrittenTimeZone);
        }

        ExtendedBounds roundedBounds = null;
        if (this.extendedBounds != null) {
            // parse any string bounds to longs and round
            roundedBounds = this.extendedBounds.parseAndValidate(name, context, config.format()).round(rounding);
        }
        return new DateHistogramAggregatorFactory(name, config, offset, order, keyed, minDocCount,
                rounding, shardRounding, roundedBounds, context, parent, subFactoriesBuilder, metaData);
    }

    /** Return the interval as a date time unit if applicable. If this returns
     *  {@code null} then it means that the interval is expressed as a fixed
     *  {@link TimeValue} and may be accessed via
     *  {@link #getIntervalAsTimeValue()}. */
    private DateTimeUnit getIntervalAsDateTimeUnit() {
        if (dateHistogramInterval != null) {
            return DATE_FIELD_UNITS.get(dateHistogramInterval.toString());
        }
        return null;
    }

    /**
     * Get the interval as a {@link TimeValue}. Should only be called if
     * {@link #getIntervalAsDateTimeUnit()} returned {@code null}.
     */
    private TimeValue getIntervalAsTimeValue() {
        if (dateHistogramInterval != null) {
            return TimeValue.parseTimeValue(dateHistogramInterval.toString(), null, getClass().getSimpleName() + ".interval");
        } else {
            return TimeValue.timeValueMillis(interval);
        }
    }

    private Rounding createRounding(DateTimeZone timeZone) {
        Rounding.Builder tzRoundingBuilder;
        DateTimeUnit intervalAsUnit = getIntervalAsDateTimeUnit();
        if (intervalAsUnit != null) {
            tzRoundingBuilder = Rounding.builder(intervalAsUnit);
        } else {
            tzRoundingBuilder = Rounding.builder(getIntervalAsTimeValue());
        }
        if (timeZone != null) {
            tzRoundingBuilder.timeZone(timeZone);
        }
        Rounding rounding = tzRoundingBuilder.build();
        return rounding;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(order, keyed, minDocCount, interval, dateHistogramInterval, minDocCount, extendedBounds);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        DateHistogramAggregationBuilder other = (DateHistogramAggregationBuilder) obj;
        return Objects.equals(order, other.order)
                && Objects.equals(keyed, other.keyed)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(interval, other.interval)
                && Objects.equals(dateHistogramInterval, other.dateHistogramInterval)
                && Objects.equals(offset, other.offset)
                && Objects.equals(extendedBounds, other.extendedBounds);
    }
}
