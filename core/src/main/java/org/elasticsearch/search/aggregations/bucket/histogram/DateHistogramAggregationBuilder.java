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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * A builder for histograms on date fields.
 */
public class DateHistogramAggregationBuilder
        extends ValuesSourceAggregationBuilder<ValuesSource.Numeric, DateHistogramAggregationBuilder> {
    public static final String NAME = "date_histogram";

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

    private static final ObjectParser<DateHistogramAggregationBuilder, QueryParseContext> PARSER;
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

        PARSER.declareField(DateHistogramAggregationBuilder::order, DateHistogramAggregationBuilder::parseOrder,
                Histogram.ORDER_FIELD, ObjectParser.ValueType.OBJECT);
    }

    public static DateHistogramAggregationBuilder parse(String aggregationName, QueryParseContext context) throws IOException {
        return PARSER.parse(context.parser(), new DateHistogramAggregationBuilder(aggregationName), context);
    }

    private long interval;
    private DateHistogramInterval dateHistogramInterval;
    private long offset = 0;
    private ExtendedBounds extendedBounds;
    private InternalOrder order = (InternalOrder) Histogram.Order.KEY_ASC;
    private boolean keyed = false;
    private long minDocCount = 0;

    /** Create a new builder with the given name. */
    public DateHistogramAggregationBuilder(String name) {
        super(name, ValuesSourceType.NUMERIC, ValueType.DATE);
    }

    /** Read from a stream, for internal use only. */
    public DateHistogramAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.NUMERIC, ValueType.DATE);
        if (in.readBoolean()) {
            order = InternalOrder.Streams.readOrder(in);
        }
        keyed = in.readBoolean();
        minDocCount = in.readVLong();
        interval = in.readLong();
        dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        offset = in.readLong();
        extendedBounds = in.readOptionalWriteable(ExtendedBounds::new);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        boolean hasOrder = order != null;
        out.writeBoolean(hasOrder);
        if (hasOrder) {
            InternalOrder.Streams.writeOrder(order, out);
        }
        out.writeBoolean(keyed);
        out.writeVLong(minDocCount);
        out.writeLong(interval);
        out.writeOptionalWriteable(dateHistogramInterval);
        out.writeLong(offset);
        out.writeOptionalWriteable(extendedBounds);
    }

    /** Get the current interval in milliseconds that is set on this builder. */
    public double interval() {
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
    public double offset() {
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
    public Histogram.Order order() {
        return order;
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained. */
    public DateHistogramAggregationBuilder order(Histogram.Order order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        this.order = (InternalOrder) order;
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

    @Override
    protected ValuesSourceAggregatorFactory<Numeric, ?> innerBuild(SearchContext context, ValuesSourceConfig<Numeric> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        Rounding rounding = createRounding();
        ExtendedBounds roundedBounds = null;
        if (this.extendedBounds != null) {
            // parse any string bounds to longs and round
            roundedBounds = this.extendedBounds.parseAndValidate(name, context, config.format()).round(rounding);
        }
        return new DateHistogramAggregatorFactory(name, config, interval, dateHistogramInterval, offset, order, keyed, minDocCount,
                rounding, roundedBounds, context, parent, subFactoriesBuilder, metaData);
    }

    private Rounding createRounding() {
        Rounding.Builder tzRoundingBuilder;
        if (dateHistogramInterval != null) {
            DateTimeUnit dateTimeUnit = DATE_FIELD_UNITS.get(dateHistogramInterval.toString());
            if (dateTimeUnit != null) {
                tzRoundingBuilder = Rounding.builder(dateTimeUnit);
            } else {
                // the interval is a time value?
                tzRoundingBuilder = Rounding.builder(
                        TimeValue.parseTimeValue(dateHistogramInterval.toString(), null, getClass().getSimpleName() + ".interval"));
            }
        } else {
            // the interval is an integer time value in millis?
            tzRoundingBuilder = Rounding.builder(TimeValue.timeValueMillis(interval));
        }
        if (timeZone() != null) {
            tzRoundingBuilder.timeZone(timeZone());
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

    // similar to the parsing oh histogram orders, but also accepts _time as an alias for _key
    private static InternalOrder parseOrder(XContentParser parser, QueryParseContext context) throws IOException {
        InternalOrder order = null;
        Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                String dir = parser.text();
                boolean asc = "asc".equals(dir);
                if (!asc && !"desc".equals(dir)) {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown order direction: [" + dir
                            + "]. Should be either [asc] or [desc]");
                }
                order = resolveOrder(currentFieldName, asc);
            }
        }
        return order;
    }

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key) || "_time".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        return new InternalOrder.Aggregation(key, asc);
    }
}
