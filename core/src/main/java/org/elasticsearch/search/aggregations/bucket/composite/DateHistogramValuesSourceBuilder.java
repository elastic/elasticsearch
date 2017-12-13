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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.search.SortField;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder.DATE_FIELD_UNITS;

/**
 * A {@link CompositeValuesSourceBuilder} that that builds a {@link RoundingValuesSource} from a {@link Script} or
 * a field name.
 */
public class DateHistogramValuesSourceBuilder extends CompositeValuesSourceBuilder<DateHistogramValuesSourceBuilder> {
    static final String TYPE = "date_histogram";

    private static final ObjectParser<DateHistogramValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(DateHistogramValuesSourceBuilder.TYPE);
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
        PARSER.declareField(DateHistogramValuesSourceBuilder::timeZone, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return DateTimeZone.forID(p.text());
            } else {
                return DateTimeZone.forOffsetHours(p.intValue());
            }
        }, new ParseField("time_zone"), ObjectParser.ValueType.LONG);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER, ValueType.NUMERIC);
    }
    static DateHistogramValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new DateHistogramValuesSourceBuilder(name), null);
    }

    private long interval = 0;
    private DateTimeZone timeZone = null;
    private DateHistogramInterval dateHistogramInterval;

    public DateHistogramValuesSourceBuilder(String name) {
        super(name, ValueType.DATE);
    }

    protected DateHistogramValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        this.interval = in.readLong();
        this.dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        if (in.readBoolean()) {
            timeZone = DateTimeZone.forID(in.readString());
        }
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeLong(interval);
        out.writeOptionalWriteable(dateHistogramInterval);
        boolean hasTimeZone = timeZone != null;
        out.writeBoolean(hasTimeZone);
        if (hasTimeZone) {
            out.writeString(timeZone.getID());
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (dateHistogramInterval == null) {
            builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
        } else {
            builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), dateHistogramInterval.toString());
        }
        if (timeZone != null) {
            builder.field("time_zone", timeZone);
        }
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(interval, dateHistogramInterval, timeZone);
    }

    @Override
    protected boolean innerEquals(DateHistogramValuesSourceBuilder other) {
        return Objects.equals(interval, other.interval)
            && Objects.equals(dateHistogramInterval, other.dateHistogramInterval)
            && Objects.equals(timeZone, other.timeZone);
    }

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * Returns the interval in milliseconds that is set on this source
     **/
    public long interval() {
        return interval;
    }

    /**
     * Sets the interval on this source.
     * If both {@link #interval()} and {@link #dateHistogramInterval()} are set,
     * then the {@link #dateHistogramInterval()} wins.
     **/
    public DateHistogramValuesSourceBuilder interval(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException("[interval] must be 1 or greater for [date_histogram] source");
        }
        this.interval = interval;
        return this;
    }

    /**
     * Returns the date interval that is set on this source
     **/
    public DateHistogramInterval dateHistogramInterval() {
        return dateHistogramInterval;
    }

    public DateHistogramValuesSourceBuilder dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null");
        }
        this.dateHistogramInterval = dateHistogramInterval;
        return this;
    }

    /**
     * Sets the time zone to use for this aggregation
     */
    public DateHistogramValuesSourceBuilder timeZone(DateTimeZone timeZone) {
        if (timeZone == null) {
            throw new IllegalArgumentException("[timeZone] must not be null: [" + name + "]");
        }
        this.timeZone = timeZone;
        return this;
    }

    /**
     * Gets the time zone to use for this aggregation
     */
    public DateTimeZone timeZone() {
        return timeZone;
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
    protected CompositeValuesSourceConfig innerBuild(SearchContext context,
                                                     ValuesSourceConfig<?> config,
                                                     int pos,
                                                     int numPos,
                                                     SortField sortField) throws IOException {
        Rounding rounding = createRounding();
        ValuesSource orig = config.toValuesSource(context.getQueryShardContext());
        if (orig == null) {
            orig = ValuesSource.Numeric.EMPTY;
        }
        if (orig instanceof ValuesSource.Numeric) {
            ValuesSource.Numeric numeric = (ValuesSource.Numeric) orig;
            RoundingValuesSource vs = new RoundingValuesSource(numeric, rounding);
            boolean canEarlyTerminate = false;
            final FieldContext fieldContext = config.fieldContext();
            if (sortField != null &&
                    pos == numPos-1 &&
                    fieldContext != null)  {
                canEarlyTerminate = checkCanEarlyTerminate(context.searcher().getIndexReader(),
                    fieldContext.field(), order() == SortOrder.ASC ? false : true, sortField);
            }
            return new CompositeValuesSourceConfig(name, vs, order(), canEarlyTerminate);
        } else {
            throw new IllegalArgumentException("invalid source, expected numeric, got " + orig.getClass().getSimpleName());
        }
    }
}
