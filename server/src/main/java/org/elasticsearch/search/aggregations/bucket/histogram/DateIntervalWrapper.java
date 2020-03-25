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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.DateTimeUnit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Objects;

/**
 * A class that handles all the parsing, bwc and deprecations surrounding date histogram intervals.
 *
 * - Provides parser helpers for the deprecated interval/dateHistogramInterval parameters.
 * - Provides parser helpers for the new calendar/fixed interval parameters
 * - Can read old intervals from a stream and convert to new intervals
 * - Can write new intervals to old format when streaming out
 * - Provides a variety of helper methods to interpret the intervals as different types, depending on caller's need
 *
 * After the deprecated parameters are removed, this class can be simplified greatly.  The legacy options
 * will be removed, and the mutual-exclusion checks can be done in the setters directly removing the need
 * for the enum and the complicated "state machine" logic
 */
public class DateIntervalWrapper implements ToXContentFragment, Writeable {
    private static final DeprecationLogger DEPRECATION_LOGGER
        = new DeprecationLogger(LogManager.getLogger(DateHistogramAggregationBuilder.class));
    private static final String DEPRECATION_TEXT = "[interval] on [date_histogram] is deprecated, use [fixed_interval] or " +
        "[calendar_interval] in the future.";

    private static final ParseField FIXED_INTERVAL_FIELD = new ParseField("fixed_interval");
    private static final ParseField CALENDAR_INTERVAL_FIELD = new ParseField("calendar_interval");

    public enum IntervalTypeEnum implements Writeable {
        NONE, FIXED, CALENDAR, LEGACY_INTERVAL, LEGACY_DATE_HISTO;

        public static IntervalTypeEnum fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }

        public static IntervalTypeEnum fromStream(StreamInput in) throws IOException {
            return in.readEnum(IntervalTypeEnum.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private DateHistogramInterval dateHistogramInterval;
    private IntervalTypeEnum intervalType = IntervalTypeEnum.NONE;

    public static <T extends DateIntervalConsumer> void declareIntervalFields(ObjectParser<T, String> parser) {

        // NOTE: this field is deprecated and will be removed
        parser.declareField((wrapper, interval) -> {
            if (interval instanceof Long) {
                wrapper.interval((long) interval);
            } else {
                wrapper.dateHistogramInterval((DateHistogramInterval) interval);
            }
        }, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            } else {
                return new DateHistogramInterval(p.text());
            }
        }, Histogram.INTERVAL_FIELD, ObjectParser.ValueType.LONG);

        parser.declareField(DateIntervalConsumer::calendarInterval,
            p -> new DateHistogramInterval(p.text()), CALENDAR_INTERVAL_FIELD, ObjectParser.ValueType.STRING);

        parser.declareField(DateIntervalConsumer::fixedInterval,
            p -> new DateHistogramInterval(p.text()), FIXED_INTERVAL_FIELD, ObjectParser.ValueType.STRING);
    }

    public DateIntervalWrapper() {}

    public DateIntervalWrapper(StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_7_2_0)) {
            long interval = in.readLong();
            DateHistogramInterval histoInterval = in.readOptionalWriteable(DateHistogramInterval::new);

            if (histoInterval != null) {
                dateHistogramInterval = histoInterval;
                intervalType = IntervalTypeEnum.LEGACY_DATE_HISTO;
            } else {
                dateHistogramInterval = new DateHistogramInterval(interval + "ms");
                intervalType = IntervalTypeEnum.LEGACY_INTERVAL;
            }
        } else {
            dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
            intervalType = IntervalTypeEnum.fromStream(in);
        }
    }

    public IntervalTypeEnum getIntervalType() {
        return intervalType;
    }

    /** Get the current interval in milliseconds that is set on this builder. */
    @Deprecated
    public long interval() {
        DEPRECATION_LOGGER.deprecatedAndMaybeLog("date-interval-getter", DEPRECATION_TEXT);
        if (intervalType.equals(IntervalTypeEnum.LEGACY_INTERVAL)) {
            return TimeValue.parseTimeValue(dateHistogramInterval.toString(), "interval").getMillis();
        }
        return 0;
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained.
     *  If both {@link #interval()} and {@link #dateHistogramInterval()} are set, then the
     *  {@link #dateHistogramInterval()} wins.
     *
     *  @deprecated use {@link DateHistogramAggregationBuilder#fixedInterval(DateHistogramInterval)}
     *              or {@link DateHistogramAggregationBuilder#calendarInterval(DateHistogramInterval)} instead
     *  @since 7.2.0
     */
    @Deprecated
    public void interval(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException("[interval] must be 1 or greater for aggregation [date_histogram]");
        }
        setIntervalType(IntervalTypeEnum.LEGACY_INTERVAL);
        DEPRECATION_LOGGER.deprecatedAndMaybeLog("date-interval-setter", DEPRECATION_TEXT);
        this.dateHistogramInterval = new DateHistogramInterval(interval + "ms");
    }

    /** Get the current date interval that is set on this builder. */
    @Deprecated
    public DateHistogramInterval dateHistogramInterval() {
        DEPRECATION_LOGGER.deprecatedAndMaybeLog("date-histogram-interval-getter", DEPRECATION_TEXT);
        if (intervalType.equals(IntervalTypeEnum.LEGACY_DATE_HISTO)) {
            return dateHistogramInterval;
        }
        return null;
    }

    /** Set the interval on this builder, and return the builder so that calls can be chained.
     *  If both {@link #interval()} and {@link #dateHistogramInterval()} are set, then the
     *  {@link #dateHistogramInterval()} wins.
     *
     *  @deprecated use {@link DateIntervalWrapper#fixedInterval(DateHistogramInterval)}
     *              or {@link DateIntervalWrapper#calendarInterval(DateHistogramInterval)} instead
     *  @since 7.2.0
     */
    @Deprecated
    public void dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null || Strings.isNullOrEmpty(dateHistogramInterval.toString())) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null: [date_histogram]");
        }
        setIntervalType(IntervalTypeEnum.LEGACY_DATE_HISTO);
        DEPRECATION_LOGGER.deprecatedAndMaybeLog("date-histogram-interval-setter", DEPRECATION_TEXT);
        this.dateHistogramInterval = dateHistogramInterval;
    }

    /**
     * Returns the interval as a calendar interval.  Throws an exception if the value cannot be converted
     * into a calendar interval
     */
    public DateHistogramInterval getAsCalendarInterval() {
        if (intervalType.equals(IntervalTypeEnum.CALENDAR) || tryIntervalAsCalendarUnit() != null) {
            return dateHistogramInterval;
        }
        throw new IllegalStateException("Cannot convert [" + intervalType.toString() + "] interval type into calendar interval");
    }

    /**
     * Sets the interval of the DateHistogram using calendar units (`1d`, `1w`, `1M`, etc).  These units
     * are calendar-aware, meaning they respect leap additions, variable days per month, etc.
     *
     * This is mutually exclusive with {@link DateIntervalWrapper#fixedInterval(DateHistogramInterval)}
     *
     * @param interval The fixed interval to use
     */
    public void calendarInterval(DateHistogramInterval interval) {
        if (interval == null || Strings.isNullOrEmpty(interval.toString())) {
            throw new IllegalArgumentException("[interval] must not be null: [date_histogram]");
        }
        if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString()) == null) {
            throw new IllegalArgumentException("The supplied interval [" + interval +"] could not be parsed " +
                "as a calendar interval.");
        }
        setIntervalType(IntervalTypeEnum.CALENDAR);
        this.dateHistogramInterval = interval;
    }

    /**
     * Returns the interval as a Fixed interval. Throws an exception if the value cannot be converted
     * into a fixed interval
     */
    public DateHistogramInterval getAsFixedInterval() {
        if (intervalType.equals(IntervalTypeEnum.FIXED) || tryIntervalAsFixedUnit() != null) {
            return dateHistogramInterval;
        }
        throw new IllegalStateException("Cannot convert [" + intervalType.toString() + "] interval type into fixed interval");
    }

    /**
     * Sets the interval of the DateHistogram using fixed units (`1ms`, `1s`, `10m`, `4h`, etc).  These are
     * not calendar aware and are simply multiples of fixed, SI units.
     *
     * This is mutually exclusive with {@link DateIntervalWrapper#calendarInterval(DateHistogramInterval)}
     *
     * @param interval The fixed interval to use
     */
    public void fixedInterval(DateHistogramInterval interval) {
        if (interval == null || Strings.isNullOrEmpty(interval.toString())) {
            throw new IllegalArgumentException("[interval] must not be null: [date_histogram]");
        }
        setIntervalType(IntervalTypeEnum.FIXED);
        // Parse to make sure it is a valid fixed too
        TimeValue.parseTimeValue(interval.toString(), DateHistogramAggregationBuilder.NAME + ".fixedInterval");
        this.dateHistogramInterval = interval;
    }

    /** Return the interval as a date time unit if applicable, regardless of how it was configured. If this returns
     *  {@code null} then it means that the interval is expressed as a fixed
     *  {@link TimeValue} and may be accessed via {@link #tryIntervalAsFixedUnit()}. */
    DateTimeUnit tryIntervalAsCalendarUnit() {
        if (intervalType.equals(IntervalTypeEnum.CALENDAR) || intervalType.equals(IntervalTypeEnum.LEGACY_DATE_HISTO)) {
            return DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(dateHistogramInterval.toString());
        }
        return null;
    }

    /**
     * Get the interval as a {@link TimeValue}, regardless of how it was configured. Returns null if
     * the interval cannot be parsed as a fixed time.
     */
    TimeValue tryIntervalAsFixedUnit() {
        if (dateHistogramInterval == null || Strings.isNullOrEmpty(dateHistogramInterval.toString())) {
            return null;
        }
        try {
            return TimeValue.parseTimeValue(dateHistogramInterval.toString(), null, getClass().getSimpleName() + ".interval");
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public Rounding createRounding(ZoneId timeZone, long offset) {
        Rounding.Builder tzRoundingBuilder;
        if (isEmpty()) {
            throw new IllegalArgumentException("Invalid interval specified, must be non-null and non-empty");
        }
        DateIntervalWrapper.IntervalTypeEnum intervalType = getIntervalType();
        if (intervalType.equals(DateIntervalWrapper.IntervalTypeEnum.FIXED)) {
            tzRoundingBuilder = Rounding.builder(tryIntervalAsFixedUnit());
        } else if (intervalType.equals(DateIntervalWrapper.IntervalTypeEnum.CALENDAR)) {
            tzRoundingBuilder = Rounding.builder(tryIntervalAsCalendarUnit());
        } else {
            // We're not sure what the interval was originally (legacy) so use old behavior of assuming
            // calendar first, then fixed.  Required because fixed/cal overlap in places ("1h")
            DateTimeUnit calInterval = tryIntervalAsCalendarUnit();
            TimeValue fixedInterval = tryIntervalAsFixedUnit();
            if (calInterval != null) {
                tzRoundingBuilder = Rounding.builder(calInterval);
            } else if (fixedInterval != null) {
                tzRoundingBuilder = Rounding.builder(fixedInterval);
            } else {
                // If we get here we have exhausted our options and are not able to parse this interval
                throw new IllegalArgumentException("Unable to parse interval [" + dateHistogramInterval + "]");
            }
        }
        if (timeZone != null) {
            tzRoundingBuilder.timeZone(timeZone);
        }
        tzRoundingBuilder.offset(offset);
        return tzRoundingBuilder.build();
    }

    private void setIntervalType(IntervalTypeEnum type) {
        // If we're the same or have no existing type, just use the provided type
        if (intervalType.equals(IntervalTypeEnum.NONE) || type.equals(intervalType)) {
            intervalType = type;
            return;
        }

        // interval() method
        switch (type) {
            case LEGACY_INTERVAL:
                if (intervalType.equals(IntervalTypeEnum.CALENDAR) || intervalType.equals(IntervalTypeEnum.FIXED)) {
                    throw new IllegalArgumentException("Cannot use [interval] with [fixed_interval] or [calendar_interval] " +
                        "configuration options.");
                }

                // dateHistogramInterval() takes precedence over interval()
                if (intervalType.equals(IntervalTypeEnum.LEGACY_DATE_HISTO) == false) {
                    intervalType = IntervalTypeEnum.LEGACY_INTERVAL;
                }
                break;

            case LEGACY_DATE_HISTO:
                if (intervalType.equals(IntervalTypeEnum.CALENDAR) || intervalType.equals(IntervalTypeEnum.FIXED)) {
                    throw new IllegalArgumentException("Cannot use [interval] with [fixed_interval] or [calendar_interval] " +
                        "configuration options.");
                }

                // dateHistogramInterval() takes precedence over interval()
                intervalType = IntervalTypeEnum.LEGACY_DATE_HISTO;
                break;

            case FIXED:
                if (intervalType.equals(IntervalTypeEnum.LEGACY_INTERVAL) || intervalType.equals(IntervalTypeEnum.LEGACY_DATE_HISTO)) {
                    throw new IllegalArgumentException("Cannot use [fixed_interval] with [interval] " +
                        "configuration option.");
                }
                if (intervalType.equals(IntervalTypeEnum.CALENDAR)) {
                    throw new IllegalArgumentException("Cannot use [fixed_interval] with [calendar_interval] " +
                        "configuration option.");
                }
                intervalType = IntervalTypeEnum.FIXED;
                break;

            case CALENDAR:
                if (intervalType.equals(IntervalTypeEnum.LEGACY_INTERVAL) || intervalType.equals(IntervalTypeEnum.LEGACY_DATE_HISTO)) {
                    throw new IllegalArgumentException("Cannot use [calendar_interval] with [interval] " +
                        "configuration option.");
                }
                if (intervalType.equals(IntervalTypeEnum.FIXED)) {
                    throw new IllegalArgumentException("Cannot use [calendar_interval] with [fixed_interval] " +
                        "configuration option.");
                }
                intervalType = IntervalTypeEnum.CALENDAR;
                break;

            default:
                throw new IllegalStateException("Unknown interval type.");
        }
    }

    public boolean isEmpty() {
        if (intervalType.equals(IntervalTypeEnum.NONE)) {
            return true;
        }
        return dateHistogramInterval == null || Strings.isNullOrEmpty(dateHistogramInterval.toString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_7_2_0)) {
            if (intervalType.equals(IntervalTypeEnum.LEGACY_INTERVAL)) {
                out.writeLong(TimeValue.parseTimeValue(dateHistogramInterval.toString(),
                    DateHistogramAggregationBuilder.NAME + ".innerWriteTo").getMillis());
            } else {
                out.writeLong(0L);
            }
            out.writeOptionalWriteable(dateHistogramInterval);
        } else {
            out.writeOptionalWriteable(dateHistogramInterval);
            intervalType.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (intervalType.equals(IntervalTypeEnum.LEGACY_DATE_HISTO) || intervalType.equals(IntervalTypeEnum.LEGACY_INTERVAL)) {
            builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), dateHistogramInterval.toString());
        } else if (intervalType.equals(IntervalTypeEnum.FIXED)){
            builder.field(FIXED_INTERVAL_FIELD.getPreferredName(), dateHistogramInterval.toString());
        } else if (intervalType.equals(IntervalTypeEnum.CALENDAR)) {
            builder.field(CALENDAR_INTERVAL_FIELD.getPreferredName(), dateHistogramInterval.toString());
        }
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final DateIntervalWrapper that = (DateIntervalWrapper) other;
        if (tryIntervalAsCalendarUnit() != null && that.tryIntervalAsCalendarUnit() == null) {
            return false;
        }
        if (tryIntervalAsCalendarUnit() == null && that.tryIntervalAsCalendarUnit() != null) {
            return false;
        }
        return Objects.equals(this.dateHistogramInterval, that.dateHistogramInterval);
    }

    @Override
    public int hashCode() {
        boolean isCalendar = tryIntervalAsCalendarUnit() != null;
        return Objects.hash(dateHistogramInterval, isCalendar);
    }
}
