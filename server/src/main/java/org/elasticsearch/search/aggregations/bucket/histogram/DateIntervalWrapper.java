/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.DateTimeUnit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.core.RestApiVersion.equalTo;

/**
 * A class that handles all the parsing, bwc and deprecations surrounding date histogram intervals.
 *
 * - Provides parser helpers for the new calendar/fixed interval parameters
 * - Can write new intervals to old format when streaming out
 * - Provides a variety of helper methods to interpret the intervals as different types, depending on caller's need
 */
public class DateIntervalWrapper implements ToXContentFragment, Writeable {
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(DateHistogramAggregationBuilder.class);
    private static final String DEPRECATION_TEXT = "[interval] on [date_histogram] is deprecated, use [fixed_interval] or "
        + "[calendar_interval] in the future.";
    private static final ParseField FIXED_INTERVAL_FIELD = new ParseField("fixed_interval");
    private static final ParseField CALENDAR_INTERVAL_FIELD = new ParseField("calendar_interval");

    public enum IntervalTypeEnum implements Writeable {
        /*
         LEGACY_INTERVAL and LEGACY_DATE_HISTO are no longer used, but since this is a writeable enum, I'm leaving them
         to hold the ordinal places for now.
         */

        NONE("none"),
        FIXED(FIXED_INTERVAL_FIELD.getPreferredName()),
        CALENDAR(CALENDAR_INTERVAL_FIELD.getPreferredName()),
        @Deprecated
        LEGACY_INTERVAL(null),
        @Deprecated
        LEGACY_DATE_HISTO(null);

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

        public boolean isValid() {
            // I'm being a little cheeky here and just reusing the name for signaling invlaid choices too
            return this.preferredName != null;
        }

        public String getPreferredName() {
            if (preferredName == null) {
                throw new IllegalStateException("Invalid use of legacy date histogram interval");
            }
            return preferredName;
        }

        private String preferredName;

        IntervalTypeEnum(String preferredName) {
            this.preferredName = preferredName;
        }
    }

    private DateHistogramInterval dateHistogramInterval;
    private IntervalTypeEnum intervalType = IntervalTypeEnum.NONE;

    public static <T extends DateIntervalConsumer<T>> void declareIntervalFields(ObjectParser<T, String> parser) {
        /*
         REST version compatibility.  When in V_7 compatibility mode, continue to parse the old style interval parameter,
         but immediately adapt it into either fixed or calendar interval.
         */
        parser.declareField((wrapper, interval) -> {
            DEPRECATION_LOGGER.warn(DeprecationCategory.AGGREGATIONS, "date-interval-getter", DEPRECATION_TEXT);
            if (interval instanceof Long) {
                wrapper.fixedInterval(new DateHistogramInterval(interval + "ms"));
            } else {
                if (interval != null && DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(interval.toString())) {
                    wrapper.calendarInterval((DateHistogramInterval) interval);
                } else {
                    wrapper.fixedInterval((DateHistogramInterval) interval);
                }
            }
        }, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            } else {
                return new DateHistogramInterval(p.text());
            }
        }, Histogram.INTERVAL_FIELD.forRestApiVersion(equalTo(RestApiVersion.V_7)), ObjectParser.ValueType.LONG);

        parser.declareField(
            DateIntervalConsumer::calendarInterval,
            p -> new DateHistogramInterval(p.text()),
            CALENDAR_INTERVAL_FIELD,
            ObjectParser.ValueType.STRING
        );

        parser.declareField(
            DateIntervalConsumer::fixedInterval,
            p -> new DateHistogramInterval(p.text()),
            FIXED_INTERVAL_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    public DateIntervalWrapper() {}

    public DateIntervalWrapper(StreamInput in) throws IOException {
        dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        intervalType = IntervalTypeEnum.fromStream(in);
    }

    public IntervalTypeEnum getIntervalType() {
        return intervalType;
    }

    /**
     * Returns the interval as a calendar interval.  Throws an exception if the value cannot be converted
     * into a calendar interval
     */
    public DateHistogramInterval getAsCalendarInterval() {
        if (intervalType.equals(IntervalTypeEnum.CALENDAR)) {
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
            throw new IllegalArgumentException("The supplied interval [" + interval + "] could not be parsed " + "as a calendar interval.");
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
        if (intervalType.equals(IntervalTypeEnum.CALENDAR)) {
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
            // If we get here we have exhausted our options and are not able to parse this interval
            throw new IllegalArgumentException("Unable to parse interval [" + dateHistogramInterval + "]");
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
        if (type.isValid() == false || intervalType.isValid() == false) {
            throw new IllegalArgumentException("Unknown interval type.");
        }
        throw new IllegalArgumentException(
            "Cannot use [" + type.getPreferredName() + "] with [" + intervalType.getPreferredName() + "] configuration option."
        );
    }

    public boolean isEmpty() {
        if (intervalType.equals(IntervalTypeEnum.NONE)) {
            return true;
        }
        return dateHistogramInterval == null || Strings.isNullOrEmpty(dateHistogramInterval.toString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(dateHistogramInterval);
        intervalType.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (intervalType.equals(IntervalTypeEnum.FIXED)) {
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
