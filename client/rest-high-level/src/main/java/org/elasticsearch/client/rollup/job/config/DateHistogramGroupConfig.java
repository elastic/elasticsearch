/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.ObjectParser.ValueType;

/**
 * The configuration object for the histograms in the rollup config
 *
 * {
 *     "groups": [
 *        "date_histogram": {
 *            "field" : "foo",
 *            "interval" : "1d",
 *            "delay": "30d",
 *            "time_zone" : "EST"
 *        }
 *     ]
 * }
 */
public class DateHistogramGroupConfig implements Validatable, ToXContentObject {

    static final String NAME = "date_histogram";
    private static final String INTERVAL = "interval";
    private static final String FIELD = "field";
    private static final String TIME_ZONE = "time_zone";
    private static final String DELAY = "delay";
    private static final String DEFAULT_TIMEZONE = "UTC";
    private static final String CALENDAR_INTERVAL = "calendar_interval";
    private static final String FIXED_INTERVAL = "fixed_interval";

    // From DateHistogramAggregationBuilder in core, transplanted and modified to a set
    // so we don't need to import a dependency on the class
    private static final Set<String> DATE_FIELD_UNITS;
    static {
        Set<String> dateFieldUnits = new HashSet<>();
        dateFieldUnits.add("year");
        dateFieldUnits.add("1y");
        dateFieldUnits.add("quarter");
        dateFieldUnits.add("1q");
        dateFieldUnits.add("month");
        dateFieldUnits.add("1M");
        dateFieldUnits.add("week");
        dateFieldUnits.add("1w");
        dateFieldUnits.add("day");
        dateFieldUnits.add("1d");
        dateFieldUnits.add("hour");
        dateFieldUnits.add("1h");
        dateFieldUnits.add("minute");
        dateFieldUnits.add("1m");
        dateFieldUnits.add("second");
        dateFieldUnits.add("1s");
        DATE_FIELD_UNITS = Collections.unmodifiableSet(dateFieldUnits);
    }

    private static final ConstructingObjectParser<DateHistogramGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, true, a -> {
            DateHistogramInterval oldInterval = (DateHistogramInterval) a[1];
            DateHistogramInterval calendarInterval = (DateHistogramInterval) a[2];
            DateHistogramInterval fixedInterval = (DateHistogramInterval) a[3];

            if (oldInterval != null) {
                if  (calendarInterval != null || fixedInterval != null) {
                    throw new IllegalArgumentException("Cannot use [interval] with [fixed_interval] or [calendar_interval] " +
                        "configuration options.");
                }
                return new DateHistogramGroupConfig((String) a[0], oldInterval, (DateHistogramInterval) a[4], (String) a[5]);
            } else if (calendarInterval != null && fixedInterval == null) {
                return new CalendarInterval((String) a[0], calendarInterval, (DateHistogramInterval) a[4], (String) a[5]);
            } else if (calendarInterval == null && fixedInterval != null) {
                return new FixedInterval((String) a[0], fixedInterval, (DateHistogramInterval) a[4], (String) a[5]);
            } else if (calendarInterval != null && fixedInterval != null) {
                throw new IllegalArgumentException("Cannot set both [fixed_interval] and [calendar_interval] at the same time");
            } else {
                throw new IllegalArgumentException("An interval is required.  Use [fixed_interval] or [calendar_interval].");
            }
        });
        PARSER.declareString(constructorArg(), new ParseField(FIELD));
        PARSER.declareField(optionalConstructorArg(), p -> new DateHistogramInterval(p.text()), new ParseField(INTERVAL), ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), p -> new DateHistogramInterval(p.text()),
            new ParseField(CALENDAR_INTERVAL), ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), p -> new DateHistogramInterval(p.text()),
            new ParseField(FIXED_INTERVAL), ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(),  p -> new DateHistogramInterval(p.text()), new ParseField(DELAY), ValueType.STRING);
        PARSER.declareString(optionalConstructorArg(), new ParseField(TIME_ZONE));
    }

    private final String field;
    private final DateHistogramInterval interval;
    private final DateHistogramInterval delay;
    private final String timeZone;

    /**
     * FixedInterval is a {@link DateHistogramGroupConfig} that uses a fixed time interval for rolling up data.
     * The fixed time interval is one or multiples of SI units and has no calendar-awareness (e.g. doesn't account
     * for leap corrections, does not have variable length months, etc).
     *
     * For calendar-aware rollups, use {@link CalendarInterval}
     */
    public static class FixedInterval extends DateHistogramGroupConfig {
        public FixedInterval(String field, DateHistogramInterval interval) {
            this(field, interval, null, null);
        }

        public FixedInterval(String field, DateHistogramInterval interval, DateHistogramInterval delay, String timeZone) {
            super(field, interval, delay, timeZone);
            // validate fixed time
            TimeValue.parseTimeValue(interval.toString(), NAME + ".FixedInterval");
        }
    }

    /**
     * CalendarInterval is a {@link DateHistogramGroupConfig} that uses calendar-aware intervals for rolling up data.
     * Calendar time intervals understand leap corrections and contextual differences in certain calendar units (e.g.
     * months are variable length depending on the month).  Calendar units are only available in singular quantities:
     * 1s, 1m, 1h, 1d, 1w, 1q, 1M, 1y
     *
     * For fixed time rollups, use {@link FixedInterval}
     */
    public static class CalendarInterval extends DateHistogramGroupConfig {
        public CalendarInterval(String field, DateHistogramInterval interval) {
            this(field, interval, null, null);

        }

        public CalendarInterval(String field, DateHistogramInterval interval, DateHistogramInterval delay, String timeZone) {
            super(field, interval, delay, timeZone);
            if (DATE_FIELD_UNITS.contains(interval.toString()) == false) {
                throw new IllegalArgumentException("The supplied interval [" + interval +"] could not be parsed " +
                    "as a calendar interval.");
            }
        }

    }

    /**
     * Create a new {@link DateHistogramGroupConfig} using the given field and interval parameters.
     *
     * @deprecated Build a DateHistoConfig using {@link DateHistogramGroupConfig.CalendarInterval}
     * or {@link DateHistogramGroupConfig.FixedInterval} instead
     *
     * @since 7.2.0
     */
    @Deprecated
    public DateHistogramGroupConfig(final String field, final DateHistogramInterval interval) {
        this(field, interval, null, null);
    }

    /**
     * Create a new {@link DateHistogramGroupConfig} using the given configuration parameters.
     * <p>
     *     The {@code field} and {@code interval} are required to compute the date histogram for the rolled up documents.
     *     The {@code delay} is optional and can be set to {@code null}. It defines how long to wait before rolling up new documents.
     *     The {@code timeZone} is optional and can be set to {@code null}. When configured, the time zone value  is resolved using
     *     ({@link DateTimeZone#forID(String)} and must match a time zone identifier provided by the Joda Time library.
     * </p>
     * @param field the name of the date field to use for the date histogram (required)
     * @param interval the interval to use for the date histogram (required)
     * @param delay the time delay (optional)
     * @param timeZone the id of time zone to use to calculate the date histogram (optional). When {@code null}, the UTC timezone is used.
     *
     * @deprecated Build a DateHistoConfig using {@link DateHistogramGroupConfig.CalendarInterval}
     * or {@link DateHistogramGroupConfig.FixedInterval} instead
     *
     * @since 7.2.0
     */
    @Deprecated
    public DateHistogramGroupConfig(final String field,
                                    final DateHistogramInterval interval,
                                    final @Nullable DateHistogramInterval delay,
                                    final @Nullable String timeZone) {
        this.field = field;
        this.interval = interval;
        this.delay = delay;
        this.timeZone = (timeZone != null && timeZone.isEmpty() == false) ? timeZone : DEFAULT_TIMEZONE;
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (field == null || field.isEmpty()) {
            validationException.addValidationError("Field name is required");
        }
        if (interval == null) {
            validationException.addValidationError("Interval is required");
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(validationException);
    }

    /**
     * Get the date field
     */
    public String getField() {
        return field;
    }

    /**
     * Get the date interval
     */
    public DateHistogramInterval getInterval() {
        return interval;
    }

    /**
     * Get the time delay for this histogram
     */
    public DateHistogramInterval getDelay() {
        return delay;
    }

    /**
     * Get the timezone to apply
     */
    public String getTimeZone() {
        return timeZone;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            if (this.getClass().equals(CalendarInterval.class)) {
                builder.field(CALENDAR_INTERVAL, interval.toString());
            } else if (this.getClass().equals(FixedInterval.class)) {
                builder.field(FIXED_INTERVAL, interval.toString());
            } else {
                builder.field(INTERVAL, interval.toString());
            }
            builder.field(FIELD, field);
            if (delay != null) {
                builder.field(DELAY, delay.toString());
            }
            builder.field(TIME_ZONE, timeZone);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final DateHistogramGroupConfig that = (DateHistogramGroupConfig) other;
        return Objects.equals(interval, that.interval)
            && Objects.equals(field, that.field)
            && Objects.equals(delay, that.delay)
            && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, field, delay, timeZone);
    }

    public static DateHistogramGroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
