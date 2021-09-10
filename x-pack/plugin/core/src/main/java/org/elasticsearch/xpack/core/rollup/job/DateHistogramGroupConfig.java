/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;

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
public abstract class DateHistogramGroupConfig implements Writeable, ToXContentObject {

    static final String NAME = "date_histogram";
    public static final String INTERVAL = "interval";
    public static final String FIXED_INTERVAL = "fixed_interval";
    public static final String CALENDAR_INTERVAL = "calendar_interval";
    public static final String TIME_ZONE = "time_zone";
    public static final String DELAY = "delay";

    private static final String DEFAULT_TIMEZONE = "UTC";
    public static final ZoneId DEFAULT_ZONEID_TIMEZONE = ZoneOffset.UTC;
    private static final String FIELD = "field";
    private static final String TYPE_NAME = "interval";

    private static final ConstructingObjectParser<DateHistogramGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, a -> {
            DateHistogramInterval oldInterval = (DateHistogramInterval) a[1];
            DateHistogramInterval calendarInterval = (DateHistogramInterval) a[2];
            DateHistogramInterval fixedInterval = (DateHistogramInterval) a[3];

            if (oldInterval != null) {
                if  (calendarInterval != null || fixedInterval != null) {
                    throw new IllegalArgumentException("Cannot use [interval] with [fixed_interval] or [calendar_interval] " +
                        "configuration options.");
                }
                return fromUnknownTimeUnit((String) a[0], oldInterval, (DateHistogramInterval) a[4], (String) a[5]);
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
        private static final String TYPE_NAME = "fixed_interval";
        public FixedInterval(String field, DateHistogramInterval interval) {
            this(field, interval, null, null);
        }

        public FixedInterval(String field, DateHistogramInterval interval, DateHistogramInterval delay, String timeZone) {
            super(field, interval, delay, timeZone);
            // validate fixed time
            TimeValue.parseTimeValue(interval.toString(), NAME + ".FixedInterval");
        }

        FixedInterval(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getIntervalTypeName() {
            return TYPE_NAME;
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
        private static final String TYPE_NAME = "calendar_interval";
        public CalendarInterval(String field, DateHistogramInterval interval) {
            this(field, interval, null, null);
        }

        public CalendarInterval(String field, DateHistogramInterval interval, DateHistogramInterval delay, String timeZone) {
            super(field, interval, delay, timeZone);
            if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString()) == null) {
                throw new IllegalArgumentException("The supplied interval [" + interval +"] could not be parsed " +
                    "as a calendar interval.");
            }
        }

        CalendarInterval(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getIntervalTypeName() {
            return TYPE_NAME;
        }
    }

    /**
     * This helper can be used to "up-convert" a legacy job date histo config stored with plain "interval" into
     * one of the new Fixed or Calendar intervals.  It follows the old behavior where the interval is first
     * parsed with the calendar logic, and if that fails, it is assumed to be a fixed interval
     */
    private static DateHistogramGroupConfig fromUnknownTimeUnit(String field, DateHistogramInterval interval,
                                                                DateHistogramInterval delay, String timeZone) {
        if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString()) != null) {
            return new CalendarInterval(field, interval, delay, timeZone);
        } else {
            return new FixedInterval(field, interval, delay, timeZone);
        }
    }

    static DateHistogramGroupConfig fromUnknownTimeUnit(StreamInput in) throws IOException {
        DateHistogramInterval interval = new DateHistogramInterval(in);
        String field = in.readString();
        DateHistogramInterval delay = in.readOptionalWriteable(DateHistogramInterval::new);
        String timeZone = in.readString();
        return fromUnknownTimeUnit(field, interval, delay, timeZone);
    }

    /**
     * Create a new {@link DateHistogramGroupConfig} using the given configuration parameters.
     * <p>
     *     The {@code field} and {@code interval} are required to compute the date histogram for the rolled up documents.
     *     The {@code delay} is optional and can be set to {@code null}. It defines how long to wait before rolling up new documents.
     *     The {@code timeZone} is optional and can be set to {@code null}. When configured, the time zone value  is resolved using
     *     ({@link ZoneId#of(String)} and must match a time zone identifier.
     * </p>
     * @param field the name of the date field to use for the date histogram (required)
     * @param interval the interval to use for the date histogram (required)
     * @param delay the time delay (optional)
     * @param timeZone the id of time zone to use to calculate the date histogram (optional). When {@code null}, the UTC timezone is used.
     *
     * @since 7.2.0
     */
    protected DateHistogramGroupConfig(final String field,
                                    final DateHistogramInterval interval,
                                    final @Nullable DateHistogramInterval delay,
                                    final @Nullable String timeZone) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("Field must be a non-null, non-empty string");
        }
        if (interval == null) {
            throw new IllegalArgumentException("Interval must be non-null");
        }

        this.interval = interval;
        this.field = field;
        this.delay = delay;
        this.timeZone = (timeZone != null && timeZone.isEmpty() == false) ? timeZone : DEFAULT_TIMEZONE;

        // validate interval
        createRounding(this.interval.toString(), this.timeZone);
        if (delay != null) {
            // and delay
            TimeValue.parseTimeValue(this.delay.toString(), DELAY);
        }
    }

    protected DateHistogramGroupConfig(final StreamInput in) throws IOException {
        interval = new DateHistogramInterval(in);
        field = in.readString();
        delay = in.readOptionalWriteable(DateHistogramInterval::new);
        timeZone = in.readString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        interval.writeTo(out);
        out.writeString(field);
        out.writeOptionalWriteable(delay);
        out.writeString(timeZone);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(getIntervalTypeName(), interval.toString());
            builder.field(FIELD, field);
            if (delay != null) {
                builder.field(DELAY, delay.toString());
            }
            builder.field(TIME_ZONE, timeZone);
        }
        return builder.endObject();
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

    /**
     * Create the rounding for this date histogram
     */
    public Rounding.Prepared createRounding() {
        return createRounding(interval.toString(), timeZone);
    }

    public String getIntervalTypeName() {
        return TYPE_NAME;
    }

    public void validateMappings(Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                                             ActionRequestValidationException validationException) {
        Map<String, FieldCapabilities> fieldCaps = fieldCapsResponse.get(field);
        if (fieldCaps != null && fieldCaps.isEmpty() == false) {
            boolean matchesDateType = false;
            for (String dateType : RollupField.DATE_FIELD_MAPPER_TYPES) {
                if (fieldCaps.containsKey(dateType) && fieldCaps.size() == 1) {
                    matchesDateType |= true;
                    if (fieldCaps.get(dateType).isAggregatable()) {
                        return;
                    } else {
                        validationException.addValidationError("The field [" + field + "] must be aggregatable across all indices, " +
                            "but is not.");
                    }
                }
            }
            if (matchesDateType == false) {
                validationException.addValidationError("The field referenced by a date_histo group must be one of type [" +
                    Strings.collectionToCommaDelimitedString(RollupField.DATE_FIELD_MAPPER_TYPES) + "] across all " +
                    "indices in the index pattern.  Found: " + fieldCaps.keySet().toString() + " for field [" + field + "]");
            }
        } else {
            validationException.addValidationError("Could not find one of [" +
                Strings.collectionToCommaDelimitedString(RollupField.DATE_FIELD_MAPPER_TYPES) + "] fields with name [" +
                field + "] in any of the indices matching " +
                "the index pattern.");
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other instanceof DateHistogramGroupConfig == false) {
            return false;
        }
        final DateHistogramGroupConfig that = (DateHistogramGroupConfig) other;
        return Objects.equals(interval, that.interval)
            && Objects.equals(field, that.field)
            && Objects.equals(delay, that.delay)
            && ZoneId.of(timeZone, ZoneId.SHORT_IDS).getRules().equals(ZoneId.of(that.timeZone, ZoneId.SHORT_IDS).getRules());
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, field, delay, ZoneId.of(timeZone));
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static DateHistogramGroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static Rounding.Prepared createRounding(final String expr, final String timeZone) {
        Rounding.DateTimeUnit timeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expr);
        final Rounding.Builder rounding;
        if (timeUnit != null) {
            rounding = new Rounding.Builder(timeUnit);
        } else {
            rounding = new Rounding.Builder(TimeValue.parseTimeValue(expr, "createRounding"));
        }
        rounding.timeZone(ZoneId.of(timeZone, ZoneId.SHORT_IDS));
        return rounding.build().prepareForUnknown();
    }
}
