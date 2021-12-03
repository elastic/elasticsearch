/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xcontent.ObjectParser.ValueType;

/**
 * The configuration object for the histograms in the rollup config
 *
 * {
 *     "groups": [
 *        "date_histogram": {
 *            "field" : "foo",
 *            "calendar_interval" : "1d",
 *            "time_zone" : "EST"
 *        }
 *     ]
 * }
 */
public abstract class RollupActionDateHistogramGroupConfig implements Writeable, ToXContentObject {

    static final String NAME = "date_histogram";
    public static final String FIXED_INTERVAL = "fixed_interval";
    public static final String CALENDAR_INTERVAL = "calendar_interval";
    public static final String TIME_ZONE = "time_zone";

    // this should really be ZoneOffset.UTC, but the literal UTC timezone is used because it came from Joda
    public static final String DEFAULT_TIMEZONE = ZoneId.of("UTC").getId();
    private static final String FIELD = "field";

    private static final ConstructingObjectParser<RollupActionDateHistogramGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, a -> {
            DateHistogramInterval calendarInterval = (DateHistogramInterval) a[1];
            DateHistogramInterval fixedInterval = (DateHistogramInterval) a[2];

            if (calendarInterval != null && fixedInterval == null) {
                return new CalendarInterval((String) a[0], calendarInterval, (String) a[3]);
            } else if (calendarInterval == null && fixedInterval != null) {
                return new FixedInterval((String) a[0], fixedInterval, (String) a[3]);
            } else if (calendarInterval != null && fixedInterval != null) {
                throw new IllegalArgumentException("Cannot set both [fixed_interval] and [calendar_interval] at the same time");
            } else {
                throw new IllegalArgumentException("An interval is required.  Use [fixed_interval] or [calendar_interval].");
            }
        });
        PARSER.declareString(constructorArg(), new ParseField(FIELD));
        PARSER.declareField(
            optionalConstructorArg(),
            p -> new DateHistogramInterval(p.text()),
            new ParseField(CALENDAR_INTERVAL),
            ValueType.STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            p -> new DateHistogramInterval(p.text()),
            new ParseField(FIXED_INTERVAL),
            ValueType.STRING
        );
        PARSER.declareString(optionalConstructorArg(), new ParseField(TIME_ZONE));
    }

    private final String field;
    private final DateHistogramInterval interval;
    private final String timeZone;

    /**
     * FixedInterval is a {@link RollupActionDateHistogramGroupConfig} that uses a fixed time interval for rolling up data.
     * The fixed time interval is one or multiples of SI units and has no calendar-awareness (e.g. doesn't account
     * for leap corrections, does not have variable length months, etc).
     *
     * For calendar-aware rollups, use {@link CalendarInterval}
     */
    public static class FixedInterval extends RollupActionDateHistogramGroupConfig {
        private static final String TYPE_NAME = "fixed_interval";

        public FixedInterval(String field, DateHistogramInterval interval) {
            this(field, interval, null);
        }

        public FixedInterval(String field, DateHistogramInterval interval, String timeZone) {
            super(field, interval, timeZone);
            // validate fixed time
            TimeValue.parseTimeValue(interval.toString(), NAME + ".FixedInterval");
        }

        @Override
        public String getIntervalTypeName() {
            return TYPE_NAME;
        }
    }

    /**
     * CalendarInterval is a {@link RollupActionDateHistogramGroupConfig} that uses calendar-aware intervals for rolling up data.
     * Calendar time intervals understand leap corrections and contextual differences in certain calendar units (e.g.
     * months are variable length depending on the month).  Calendar units are only available in singular quantities:
     * 1s, 1m, 1h, 1d, 1w, 1q, 1M, 1y
     *
     * For fixed time rollups, use {@link FixedInterval}
     */
    public static class CalendarInterval extends RollupActionDateHistogramGroupConfig {
        private static final String TYPE_NAME = "calendar_interval";

        public CalendarInterval(String field, DateHistogramInterval interval) {
            this(field, interval, null);
        }

        public CalendarInterval(String field, DateHistogramInterval interval, String timeZone) {
            super(field, interval, timeZone);
            if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString()) == null) {
                throw new IllegalArgumentException(
                    "The supplied interval [" + interval + "] could not be parsed " + "as a calendar interval."
                );
            }
        }

        @Override
        public String getIntervalTypeName() {
            return TYPE_NAME;
        }
    }

    /**
     * Create a new {@link RollupActionDateHistogramGroupConfig} using the given field and interval parameters.
     */
    protected RollupActionDateHistogramGroupConfig(final String field, final DateHistogramInterval interval) {
        this(field, interval, null);
    }

    /**
     * Create a new {@link RollupActionDateHistogramGroupConfig} using the given configuration parameters.
     * <p>
     *     The {@code field} and {@code interval} are required to compute the date histogram for the rolled up documents.
     *     The {@code timeZone} is optional and can be set to {@code null}. When configured, the time zone value  is resolved using
     *     ({@link ZoneId#of(String)} and must match a time zone identifier.
     * </p>
     * @param field the name of the date field to use for the date histogram (required)
     * @param interval the interval to use for the date histogram (required)
     * @param timeZone the id of time zone to use to calculate the date histogram (optional). When {@code null}, the UTC timezone is used.
     */
    protected RollupActionDateHistogramGroupConfig(
        final String field,
        final DateHistogramInterval interval,
        final @Nullable String timeZone
    ) {
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("Field must be a non-null, non-empty string");
        }
        if (interval == null) {
            throw new IllegalArgumentException("Interval must be non-null");
        }

        this.interval = interval;
        this.field = field;
        this.timeZone = (timeZone != null && timeZone.isEmpty() == false) ? timeZone : DEFAULT_TIMEZONE;

        // validate interval
        createRounding(this.interval.toString(), this.timeZone);
    }

    public static RollupActionDateHistogramGroupConfig readFrom(final StreamInput in) throws IOException {
        String type = in.readString();
        String field = in.readString();
        DateHistogramInterval interval = new DateHistogramInterval(in);
        String timeZone = in.readString();
        if (CalendarInterval.TYPE_NAME.equals(type)) {
            return new CalendarInterval(field, interval, timeZone);
        } else if (FixedInterval.TYPE_NAME.equals(type)) {
            return new FixedInterval(field, interval, timeZone);
        }
        throw new IllegalStateException("invalid type [" + type + "]");
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(getIntervalTypeName());
        out.writeString(field);
        interval.writeTo(out);
        out.writeString(timeZone);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(getIntervalTypeName(), interval.toString());
            builder.field(FIELD, field);
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

    public abstract String getIntervalTypeName();

    public void validateMappings(
        Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
        ActionRequestValidationException validationException
    ) {
        Map<String, FieldCapabilities> fieldCaps = fieldCapsResponse.get(field);
        if (fieldCaps != null && fieldCaps.isEmpty() == false) {
            boolean matchesDateType = false;
            for (String dateType : RollupField.DATE_FIELD_MAPPER_TYPES) {
                if (fieldCaps.containsKey(dateType) && fieldCaps.size() == 1) {
                    matchesDateType |= true;
                    if (fieldCaps.get(dateType).isAggregatable()) {
                        return;
                    } else {
                        validationException.addValidationError("The field [" + field + "] must be aggregatable, " + "but is not.");
                    }
                }
            }
            if (matchesDateType == false) {
                validationException.addValidationError(
                    "The field referenced by a date_histo group must be one of type ["
                        + Strings.collectionToCommaDelimitedString(RollupField.DATE_FIELD_MAPPER_TYPES)
                        + "]."
                        + " Found: "
                        + fieldCaps.keySet().toString()
                        + " for field ["
                        + field
                        + "]"
                );
            }
        } else {
            validationException.addValidationError(
                "Could not find one of ["
                    + Strings.collectionToCommaDelimitedString(RollupField.DATE_FIELD_MAPPER_TYPES)
                    + "] fields with name ["
                    + field
                    + "]."
            );
        }
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other instanceof RollupActionDateHistogramGroupConfig == false) {
            return false;
        }
        final RollupActionDateHistogramGroupConfig that = (RollupActionDateHistogramGroupConfig) other;
        return Objects.equals(interval, that.interval)
            && Objects.equals(field, that.field)
            && ZoneId.of(timeZone, ZoneId.SHORT_IDS).getRules().equals(ZoneId.of(that.timeZone, ZoneId.SHORT_IDS).getRules());
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, field, ZoneId.of(timeZone));
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static RollupActionDateHistogramGroupConfig fromXContent(final XContentParser parser) throws IOException {
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
