/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.joda.time.DateTimeZone;

import java.io.IOException;
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
public class DateHistogramGroupConfig implements Writeable, ToXContentObject {

    static final String NAME = "date_histogram";
    public static final String INTERVAL = "interval";
    private static final String FIELD = "field";
    public static final String TIME_ZONE = "time_zone";
    public static final String DELAY = "delay";
    private static final String DEFAULT_TIMEZONE = "UTC";
    private static final ConstructingObjectParser<DateHistogramGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, a ->
            new DateHistogramGroupConfig((String) a[0], (DateHistogramInterval) a[1], (DateHistogramInterval) a[2], (String) a[3]));
        PARSER.declareString(constructorArg(), new ParseField(FIELD));
        PARSER.declareField(constructorArg(), p -> new DateHistogramInterval(p.text()), new ParseField(INTERVAL), ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(),  p -> new DateHistogramInterval(p.text()), new ParseField(DELAY), ValueType.STRING);
        PARSER.declareString(optionalConstructorArg(), new ParseField(TIME_ZONE));
    }

    private final String field;
    private final DateHistogramInterval interval;
    private final DateHistogramInterval delay;
    private final String timeZone;

    /**
     * Create a new {@link DateHistogramGroupConfig} using the given field and interval parameters.
     */
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
     */
    public DateHistogramGroupConfig(final String field,
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

    DateHistogramGroupConfig(final StreamInput in) throws IOException {
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
            builder.field(INTERVAL, interval.toString());
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
    public Rounding createRounding() {
        return createRounding(interval.toString(), timeZone);
    }

    public void validateMappings(Map<String, Map<String, FieldCapabilities>> fieldCapsResponse,
                                                             ActionRequestValidationException validationException) {

        Map<String, FieldCapabilities> fieldCaps = fieldCapsResponse.get(field);
        if (fieldCaps != null && fieldCaps.isEmpty() == false) {
            if (fieldCaps.containsKey("date") && fieldCaps.size() == 1) {
                if (fieldCaps.get("date").isAggregatable()) {
                    return;
                } else {
                    validationException.addValidationError("The field [" + field + "] must be aggregatable across all indices, " +
                                    "but is not.");
                }

            } else {
                validationException.addValidationError("The field referenced by a date_histo group must be a [date] type across all " +
                        "indices in the index pattern.  Found: " + fieldCaps.keySet().toString() + " for field [" + field + "]");
            }
        }
        validationException.addValidationError("Could not find a [date] field with name [" + field + "] in any of the indices matching " +
                "the index pattern.");
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

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static DateHistogramGroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static Rounding createRounding(final String expr, final String timeZone) {
        DateTimeUnit timeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expr);
        final Rounding.Builder rounding;
        if (timeUnit != null) {
            rounding = new Rounding.Builder(timeUnit);
        } else {
            rounding = new Rounding.Builder(TimeValue.parseTimeValue(expr, "createRounding"));
        }
        rounding.timeZone(toDateTimeZone(timeZone));
        return rounding.build();
    }

    private static DateTimeZone toDateTimeZone(final String timezone) {
        try {
            return DateTimeZone.forOffsetHours(Integer.parseInt(timezone));
        } catch (NumberFormatException e) {
            return DateTimeZone.forID(timezone);
        }
    }

}
