/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.DateHistogramValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
public class DateHistoGroupConfig implements Writeable, ToXContentFragment {
    private static final String NAME = "date_histo_group_config";
    public static final ObjectParser<DateHistoGroupConfig.Builder, Void> PARSER
            = new ObjectParser<>(NAME, DateHistoGroupConfig.Builder::new);

    private static final ParseField INTERVAL = new ParseField("interval");
    private static final ParseField DELAY = new ParseField("delay");
    private static final ParseField FIELD = new ParseField("field");
    public static final ParseField TIME_ZONE = new ParseField("time_zone");

    private final DateHistogramInterval interval;
    private final String field;
    private final DateTimeZone timeZone;
    private final DateHistogramInterval delay;

    static {
        PARSER.declareField(DateHistoGroupConfig.Builder::setInterval,
                p -> new DateHistogramInterval(p.text()), INTERVAL, ObjectParser.ValueType.STRING);
        PARSER.declareString(DateHistoGroupConfig.Builder::setField, FIELD);
        PARSER.declareField(DateHistoGroupConfig.Builder::setDelay,
                p -> new DateHistogramInterval(p.text()), DELAY, ObjectParser.ValueType.LONG);
        PARSER.declareField(DateHistoGroupConfig.Builder::setTimeZone, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return DateTimeZone.forID(p.text());
            } else {
                return DateTimeZone.forOffsetHours(p.intValue());
            }
        }, TIME_ZONE, ObjectParser.ValueType.LONG);
    }

    private DateHistoGroupConfig(DateHistogramInterval interval,
                                 String field,
                                 DateHistogramInterval delay,
                                 DateTimeZone timeZone) {
        this.interval = interval;
        this.field = field;
        this.delay = delay;
        this.timeZone = Objects.requireNonNull(timeZone);
    }

    DateHistoGroupConfig(StreamInput in) throws IOException {
        interval = new DateHistogramInterval(in);
        field = in.readString();
        delay = in.readOptionalWriteable(DateHistogramInterval::new);
        timeZone = in.readTimeZone();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        interval.writeTo(out);
        out.writeString(field);
        out.writeOptionalWriteable(delay);
        out.writeTimeZone(timeZone);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INTERVAL.getPreferredName(), interval.toString());
        builder.field(FIELD.getPreferredName(), field);
        if (delay != null) {
            builder.field(DELAY.getPreferredName(), delay.toString());
        }
        builder.field(TIME_ZONE.getPreferredName(), timeZone.toString());

        return builder;
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
    public DateTimeZone getTimeZone() {
        return timeZone;
    }

    /**
     * Create the rounding for this date histogram
     */
    public Rounding createRounding() {
        return createRounding(interval.toString(), timeZone, "");
    }
    ;
    /**
     * This returns a set of aggregation builders which represent the configured
     * set of date histograms.  Used by the rollup indexer to iterate over historical data
     */
    public List<CompositeValuesSourceBuilder<?>> toBuilders() {
        DateHistogramValuesSourceBuilder vsBuilder =
                new DateHistogramValuesSourceBuilder(RollupField.formatIndexerAggName(field, DateHistogramAggregationBuilder.NAME));
        vsBuilder.dateHistogramInterval(interval);
        vsBuilder.field(field);
        vsBuilder.timeZone(timeZone);
        return Collections.singletonList(vsBuilder);
    }

    /**
     * @return A map representing this config object as a RollupCaps aggregation object
     */
    public Map<String, Object> toAggCap() {
        Map<String, Object> map = new HashMap<>(3);
        map.put("agg", DateHistogramAggregationBuilder.NAME);
        map.put(INTERVAL.getPreferredName(), interval.toString());
        if (delay != null) {
            map.put(DELAY.getPreferredName(), delay.toString());
        }
        map.put(TIME_ZONE.getPreferredName(), timeZone.toString());

        return map;
    }

    public Map<String, Object> getMetadata() {
        return Collections.singletonMap(RollupField.formatMetaField(RollupField.INTERVAL), interval.toString());
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
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DateHistoGroupConfig that = (DateHistoGroupConfig) other;

        return Objects.equals(this.interval, that.interval)
                && Objects.equals(this.field, that.field)
                && Objects.equals(this.delay, that.delay)
                && Objects.equals(this.timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, field, delay, timeZone);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    private static Rounding createRounding(String expr, DateTimeZone timeZone, String settingName) {
        DateTimeUnit timeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expr);
        final Rounding.Builder rounding;
        if (timeUnit != null) {
            rounding = new Rounding.Builder(timeUnit);
        } else {
            rounding = new Rounding.Builder(TimeValue.parseTimeValue(expr, settingName));
        }
        rounding.timeZone(timeZone);
        return rounding.build();
    }

    public static class Builder {
        private DateHistogramInterval interval;
        private String field;
        private DateHistogramInterval delay;
        private DateTimeZone timeZone;

        public DateHistogramInterval getInterval() {
            return interval;
        }

        public DateHistoGroupConfig.Builder setInterval(DateHistogramInterval interval) {
            this.interval = interval;
            return this;
        }

        public String getField() {
            return field;
        }

        public DateHistoGroupConfig.Builder setField(String field) {
            this.field = field;
            return this;
        }

        public DateTimeZone getTimeZone() {
            return timeZone;
        }

        public DateHistoGroupConfig.Builder setTimeZone(DateTimeZone timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public DateHistogramInterval getDelay() {
            return delay;
        }

        public DateHistoGroupConfig.Builder setDelay(DateHistogramInterval delay) {
            this.delay = delay;
            return this;
        }

        public DateHistoGroupConfig build() {
            if (field == null || field.isEmpty()) {
                throw new IllegalArgumentException("Parameter [" + FIELD.getPreferredName() + "] is mandatory.");
            }
            if (timeZone == null) {
                timeZone = DateTimeZone.UTC;
            }
            if (interval == null) {
                throw new IllegalArgumentException("Parameter [" + INTERVAL.getPreferredName() + "] is mandatory.");
            }
            // validate interval
            createRounding(interval.toString(), timeZone, INTERVAL.getPreferredName());
            if (delay != null) {
                // and delay
                TimeValue.parseTimeValue(delay.toString(), INTERVAL.getPreferredName());
            }
            return new DateHistoGroupConfig(interval, field, delay, timeZone);
        }
    }
}
