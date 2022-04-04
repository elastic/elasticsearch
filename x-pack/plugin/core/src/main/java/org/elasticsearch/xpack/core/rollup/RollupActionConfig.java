/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a {@link RollupAction} job, such as the groupings, metrics, what
 * index to rollup and where to roll them to.
 *
 *      * FixedInterval is a {@link RollupActionConfig} that uses a fixed time interval for rolling up data.
 *      * The fixed time interval is one or multiples of SI units and has no calendar-awareness (e.g. doesn't account
 *      * for leap corrections, does not have variable length months, etc).
 *      *
 *      * Calendar-aware interval is not currently supported
 *
 *  {
 *    "fixed_interval" : "1d",
 *    "time_zone" : "UTC"
 *  }
 */
public class RollupActionConfig implements NamedWriteable, ToXContentObject {

    private static final String NAME = "rollup/action/config";
    public static final String FIXED_INTERVAL = "fixed_interval";
    public static final String TIME_ZONE = "time_zone";
    public static final String DEFAULT_TIMEZONE = ZoneId.of("UTC").getId();

    private static final String timestampField = DataStreamTimestampFieldMapper.DEFAULT_PATH;
    private final DateHistogramInterval fixedInterval;
    private final String timeZone;
    private final String intervalType = FIXED_INTERVAL;

    private static final ConstructingObjectParser<RollupActionConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, a -> {
            DateHistogramInterval fixedInterval = (DateHistogramInterval) a[0];
            if (fixedInterval != null) {
                return new RollupActionConfig(fixedInterval, (String) a[1]);
            } else {
                throw new IllegalArgumentException("Parameter [" + FIXED_INTERVAL + "] is required.");
            }
        });

        PARSER.declareField(
            constructorArg(),
            p -> new DateHistogramInterval(p.text()),
            new ParseField(FIXED_INTERVAL),
            ObjectParser.ValueType.STRING
        );
        PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField(TIME_ZONE));
    }

    /**
     * Create a new {@link RollupActionConfig} using the given configuration parameters.
     * <p>
     *     The {@code field} and {@code interval} are required to compute the date histogram for the rolled up documents.
     *     The {@code timeZone} is optional and can be set to {@code null}. When configured, the time zone value  is resolved using
     *     ({@link ZoneId#of(String)} and must match a time zone identifier.
     * </p>
     * @param fixedInterval the interval to use for the date histogram (required)
     * @param timeZone the id of time zone to use to calculate the date histogram (optional). When {@code null}, the UTC timezone is used.
     */
    public RollupActionConfig(final DateHistogramInterval fixedInterval, final @Nullable String timeZone) {
        if (fixedInterval == null) {
            throw new IllegalArgumentException("Parameter [" + FIXED_INTERVAL + "] is required.");
        }
        if (timeZone != null && DEFAULT_TIMEZONE.equals(timeZone) == false) {
            throw new IllegalArgumentException("Parameter [" + TIME_ZONE + "] supports only [" + DEFAULT_TIMEZONE + "].");
        }
        this.fixedInterval = fixedInterval;
        this.timeZone = (timeZone != null && timeZone.isEmpty() == false) ? timeZone : DEFAULT_TIMEZONE;

        // validate interval
        createRounding(this.fixedInterval.toString(), this.timeZone);
    }

    public RollupActionConfig(final StreamInput in) throws IOException {
        String intervalType = in.readString();
        if (FIXED_INTERVAL.equals(intervalType) == false) {
            throw new IllegalStateException("Invalid interval type [" + intervalType + "]");
        }
        fixedInterval = new DateHistogramInterval(in);
        timeZone = in.readString();
    }

    public String getTimestampField() {
        return timestampField;
    }

    public String getIntervalType() {
        return intervalType;
    }

    /**
     * Get the interval value
     */
    public DateHistogramInterval getInterval() {
        return getFixedInterval();
    }

    /**
     * Get the fixed_interval value
     */
    public DateHistogramInterval getFixedInterval() {
        return fixedInterval;
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
        return createRounding(fixedInterval.toString(), timeZone);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(FIXED_INTERVAL);
        fixedInterval.writeTo(out);
        out.writeString(timeZone);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(FIXED_INTERVAL, fixedInterval.toString());
            builder.field(TIME_ZONE, timeZone);
        }
        return builder.endObject();
    }

    public static RollupActionConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other instanceof RollupActionConfig == false) {
            return false;
        }
        final RollupActionConfig that = (RollupActionConfig) other;
        return Objects.equals(fixedInterval, that.fixedInterval)
            && Objects.equals(intervalType, that.intervalType)
            && ZoneId.of(timeZone, ZoneId.SHORT_IDS).getRules().equals(ZoneId.of(that.timeZone, ZoneId.SHORT_IDS).getRules());
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedInterval, intervalType, ZoneId.of(timeZone));
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static Rounding.Prepared createRounding(final String expr, final String timeZone) {
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
