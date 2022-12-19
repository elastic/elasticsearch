/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This class holds the configuration details of a {@link DownsampleAction} that downsamples time series
 * (TSDB) indices. We have made great effort to simplify the rollup configuration and currently
 * only requires a fixed time interval. So, it has the following format:
 *
 *  {
 *    "fixed_interval" : "1d",
 *  }
 *
 * fixed_interval is one or multiples of SI units and has no calendar-awareness (e.g. doesn't account
 * for leap corrections, does not have variable length months, etc). Calendar-aware interval is not currently
 * supported.
 *
 * Also, the rollup configuration uses the UTC time zone by default and the "@timestamp" field as
 * the index field that stores the timestamp of the time series index.
 *
 * Finally, we have left methods such as {@link DownsampleConfig#getTimestampField()},
 * {@link DownsampleConfig#getTimeZone()} and  {@link DownsampleConfig#getIntervalType()} for
 * future extensions.
 */
public class DownsampleConfig implements NamedWriteable, ToXContentObject {

    private static final String NAME = "downsample/action/config";
    public static final String FIXED_INTERVAL = "fixed_interval";
    public static final String TIME_ZONE = "time_zone";
    public static final String DEFAULT_TIMEZONE = ZoneId.of("UTC").getId();

    private static final String timestampField = DataStreamTimestampFieldMapper.DEFAULT_PATH;
    private final DateHistogramInterval fixedInterval;
    private final String timeZone = DEFAULT_TIMEZONE;
    private final String intervalType = FIXED_INTERVAL;

    private static final ConstructingObjectParser<DownsampleConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, a -> {
            DateHistogramInterval fixedInterval = (DateHistogramInterval) a[0];
            if (fixedInterval != null) {
                return new DownsampleConfig(fixedInterval);
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
    }

    /**
     * Create a new {@link DownsampleConfig} using the given configuration parameters.
     * @param fixedInterval the fixed interval to use for computing the date histogram for the rolled up documents (required).
     */
    public DownsampleConfig(final DateHistogramInterval fixedInterval) {
        if (fixedInterval == null) {
            throw new IllegalArgumentException("Parameter [" + FIXED_INTERVAL + "] is required.");
        }
        this.fixedInterval = fixedInterval;

        // validate interval
        createRounding(this.fixedInterval.toString(), this.timeZone);
    }

    public DownsampleConfig(final StreamInput in) throws IOException {
        fixedInterval = new DateHistogramInterval(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        fixedInterval.writeTo(out);
    }

    /**
     * Get the timestamp field to be used for rolling up data. Currently,
     * only the "@timestamp" value is supported.
     */
    public String getTimestampField() {
        return timestampField;
    }

    /**
     * Get the interval type. Currently, only fixed_interval is supported
     */
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
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(FIXED_INTERVAL, fixedInterval.toString());
        }
        return builder.endObject();
    }

    public static DownsampleConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || other instanceof DownsampleConfig == false) {
            return false;
        }
        final DownsampleConfig that = (DownsampleConfig) other;
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
