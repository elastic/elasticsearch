/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.downsample;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DownsampleDateHistogramConfig implements Writeable, ToXContentObject {
    static final String NAME = "date_histogram";
    public static final String FIXED_INTERVAL = "fixed_interval";
    public static final String CALENDAR_INTERVAL = "calendar_interval";
    public static final String TIME_ZONE = "time_zone";
    public static final String DEFAULT_TIMEZONE = ZoneId.of("UTC").getId();

    private static final ConstructingObjectParser<DownsampleDateHistogramConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(
            NAME,
            a -> new DownsampleDateHistogramConfig((DateHistogramInterval) a[0], (DateHistogramInterval) a[1], (String) a[2])
        );
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

    private final DateHistogramInterval calendarInterval;
    private final DateHistogramInterval fixedInterval;
    private final String timeZone;

    public DownsampleDateHistogramConfig(
        final @Nullable DateHistogramInterval calendarInterval,
        final @Nullable DateHistogramInterval fixedInterval,
        final @Nullable String timeZone
    ) {
        this.calendarInterval = calendarInterval;
        this.fixedInterval = fixedInterval;
        this.timeZone = (timeZone != null && timeZone.isEmpty() == false) ? timeZone : DEFAULT_TIMEZONE;
    }

    public DownsampleDateHistogramConfig(final StreamInput in) throws IOException {
        this.calendarInterval = new DateHistogramInterval(in);
        this.fixedInterval = new DateHistogramInterval(in);
        this.timeZone = in.readString();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalWriteable(calendarInterval);
        out.writeOptionalWriteable(fixedInterval);
        out.writeString(timeZone);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(CALENDAR_INTERVAL, calendarInterval);
            builder.field(FIXED_INTERVAL, fixedInterval);
            builder.field(TIME_ZONE, timeZone);
        }
        return builder.endObject();
    }

    public static DownsampleDateHistogramConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public DateHistogramInterval getCalendarInterval() {
        return calendarInterval;
    }

    public DateHistogramInterval getFixedInterval() {
        return fixedInterval;
    }

    public String getTimeZone() {
        return timeZone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DownsampleDateHistogramConfig that = (DownsampleDateHistogramConfig) o;
        return Objects.equals(calendarInterval, that.calendarInterval)
            && Objects.equals(fixedInterval, that.fixedInterval)
            && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(calendarInterval, fixedInterval, timeZone);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
