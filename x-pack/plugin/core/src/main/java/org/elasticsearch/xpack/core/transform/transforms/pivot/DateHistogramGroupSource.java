/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DateHistogramGroupSource extends SingleGroupSource {

    private static final int CALENDAR_INTERVAL_ID = 1;
    private static final int FIXED_INTERVAL_ID = 0;

    /**
     * Interval can be specified in 2 ways:
     *
     * fixed_interval fixed intervals like 1h, 1m, 1d
     * calendar_interval calendar aware intervals like 1M, 1Y, ...
     *
     * Note: data frames do not support the deprecated interval option
     */
    public interface Interval extends Writeable, ToXContentFragment {
        String getName();

        DateHistogramInterval getInterval();

        byte getIntervalTypeId();
    }

    public static class FixedInterval implements Interval {
        private static final String NAME = "fixed_interval";
        private final DateHistogramInterval interval;

        public FixedInterval(DateHistogramInterval interval) {
            this.interval = interval;
        }

        public FixedInterval(StreamInput in) throws IOException {
            this.interval = new DateHistogramInterval(in);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public DateHistogramInterval getInterval() {
            return interval;
        }

        @Override
        public byte getIntervalTypeId() {
            return FIXED_INTERVAL_ID;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME);
            interval.toXContent(builder, params);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            interval.writeTo(out);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final FixedInterval that = (FixedInterval) other;
            return Objects.equals(this.interval, that.interval);
        }

        @Override
        public int hashCode() {
            return Objects.hash(interval);
        }

        @Override
        public String toString() {
            return interval.toString();
        }
    }

    public static class CalendarInterval implements Interval {
        private static final String NAME = "calendar_interval";
        private final DateHistogramInterval interval;

        public CalendarInterval(DateHistogramInterval interval) {
            this.interval = interval;
            if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString()) == null) {
                throw new IllegalArgumentException(
                    "The supplied interval [" + interval + "] could not be parsed " + "as a calendar interval."
                );
            }
        }

        public CalendarInterval(StreamInput in) throws IOException {
            this.interval = new DateHistogramInterval(in);
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public DateHistogramInterval getInterval() {
            return interval;
        }

        @Override
        public byte getIntervalTypeId() {
            return CALENDAR_INTERVAL_ID;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME);
            interval.toXContent(builder, params);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            interval.writeTo(out);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final CalendarInterval that = (CalendarInterval) other;
            return Objects.equals(this.interval, that.interval);
        }

        @Override
        public int hashCode() {
            return Objects.hash(interval);
        }

        @Override
        public String toString() {
            return interval.toString();
        }
    }

    private Interval readInterval(StreamInput in) throws IOException {
        byte id = in.readByte();
        return switch (id) {
            case FIXED_INTERVAL_ID -> new FixedInterval(in);
            case CALENDAR_INTERVAL_ID -> new CalendarInterval(in);
            default -> throw new IllegalArgumentException("unknown interval type [" + id + "]");
        };
    }

    private void writeInterval(Interval anInterval, StreamOutput out) throws IOException {
        out.write(anInterval.getIntervalTypeId());
        anInterval.writeTo(out);
    }

    private static final String NAME = "data_frame_date_histogram_group";
    private static final ParseField TIME_ZONE = new ParseField("time_zone");

    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> LENIENT_PARSER = createParser(true);

    private final Interval interval;
    private final ZoneId timeZone;
    private final Rounding.Prepared rounding;

    public DateHistogramGroupSource(String field, ScriptConfig scriptConfig, boolean missingBucket, Interval interval, ZoneId timeZone) {
        super(field, scriptConfig, missingBucket);
        this.interval = interval;
        this.timeZone = timeZone;
        rounding = buildRounding();
    }

    public DateHistogramGroupSource(StreamInput in) throws IOException {
        super(in);
        this.interval = readInterval(in);
        this.timeZone = in.readOptionalZoneId();
        rounding = buildRounding();
    }

    private Rounding.Prepared buildRounding() {
        Rounding.DateTimeUnit timeUnit = DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString());
        final Rounding.Builder roundingBuilder;
        if (timeUnit != null) {
            roundingBuilder = new Rounding.Builder(timeUnit);
        } else {
            roundingBuilder = new Rounding.Builder(TimeValue.parseTimeValue(interval.toString(), interval.getName()));
        }

        if (timeZone != null) {
            roundingBuilder.timeZone(timeZone);
        }
        return roundingBuilder.build().prepareForUnknown();
    }

    private static ConstructingObjectParser<DateHistogramGroupSource, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DateHistogramGroupSource, Void> parser = new ConstructingObjectParser<>(NAME, lenient, (args) -> {
            String field = (String) args[0];
            ScriptConfig scriptConfig = (ScriptConfig) args[1];
            boolean missingBucket = args[2] == null ? false : (boolean) args[2];
            String fixedInterval = (String) args[3];
            String calendarInterval = (String) args[4];
            ZoneId zoneId = (ZoneId) args[5];

            Interval interval = null;

            if (fixedInterval != null && calendarInterval != null) {
                throw new IllegalArgumentException("You must specify either fixed_interval or calendar_interval, found both");
            } else if (fixedInterval != null) {
                interval = new FixedInterval(new DateHistogramInterval(fixedInterval));
            } else if (calendarInterval != null) {
                interval = new CalendarInterval(new DateHistogramInterval(calendarInterval));
            } else {
                throw new IllegalArgumentException("You must specify either fixed_interval or calendar_interval, found none");
            }

            return new DateHistogramGroupSource(field, scriptConfig, missingBucket, interval, zoneId);
        });

        declareValuesSourceFields(parser, lenient);

        parser.declareString(optionalConstructorArg(), new ParseField(FixedInterval.NAME));
        parser.declareString(optionalConstructorArg(), new ParseField(CalendarInterval.NAME));

        parser.declareField(optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZoneId.of(p.text());
            } else {
                return ZoneOffset.ofHours(p.intValue());
            }
        }, TIME_ZONE, ObjectParser.ValueType.LONG);

        return parser;
    }

    public static DateHistogramGroupSource fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    @Override
    public Type getType() {
        return Type.DATE_HISTOGRAM;
    }

    public Interval getInterval() {
        return interval;
    }

    public ZoneId getTimeZone() {
        return timeZone;
    }

    public Rounding.Prepared getRounding() {
        return rounding;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeInterval(interval, out);
        out.writeOptionalZoneId(timeZone);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.innerXContent(builder, params);
        interval.toXContent(builder, params);
        if (timeZone != null) {
            builder.field(TIME_ZONE.getPreferredName(), timeZone.toString());
        }
        builder.endObject();
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

        final DateHistogramGroupSource that = (DateHistogramGroupSource) other;

        return this.missingBucket == that.missingBucket
            && Objects.equals(this.field, that.field)
            && Objects.equals(this.scriptConfig, that.scriptConfig)
            && Objects.equals(this.interval, that.interval)
            && Objects.equals(this.timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, scriptConfig, missingBucket, interval, timeZone);
    }
}
