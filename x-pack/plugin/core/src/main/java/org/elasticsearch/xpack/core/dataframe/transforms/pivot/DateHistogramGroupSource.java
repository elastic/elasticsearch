/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;


public class DateHistogramGroupSource extends SingleGroupSource {

    private static final int CALENDAR_INTERVAL_ID = 1;
    private static final int FIXED_INTERVAL_ID = 0;
    private static final long THIRTY_ONE_DAYS_MS = new TimeValue(31, TimeUnit.DAYS).getMillis();


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
    }

    public static class CalendarInterval implements Interval {
        private static final String NAME = "calendar_interval";
        private final DateHistogramInterval interval;

        public CalendarInterval(DateHistogramInterval interval) {
            this.interval = interval;
            if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(interval.toString()) == null) {
                throw new IllegalArgumentException("The supplied interval [" + interval + "] could not be parsed " +
                    "as a calendar interval.");
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
    }

    private Interval readInterval(StreamInput in) throws IOException {
        byte id = in.readByte();
        switch (id) {
        case FIXED_INTERVAL_ID:
            return new FixedInterval(in);
        case CALENDAR_INTERVAL_ID:
            return new CalendarInterval(in);
        default:
            throw new IllegalArgumentException("unknown interval type [" + id + "]");
        }
    }

    private void writeInterval(Interval interval, StreamOutput out) throws IOException {
        out.write(interval.getIntervalTypeId());
        interval.writeTo(out);
    }

    private static final String NAME = "data_frame_date_histogram_group";
    private static final ParseField TIME_ZONE = new ParseField("time_zone");
    private static final ParseField FORMAT = new ParseField("format");

    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> LENIENT_PARSER = createParser(true);

    private final Interval interval;
    private String format;
    private ZoneId timeZone;

    public DateHistogramGroupSource(String field, Interval interval) {
        super(field);
        this.interval = interval;
    }

    public DateHistogramGroupSource(StreamInput in) throws IOException {
        super(in);
        this.interval = readInterval(in);
        this.timeZone = in.readOptionalZoneId();
        this.format = in.readOptionalString();
    }

    private static ConstructingObjectParser<DateHistogramGroupSource, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DateHistogramGroupSource, Void> parser = new ConstructingObjectParser<>(NAME, lenient, (args) -> {
            String field = (String) args[0];
            String fixedInterval = (String) args[1];
            String calendarInterval = (String) args[2];

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

            return new DateHistogramGroupSource(field, interval);
        });

        declareValuesSourceFields(parser);

        parser.declareString(optionalConstructorArg(), new ParseField(FixedInterval.NAME));
        parser.declareString(optionalConstructorArg(), new ParseField(CalendarInterval.NAME));

        parser.declareField(DateHistogramGroupSource::setTimeZone, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZoneId.of(p.text());
            } else {
                return ZoneOffset.ofHours(p.intValue());
            }
        }, TIME_ZONE, ObjectParser.ValueType.LONG);

        parser.declareString(DateHistogramGroupSource::setFormat, FORMAT);
        return parser;
    }

    public static DateHistogramGroupSource fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public static boolean isInvalidFormat(String format, DateHistogramInterval interval) {
        if (format == null || interval == null) {
            return false;
        }
        DateFormatter formatter = DateFormatter.forPattern(format);
        String expression = interval.toString();
        String epochFormat = formatter.formatMillis(Instant.EPOCH.toEpochMilli());
        String epochPlusIntervalFormat = formatter.formatMillis(Instant.EPOCH.toEpochMilli() + interval.estimateMillis());
        if (DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(expression)) {
            // The estimated rounding defaults to 30 days for monthly, EPOCH starts in January, which has 31 days.
            if (Rounding.DateTimeUnit.MONTH_OF_YEAR.equals(DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expression))) {
                epochPlusIntervalFormat = formatter.formatMillis(Instant.EPOCH.toEpochMilli() + THIRTY_ONE_DAYS_MS);
            }
        } else {
            // This should never really happen in practice. date_histogram does not support multiple month intervals
            if (expression.endsWith("M")) {
                throw new IllegalArgumentException("only single month intervals are supported");
            }
            final String normalized = expression.toLowerCase(Locale.ROOT).trim();
            long millisecondLength;
            if (normalized.endsWith("ms")) {
                millisecondLength = unitMilliseconds(normalized, "ms", TimeUnit.MILLISECONDS).getMillis();
            } else if (normalized.endsWith("s")) {
                millisecondLength = unitMilliseconds(normalized, "s", TimeUnit.SECONDS).getMillis();
            } else if (normalized.endsWith("m")) {
                millisecondLength = unitMilliseconds(normalized, "m", TimeUnit.MINUTES).getMillis();
            } else if (normalized.endsWith("h")) {
                millisecondLength = unitMilliseconds(normalized, "h", TimeUnit.HOURS).getMillis();
            } else if (normalized.endsWith("d")) {
                millisecondLength = unitMilliseconds(normalized, "d", TimeUnit.DAYS).getMillis();
            } else {
                return true;
            }
            epochPlusIntervalFormat = formatter.formatMillis(Instant.EPOCH.toEpochMilli() + millisecondLength);
        }
        return epochFormat.equals(epochPlusIntervalFormat);
    }

    // We round down and + 1 for all format types to force the interval to make a change in the format time unit value
    private static TimeValue unitMilliseconds(String value, String suffix, TimeUnit unit) {
        long valueAsLong = parse(value, suffix);
        switch (unit) {
            case MILLISECONDS:
                return new TimeValue((valueAsLong % 999) + 1, unit);
            case SECONDS:
            case MINUTES:
                return new TimeValue((valueAsLong % 59) + 1, unit);
            case HOURS:
                return new TimeValue((valueAsLong % 23) + 1, unit);
            case DAYS:
                return new TimeValue((valueAsLong % 30) + 1, unit);
            default:
                throw new IllegalArgumentException("failed to determine TimeUnit [" + unit + "]");
        }
    }

    private static long parse(final String value, final String suffix) {
        final String s = value.substring(0, value.length() - suffix.length()).trim();
        try {
            return Long.parseLong(s);
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse [" + value + "]", e);
        }
    }

    @Override
    public Type getType() {
        return Type.DATE_HISTOGRAM;
    }

    public Interval getInterval() {
        return interval;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public ZoneId getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(ZoneId timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(field);
        writeInterval(interval, out);
        out.writeOptionalZoneId(timeZone);
        out.writeOptionalString(format);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        interval.toXContent(builder, params);
        if (timeZone != null) {
            builder.field(TIME_ZONE.getPreferredName(), timeZone.toString());
        }
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
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

        return Objects.equals(this.field, that.field) &&
            Objects.equals(interval, that.interval) &&
            Objects.equals(timeZone, that.timeZone) &&
            Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, interval, timeZone, format);
    }
}
