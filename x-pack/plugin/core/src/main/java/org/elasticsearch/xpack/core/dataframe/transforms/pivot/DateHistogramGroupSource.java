/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;


public class DateHistogramGroupSource extends SingleGroupSource {

    private static final String NAME = "data_frame_date_histogram_group";
    private static final ParseField TIME_ZONE = new ParseField("time_zone");
    private static final ParseField FORMAT = new ParseField("format");

    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> LENIENT_PARSER = createParser(true);
    private long interval = 0;
    private DateHistogramInterval dateHistogramInterval;
    private String format;
    private ZoneId timeZone;

    public DateHistogramGroupSource(String field) {
        super(field);
    }

    public DateHistogramGroupSource(StreamInput in) throws IOException {
        super(in);
        this.interval = in.readLong();
        this.dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        this.timeZone = in.readOptionalZoneId();
        this.format = in.readOptionalString();
    }

    private static ConstructingObjectParser<DateHistogramGroupSource, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DateHistogramGroupSource, Void> parser = new ConstructingObjectParser<>(NAME, lenient, (args) -> {
            String field = (String) args[0];
            return new DateHistogramGroupSource(field);
        });

        declareValuesSourceFields(parser);

        parser.declareField((histogram, interval) -> {
            if (interval instanceof Long) {
                histogram.setInterval((long) interval);
            } else {
                histogram.setDateHistogramInterval((DateHistogramInterval) interval);
            }
        }, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            } else {
                return new DateHistogramInterval(p.text());
            }
        }, HistogramGroupSource.INTERVAL, ObjectParser.ValueType.LONG);

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

    @Override
    public Type getType() {
        return Type.DATE_HISTOGRAM;
    }

    public long getInterval() {
        return interval;
    }

    public void setInterval(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException("[interval] must be greater than or equal to 1.");
        }
        this.interval = interval;
    }

    public DateHistogramInterval getDateHistogramInterval() {
        return dateHistogramInterval;
    }

    public void setDateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null");
        }
        this.dateHistogramInterval = dateHistogramInterval;
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
        out.writeLong(interval);
        out.writeOptionalWriteable(dateHistogramInterval);
        out.writeOptionalZoneId(timeZone);
        out.writeOptionalString(format);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        if (dateHistogramInterval == null) {
            builder.field(HistogramGroupSource.INTERVAL.getPreferredName(), interval);
        } else {
            builder.field(HistogramGroupSource.INTERVAL.getPreferredName(), dateHistogramInterval.toString());
        }
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
            Objects.equals(dateHistogramInterval, that.dateHistogramInterval) &&
            Objects.equals(timeZone, that.timeZone) &&
            Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, interval, dateHistogramInterval, timeZone, format);
    }
}
