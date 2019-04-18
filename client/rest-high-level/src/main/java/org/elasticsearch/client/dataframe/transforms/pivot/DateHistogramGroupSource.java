/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DateHistogramGroupSource extends SingleGroupSource implements ToXContentObject {

    private static final ParseField TIME_ZONE = new ParseField("time_zone");
    private static final ParseField FORMAT = new ParseField("format");

    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> PARSER =
            new ConstructingObjectParser<>("date_histogram_group_source", true, (args) -> new DateHistogramGroupSource((String) args[0]));

    static {
        PARSER.declareString(optionalConstructorArg(), FIELD);
        PARSER.declareField((histogram, interval) -> {
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
        PARSER.declareField(DateHistogramGroupSource::setTimeZone, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZoneId.of(p.text());
            } else {
                return ZoneOffset.ofHours(p.intValue());
            }
        }, TIME_ZONE, ObjectParser.ValueType.LONG);

        PARSER.declareString(DateHistogramGroupSource::setFormat, FORMAT);
    }

    public static DateHistogramGroupSource fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private long interval = 0;
    private DateHistogramInterval dateHistogramInterval;
    private String format;
    private ZoneId timeZone;

    public DateHistogramGroupSource(String field) {
        super(field);
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
