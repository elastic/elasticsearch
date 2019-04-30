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
import org.elasticsearch.common.unit.TimeValue;
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

/**
 * A grouping via a date histogram aggregation referencing a timefield
 */
public class DateHistogramGroupSource extends SingleGroupSource implements ToXContentObject {

    private static final ParseField TIME_ZONE = new ParseField("time_zone");
    private static final ParseField FORMAT = new ParseField("format");

    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> PARSER =
            new ConstructingObjectParser<>("date_histogram_group_source",
                true,
                (args) -> {
                   String field = (String)args[0];
                   long interval = 0;
                   DateHistogramInterval dateHistogramInterval = null;
                   if (args[1] instanceof Long) {
                       interval = (Long)args[1];
                   } else {
                       dateHistogramInterval = (DateHistogramInterval) args[1];
                   }
                   ZoneId zoneId = (ZoneId) args[2];
                   String format = (String) args[3];
                   return new DateHistogramGroupSource(field, interval, dateHistogramInterval, format, zoneId);
                });

    static {
        PARSER.declareString(optionalConstructorArg(), FIELD);
        PARSER.declareField(optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue();
            } else {
                return new DateHistogramInterval(p.text());
            }
        }, HistogramGroupSource.INTERVAL, ObjectParser.ValueType.LONG);
        PARSER.declareField(optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZoneId.of(p.text());
            } else {
                return ZoneOffset.ofHours(p.intValue());
            }
        }, TIME_ZONE, ObjectParser.ValueType.LONG);

        PARSER.declareString(optionalConstructorArg(), FORMAT);
    }

    public static DateHistogramGroupSource fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final long interval;
    private final DateHistogramInterval dateHistogramInterval;
    private final String format;
    private final ZoneId timeZone;

    DateHistogramGroupSource(String field, long interval, DateHistogramInterval dateHistogramInterval, String format, ZoneId timeZone) {
        super(field);
        this.interval = interval;
        this.dateHistogramInterval = dateHistogramInterval;
        this.format = format;
        this.timeZone = timeZone;
    }

    @Override
    public Type getType() {
        return Type.DATE_HISTOGRAM;
    }

    public long getInterval() {
        return interval;
    }

    public DateHistogramInterval getDateHistogramInterval() {
        return dateHistogramInterval;
    }

    public String getFormat() {
        return format;
    }

    public ZoneId getTimeZone() {
        return timeZone;
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String field;
        private long interval = 0;
        private DateHistogramInterval dateHistogramInterval;
        private String format;
        private ZoneId timeZone;

        /**
         * The field with which to construct the date histogram grouping
         * @param field The field name
         * @return The {@link Builder} with the field set.
         */
        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        /**
         * Set the interval for the DateHistogram grouping
         * @param interval the time interval in milliseconds
         * @return the {@link Builder} with the interval set.
         */
        public Builder setInterval(long interval) {
            if (interval < 1) {
                throw new IllegalArgumentException("[interval] must be greater than or equal to 1.");
            }
            this.interval = interval;
            return this;
        }

        /**
         * Set the interval for the DateHistogram grouping
         * @param timeValue The time value to use as the interval
         * @return the {@link Builder} with the interval set.
         */
        public Builder setInterval(TimeValue timeValue) {
            return setInterval(timeValue.getMillis());
        }

        /**
         * Sets the interval of the DateHistogram grouping
         *
         * If this DateHistogramInterval is set, it supersedes the #{@link DateHistogramGroupSource#getInterval()}
         * @param dateHistogramInterval the DateHistogramInterval to set
         * @return The {@link Builder} with the dateHistogramInterval set.
         */
        public Builder setDateHistgramInterval(DateHistogramInterval dateHistogramInterval) {
            if (dateHistogramInterval == null) {
                throw new IllegalArgumentException("[dateHistogramInterval] must not be null");
            }
            this.dateHistogramInterval = dateHistogramInterval;
            return this;
        }

        /**
         * Set the optional String formatting for the time interval.
         * @param format The format of the output for the time interval key
         * @return The {@link Builder} with the format set.
         */
        public Builder setFormat(String format) {
            this.format = format;
            return this;
        }

        /**
         * Sets the time zone to use for this aggregation
         * @param timeZone The zoneId for the timeZone
         * @return The {@link Builder} with the timeZone set.
         */
        public Builder setTimeZone(ZoneId timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public DateHistogramGroupSource build() {
            return new DateHistogramGroupSource(field, interval, dateHistogramInterval, format, timeZone);
        }
    }
}
