/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

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
public class DateHistogramGroupConfig implements Validatable, ToXContentObject {

    static final String NAME = "date_histogram";
    private static final String INTERVAL = "interval";
    private static final String FIELD = "field";
    private static final String TIME_ZONE = "time_zone";
    private static final String DELAY = "delay";
    private static final String DEFAULT_TIMEZONE = "UTC";

    private static final ConstructingObjectParser<DateHistogramGroupConfig, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(NAME, true, a ->
            new DateHistogramGroupConfig((String) a[0], (DateHistogramInterval) a[1], (DateHistogramInterval) a[2], (String) a[3]));
        PARSER.declareString(constructorArg(), new ParseField(FIELD));
        PARSER.declareField(constructorArg(), p -> new DateHistogramInterval(p.text()), new ParseField(INTERVAL), ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), p -> new DateHistogramInterval(p.text()), new ParseField(DELAY), ValueType.STRING);
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
     * The {@code field} and {@code interval} are required to compute the date histogram for the rolled up documents.
     * The {@code delay} is optional and can be set to {@code null}. It defines how long to wait before rolling up new documents.
     * The {@code timeZone} is optional and can be set to {@code null}. When configured, the time zone value  is resolved using
     * ({@link DateTimeZone#forID(String)} and must match a time zone identifier provided by the Joda Time library.
     * </p>
     *
     * @param field    the name of the date field to use for the date histogram (required)
     * @param interval the interval to use for the date histogram (required)
     * @param delay    the time delay (optional)
     * @param timeZone the id of time zone to use to calculate the date histogram (optional). When {@code null}, the UTC timezone is used.
     */
    public DateHistogramGroupConfig(final String field,
                                    final DateHistogramInterval interval,
                                    final @Nullable DateHistogramInterval delay,
                                    final @Nullable String timeZone) {
        this.field = field;
        this.interval = interval;
        this.delay = delay;
        this.timeZone = (timeZone != null && timeZone.isEmpty() == false) ? timeZone : DEFAULT_TIMEZONE;
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (field == null || field.isEmpty()) {
            validationException.addValidationError("Field name is required");
        }
        if (interval == null) {
            validationException.addValidationError("Interval is required");
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(validationException);
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

    public static DateHistogramGroupConfig fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
