/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A grouping via a date histogram aggregation referencing a timefield
 */
public class DateHistogramGroupSource extends SingleGroupSource implements ToXContentObject {

    private static final ParseField TIME_ZONE = new ParseField("time_zone");

    // From DateHistogramAggregationBuilder in core, transplanted and modified to a set
    // so we don't need to import a dependency on the class
    private static final Set<String> DATE_FIELD_UNITS = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                "year",
                "1y",
                "quarter",
                "1q",
                "month",
                "1M",
                "week",
                "1w",
                "day",
                "1d",
                "hour",
                "1h",
                "minute",
                "1m",
                "second",
                "1s"
            )
        )
    );

    /**
     * Interval can be specified in 2 ways:
     *
     * fixed_interval fixed intervals like 1h, 1m, 1d
     * calendar_interval calendar aware intervals like 1M, 1Y, ...
     *
     * Note: transform does not support the deprecated interval option
     */
    public interface Interval extends ToXContentFragment {
        String getName();

        DateHistogramInterval getInterval();
    }

    public static class FixedInterval implements Interval {
        private static final String NAME = "fixed_interval";
        private final DateHistogramInterval interval;

        public FixedInterval(DateHistogramInterval interval) {
            this.interval = interval;
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME);
            interval.toXContent(builder, params);
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
            if (DATE_FIELD_UNITS.contains(interval.toString()) == false) {
                throw new IllegalArgumentException(
                    "The supplied interval [" + interval + "] could not be parsed " + "as a calendar interval."
                );
            }
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(NAME);
            interval.toXContent(builder, params);
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

            final CalendarInterval that = (CalendarInterval) other;
            return Objects.equals(this.interval, that.interval);
        }

        @Override
        public int hashCode() {
            return Objects.hash(interval);
        }
    }

    private static final ConstructingObjectParser<DateHistogramGroupSource, Void> PARSER = new ConstructingObjectParser<>(
        "date_histogram_group_source",
        true,
        (args) -> {
            String field = (String) args[0];
            Script script = (Script) args[1];
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

            return new DateHistogramGroupSource(field, script, missingBucket, interval, zoneId);
        }
    );

    static {
        PARSER.declareString(optionalConstructorArg(), FIELD);
        Script.declareScript(PARSER, optionalConstructorArg(), SCRIPT);
        PARSER.declareBoolean(optionalConstructorArg(), MISSING_BUCKET);
        PARSER.declareString(optionalConstructorArg(), new ParseField(FixedInterval.NAME));
        PARSER.declareString(optionalConstructorArg(), new ParseField(CalendarInterval.NAME));

        PARSER.declareField(optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZoneId.of(p.text());
            } else {
                return ZoneOffset.ofHours(p.intValue());
            }
        }, TIME_ZONE, ObjectParser.ValueType.LONG);
    }

    public static DateHistogramGroupSource fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Interval interval;
    private final ZoneId timeZone;

    DateHistogramGroupSource(String field, Script script, Interval interval, ZoneId timeZone) {
        this(field, script, false, interval, timeZone);
    }

    DateHistogramGroupSource(String field, Script script, boolean missingBucket, Interval interval, ZoneId timeZone) {
        super(field, script, missingBucket);
        this.interval = interval;
        this.timeZone = timeZone;
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
            && Objects.equals(this.script, that.script)
            && Objects.equals(this.interval, that.interval)
            && Objects.equals(this.timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, script, missingBucket, interval, timeZone);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String field;
        private Script script;
        private Interval interval;
        private ZoneId timeZone;
        private boolean missingBucket;

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
         * The script with which to construct the date histogram grouping
         * @param script The script
         * @return The {@link Builder} with the script set.
         */
        public Builder setScript(Script script) {
            this.script = script;
            return this;
        }

        /**
         * Set the interval for the DateHistogram grouping
         * @param interval a fixed or calendar interval
         * @return the {@link Builder} with the interval set.
         */
        public Builder setInterval(Interval interval) {
            this.interval = interval;
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

        /**
         * Sets the value of "missing_bucket"
         * @param missingBucket value of "missing_bucket" to be set
         * @return The {@link Builder} with "missing_bucket" set.
         */
        public Builder setMissingBucket(boolean missingBucket) {
            this.missingBucket = missingBucket;
            return this;
        }

        public DateHistogramGroupSource build() {
            return new DateHistogramGroupSource(field, script, missingBucket, interval, timeZone);
        }
    }
}
