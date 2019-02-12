/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.calendars;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleAction;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.core.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ScheduledEvent implements ToXContentObject, Writeable {

    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField START_TIME = new ParseField("start_time");
    public static final ParseField END_TIME = new ParseField("end_time");
    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField EVENT_ID = new ParseField("event_id");

    public static final ParseField RESULTS_FIELD = new ParseField("events");

    public static final String SCHEDULED_EVENT_TYPE = "scheduled_event";
    public static final String DOCUMENT_ID_PREFIX = "event_";

    public static final ObjectParser<ScheduledEvent.Builder, Void> STRICT_PARSER = createParser(false);
    public static final ObjectParser<ScheduledEvent.Builder, Void> LENIENT_PARSER = createParser(true);

    private static ObjectParser<ScheduledEvent.Builder, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<ScheduledEvent.Builder, Void> parser = new ObjectParser<>("scheduled_event", ignoreUnknownFields, Builder::new);

        parser.declareString(ScheduledEvent.Builder::description, DESCRIPTION);
        parser.declareField(ScheduledEvent.Builder::startTime, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(p.longValue()), ZoneOffset.UTC);
            } else if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(TimeUtils.dateStringToEpoch(p.text())), ZoneOffset.UTC);
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + START_TIME.getPreferredName() + "]");
        }, START_TIME, ObjectParser.ValueType.VALUE);
        parser.declareField(ScheduledEvent.Builder::endTime, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(p.longValue()), ZoneOffset.UTC);
            } else if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(TimeUtils.dateStringToEpoch(p.text())), ZoneOffset.UTC);
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + END_TIME.getPreferredName() + "]");
        }, END_TIME, ObjectParser.ValueType.VALUE);

        parser.declareString(ScheduledEvent.Builder::calendarId, Calendar.ID);
        parser.declareString((builder, s) -> {}, TYPE);

        return parser;
    }

    public static String documentId(String eventId) {
        return DOCUMENT_ID_PREFIX + eventId;
    }

    private final String description;
    private final ZonedDateTime startTime;
    private final ZonedDateTime endTime;
    private final String calendarId;
    private final String eventId;

    ScheduledEvent(String description, ZonedDateTime startTime, ZonedDateTime endTime, String calendarId, @Nullable String eventId) {
        this.description = Objects.requireNonNull(description);
        this.startTime = Objects.requireNonNull(startTime);
        this.endTime = Objects.requireNonNull(endTime);
        this.calendarId = Objects.requireNonNull(calendarId);
        this.eventId = eventId;
    }

    public ScheduledEvent(StreamInput in) throws IOException {
        description = in.readString();
        startTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(in.readVLong()), ZoneOffset.UTC);
        endTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(in.readVLong()), ZoneOffset.UTC);
        calendarId = in.readString();
        eventId = in.readOptionalString();
    }

    public String getDescription() {
        return description;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public ZonedDateTime getEndTime() {
        return endTime;
    }

    public String getCalendarId() {
        return calendarId;
    }

    public String getEventId() {
        return eventId;
    }

    /**
     * Convert the scheduled event to a detection rule.
     * The rule will have 2 time based conditions for the start and
     * end of the event.
     *
     * The rule's start and end times are aligned with the bucket span
     * so the start time is rounded down to a bucket interval and the
     * end time rounded up.
     *
     * @param bucketSpan Bucket span to align to
     * @return The event as a detection rule.
     */
    public DetectionRule toDetectionRule(TimeValue bucketSpan) {
        List<RuleCondition> conditions = new ArrayList<>();

        long bucketSpanSecs = bucketSpan.getSeconds();

        long bucketStartTime = Intervals.alignToFloor(getStartTime().toEpochSecond(), bucketSpanSecs);
        conditions.add(RuleCondition.createTime(Operator.GTE, bucketStartTime));
        long bucketEndTime = Intervals.alignToCeil(getEndTime().toEpochSecond(), bucketSpanSecs);
        conditions.add(RuleCondition.createTime(Operator.LT, bucketEndTime));

        DetectionRule.Builder builder = new DetectionRule.Builder(conditions);
        builder.setActions(RuleAction.SKIP_RESULT, RuleAction.SKIP_MODEL_UPDATE);
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(description);
        out.writeVLong(startTime.toInstant().toEpochMilli());
        out.writeVLong(endTime.toInstant().toEpochMilli());
        out.writeString(calendarId);
        out.writeOptionalString(eventId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DESCRIPTION.getPreferredName(), description);
        builder.timeField(START_TIME.getPreferredName(), START_TIME.getPreferredName() + "_string", startTime.toInstant().toEpochMilli());
        builder.timeField(END_TIME.getPreferredName(), END_TIME.getPreferredName() + "_string", endTime.toInstant().toEpochMilli());
        builder.field(Calendar.ID.getPreferredName(), calendarId);
        if (eventId != null) {
            builder.field(EVENT_ID.getPreferredName(), eventId);
        }
        if (params.paramAsBoolean(ToXContentParams.INCLUDE_TYPE, false)) {
            builder.field(TYPE.getPreferredName(), SCHEDULED_EVENT_TYPE);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ScheduledEvent)) {
            return false;
        }

        ScheduledEvent other = (ScheduledEvent) obj;
        // In Java 8 the tests pass with ZonedDateTime.isEquals() or ZonedDateTime.toInstant.equals()
        // but in Java 9 & 10 the same tests fail.
        // Both isEquals() and toInstant.equals() work the same; convert to epoch seconds and
        // compare seconds and nanos are equal. For some reason the nanos are different in Java 9 & 10.
        // It's sufficient to compare just the epoch seconds for the purpose of establishing equality
        // which only occurs in testing.
        // Note ZonedDataTime.equals() fails because the time zone and date-time must be the same
        // which isn't the case in tests where the time zone is randomised.
        return description.equals(other.description)
                && Objects.equals(startTime.toInstant().getEpochSecond(), other.startTime.toInstant().getEpochSecond())
                && Objects.equals(endTime.toInstant().getEpochSecond(), other.endTime.toInstant().getEpochSecond())
                && calendarId.equals(other.calendarId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, startTime, endTime, calendarId);
    }

    public static class Builder {
        private String description;
        private ZonedDateTime startTime;
        private ZonedDateTime endTime;
        private String calendarId;
        private String eventId;

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder endTime(ZonedDateTime endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder calendarId(String calendarId) {
            this.calendarId = calendarId;
            return this;
        }

        public String getCalendarId() {
            return calendarId;
        }

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public ScheduledEvent build() {
            if (description == null) {
                throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.FIELD_CANNOT_BE_NULL, DESCRIPTION.getPreferredName()));
            }

            if (startTime == null) {
                throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.FIELD_CANNOT_BE_NULL, START_TIME.getPreferredName()));
            }

            if (endTime == null) {
                throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.FIELD_CANNOT_BE_NULL, END_TIME.getPreferredName()));
            }

            if (calendarId == null) {
                throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.FIELD_CANNOT_BE_NULL, Calendar.ID.getPreferredName()));
            }

            if (startTime.isBefore(endTime) == false) {
                throw ExceptionsHelper.badRequestException("Event start time [" + startTime +
                                "] must come before end time [" + endTime + "]");
            }

            ScheduledEvent event = new ScheduledEvent(description, startTime, endTime, calendarId, eventId);

            return event;
        }
    }
}
