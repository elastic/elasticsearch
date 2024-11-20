/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.calendars;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleAction;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.config.RuleParams;
import org.elasticsearch.xpack.core.ml.job.config.RuleParamsForForceTimeShift;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.Intervals;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ScheduledEvent implements ToXContentObject, Writeable {

    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField START_TIME = new ParseField("start_time");
    public static final ParseField END_TIME = new ParseField("end_time");
    public static final ParseField SKIP_RESULT = new ParseField("skip_result");
    public static final ParseField SKIP_MODEL_UPDATE = new ParseField("skip_model_update");
    public static final ParseField FORCE_TIME_SHIFT = new ParseField("force_time_shift");
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
        parser.declareField(
            ScheduledEvent.Builder::startTime,
            p -> TimeUtils.parseTimeFieldToInstant(p, START_TIME.getPreferredName()),
            START_TIME,
            ObjectParser.ValueType.VALUE
        );
        parser.declareField(
            ScheduledEvent.Builder::endTime,
            p -> TimeUtils.parseTimeFieldToInstant(p, END_TIME.getPreferredName()),
            END_TIME,
            ObjectParser.ValueType.VALUE
        );
        parser.declareBoolean(ScheduledEvent.Builder::skipResult, SKIP_RESULT);
        parser.declareBoolean(ScheduledEvent.Builder::skipModelUpdate, SKIP_MODEL_UPDATE);
        parser.declareInt(ScheduledEvent.Builder::forceTimeShift, FORCE_TIME_SHIFT);
        parser.declareString(ScheduledEvent.Builder::calendarId, Calendar.ID);
        parser.declareString((builder, s) -> {}, TYPE);

        return parser;
    }

    public static String documentId(String eventId) {
        return DOCUMENT_ID_PREFIX + eventId;
    }

    private final String description;
    private final Instant startTime;
    private final Instant endTime;
    private final Boolean skipResult;
    private final Boolean skipModelUpdate;
    private final Integer forceTimeShift;
    private final String calendarId;
    private final String eventId;

    ScheduledEvent(
        String description,
        Instant startTime,
        Instant endTime,
        Boolean skipResult,
        Boolean skipModelUpdate,
        @Nullable Integer forceTimeShift,
        String calendarId,
        @Nullable String eventId
    ) {
        this.description = Objects.requireNonNull(description);
        this.startTime = Instant.ofEpochMilli(Objects.requireNonNull(startTime).toEpochMilli());
        this.endTime = Instant.ofEpochMilli(Objects.requireNonNull(endTime).toEpochMilli());
        this.skipResult = Objects.requireNonNull(skipResult);
        this.skipModelUpdate = Objects.requireNonNull(skipModelUpdate);
        this.forceTimeShift = forceTimeShift;
        this.calendarId = Objects.requireNonNull(calendarId);
        this.eventId = eventId;
    }

    public ScheduledEvent(StreamInput in) throws IOException {
        description = in.readString();
        startTime = in.readInstant();
        endTime = in.readInstant();
        if (in.getTransportVersion().onOrAfter(TransportVersions.ML_SCHEDULED_EVENT_TIME_SHIFT_CONFIGURATION)) {
            skipResult = in.readBoolean();
            skipModelUpdate = in.readBoolean();
            forceTimeShift = in.readOptionalInt();
        } else {
            skipResult = true;
            skipModelUpdate = true;
            forceTimeShift = null;
        }
        calendarId = in.readString();
        eventId = in.readOptionalString();
    }

    public String getDescription() {
        return description;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public String getCalendarId() {
        return calendarId;
    }

    public Boolean getSkipResult() {
        return skipResult;
    }

    public Boolean getSkipModelUpdate() {
        return skipModelUpdate;
    }

    public Integer getForceTimeShift() {
        return forceTimeShift;
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

        long bucketStartTime = Intervals.alignToFloor(getStartTime().getEpochSecond(), bucketSpanSecs);
        conditions.add(RuleCondition.createTime(Operator.GTE, bucketStartTime));
        long bucketEndTime = Intervals.alignToCeil(getEndTime().getEpochSecond(), bucketSpanSecs);
        conditions.add(RuleCondition.createTime(Operator.LT, bucketEndTime));

        DetectionRule.Builder builder = new DetectionRule.Builder(conditions);
        List<String> ruleActions = new ArrayList<>();
        if (skipResult) {
            ruleActions.add(RuleAction.SKIP_RESULT.toString());
            builder.setActions(RuleAction.SKIP_RESULT);
        }
        if (skipModelUpdate) {
            ruleActions.add(RuleAction.SKIP_MODEL_UPDATE.toString());
        }
        if (forceTimeShift != null) {
            ruleActions.add(RuleAction.FORCE_TIME_SHIFT.toString());
            builder.setParams(new RuleParams(new RuleParamsForForceTimeShift(forceTimeShift)));
        }
        builder.setActions(ruleActions);
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(description);
        out.writeInstant(startTime);
        out.writeInstant(endTime);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_SCHEDULED_EVENT_TIME_SHIFT_CONFIGURATION)) {
            out.writeBoolean(skipResult);
            out.writeBoolean(skipModelUpdate);
            out.writeOptionalInt(forceTimeShift);
        }
        out.writeString(calendarId);
        out.writeOptionalString(eventId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DESCRIPTION.getPreferredName(), description);
        builder.timestampFieldsFromUnixEpochMillis(
            START_TIME.getPreferredName(),
            START_TIME.getPreferredName() + "_string",
            startTime.toEpochMilli()
        );
        builder.timestampFieldsFromUnixEpochMillis(
            END_TIME.getPreferredName(),
            END_TIME.getPreferredName() + "_string",
            endTime.toEpochMilli()
        );
        builder.field(SKIP_RESULT.getPreferredName(), skipResult);
        builder.field(SKIP_MODEL_UPDATE.getPreferredName(), skipModelUpdate);
        if (forceTimeShift != null) {
            builder.field(FORCE_TIME_SHIFT.getPreferredName(), forceTimeShift);
        }
        builder.field(Calendar.ID.getPreferredName(), calendarId);
        if (eventId != null) {
            builder.field(EVENT_ID.getPreferredName(), eventId);
        }
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
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

        if ((obj instanceof ScheduledEvent) == false) {
            return false;
        }

        ScheduledEvent other = (ScheduledEvent) obj;
        return description.equals(other.description)
            && Objects.equals(startTime, other.startTime)
            && Objects.equals(endTime, other.endTime)
            && Objects.equals(skipResult, other.skipResult)
            && Objects.equals(skipModelUpdate, other.skipModelUpdate)
            && Objects.equals(forceTimeShift, other.forceTimeShift)
            && calendarId.equals(other.calendarId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, startTime, endTime, skipResult, skipModelUpdate, forceTimeShift, calendarId);
    }

    public static class Builder {
        private String description;
        private Instant startTime;
        private Instant endTime;
        private Boolean skipResult;
        private Boolean skipModelUpdate;
        private Integer forceTimeShift;
        private String calendarId;
        private String eventId;

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder startTime(Instant startTime) {
            this.startTime = Instant.ofEpochMilli(Objects.requireNonNull(startTime, START_TIME.getPreferredName()).toEpochMilli());
            return this;
        }

        public Builder endTime(Instant endTime) {
            this.endTime = Instant.ofEpochMilli(Objects.requireNonNull(endTime, END_TIME.getPreferredName()).toEpochMilli());
            return this;
        }

        public Builder skipResult(Boolean skipResult) {
            this.skipResult = skipResult;
            return this;
        }

        public Builder skipModelUpdate(Boolean skipModelUpdate) {
            this.skipModelUpdate = skipModelUpdate;
            return this;
        }

        public Builder forceTimeShift(Integer forceTimeShift) {
            this.forceTimeShift = forceTimeShift;
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
                    Messages.getMessage(Messages.FIELD_CANNOT_BE_NULL, DESCRIPTION.getPreferredName())
                );
            }

            if (startTime == null) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.FIELD_CANNOT_BE_NULL, START_TIME.getPreferredName())
                );
            }

            if (endTime == null) {
                throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.FIELD_CANNOT_BE_NULL, END_TIME.getPreferredName()));
            }

            if (calendarId == null) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.FIELD_CANNOT_BE_NULL, Calendar.ID.getPreferredName())
                );
            }

            if (startTime.isBefore(endTime) == false) {
                throw ExceptionsHelper.badRequestException(
                    "Event start time [" + startTime + "] must come before end time [" + endTime + "]"
                );
            }

            skipResult = skipResult == null || skipResult;
            skipModelUpdate = skipModelUpdate == null || skipModelUpdate;

            ScheduledEvent event = new ScheduledEvent(
                description,
                startTime,
                endTime,
                skipResult,
                skipModelUpdate,
                forceTimeShift,
                calendarId,
                eventId
            );

            return event;
        }
    }
}
