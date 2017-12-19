/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.calendars;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.job.config.Connective;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.Operator;
import org.elasticsearch.xpack.ml.job.config.RuleAction;
import org.elasticsearch.xpack.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.ml.utils.Intervals;
import org.elasticsearch.xpack.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SpecialEvent implements ToXContentObject, Writeable {

    public static final ParseField ID = new ParseField("id");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField START_TIME = new ParseField("start_time");
    public static final ParseField END_TIME = new ParseField("end_time");
    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField JOB_IDS = new ParseField("job_ids");

    public static final String SPECIAL_EVENT_TYPE = "special_event";
    public static final String DOCUMENT_ID_PREFIX = "event_";

    public static final ConstructingObjectParser<SpecialEvent, Void> PARSER =
            new ConstructingObjectParser<>("special_event", a -> new SpecialEvent((String) a[0], (String) a[1], (ZonedDateTime) a[2],
                    (ZonedDateTime) a[3], (List<String>) a[4]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DESCRIPTION);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(p.longValue()), ZoneOffset.UTC);
            } else if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(TimeUtils.dateStringToEpoch(p.text())), ZoneOffset.UTC);
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + START_TIME.getPreferredName() + "]");
        }, START_TIME, ObjectParser.ValueType.VALUE);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(p.longValue()), ZoneOffset.UTC);
            } else if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(TimeUtils.dateStringToEpoch(p.text())), ZoneOffset.UTC);
            }
            throw new IllegalArgumentException(
                    "unexpected token [" + p.currentToken() + "] for [" + END_TIME.getPreferredName() + "]");
        }, END_TIME, ObjectParser.ValueType.VALUE);

        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), JOB_IDS);
        PARSER.declareString((builder, s) -> {}, TYPE);
    }

    public static String documentId(String eventId) {
        return DOCUMENT_ID_PREFIX + eventId;
    }

    private final String id;
    private final String description;
    private final ZonedDateTime startTime;
    private final ZonedDateTime endTime;
    private final Set<String> jobIds;

    public SpecialEvent(String id, String description, ZonedDateTime startTime, ZonedDateTime endTime, List<String> jobIds) {
        this.id = Objects.requireNonNull(id);
        this.description = Objects.requireNonNull(description);
        this.startTime = Objects.requireNonNull(startTime);
        this.endTime = Objects.requireNonNull(endTime);
        this.jobIds = new HashSet<>(jobIds);
    }

    public SpecialEvent(StreamInput in) throws IOException {
        id = in.readString();
        description = in.readString();
        startTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(in.readVLong()), ZoneOffset.UTC);
        endTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(in.readVLong()), ZoneOffset.UTC);
        jobIds = new HashSet<>(Arrays.asList(in.readStringArray()));
    }

    public String getId() {
        return id;
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

    public Set<String> getJobIds() {
        return jobIds;
    }

    public String documentId() {
        return documentId(id);
    }

    /**
     * Convert the special event to a detection rule.
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
        builder.setActions(RuleAction.FILTER_RESULTS, RuleAction.SKIP_SAMPLING);
        builder.setConditionsConnective(Connective.AND);
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(description);
        out.writeVLong(startTime.toInstant().toEpochMilli());
        out.writeVLong(endTime.toInstant().toEpochMilli());
        out.writeStringArray(jobIds.toArray(new String [0]));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        builder.field(DESCRIPTION.getPreferredName(), description);
        builder.dateField(START_TIME.getPreferredName(), START_TIME.getPreferredName() + "_string", startTime.toInstant().toEpochMilli());
        builder.dateField(END_TIME.getPreferredName(), END_TIME.getPreferredName() + "_string", endTime.toInstant().toEpochMilli());
        builder.field(JOB_IDS.getPreferredName(), jobIds);
        if (params.paramAsBoolean(MlMetaIndex.INCLUDE_TYPE_KEY, false)) {
            builder.field(TYPE.getPreferredName(), SPECIAL_EVENT_TYPE);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof SpecialEvent)) {
            return false;
        }

        SpecialEvent other = (SpecialEvent) obj;
        return id.equals(other.id) && description.equals(other.description) && startTime.equals(other.startTime)
                && endTime.equals(other.endTime) && jobIds.equals(other.jobIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, description, startTime, endTime, jobIds);
    }
}
