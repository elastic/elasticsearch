/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.annotations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;

public class Annotation implements ToXContentObject, Writeable {

    public enum Type {
        ANNOTATION,
        COMMENT;

        public static Type fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum Event {
        USER,
        DELAYED_DATA,
        MODEL_SNAPSHOT_STORED,
        MODEL_CHANGE,
        CATEGORIZATION_STATUS_CHANGE;

        public static Event fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Result type is needed due to the fact that {@link Annotation} can be returned from C++ as an ML result.
     */
    public static final ParseField RESULTS_FIELD = new ParseField("annotation");

    public static final ParseField ANNOTATION = new ParseField("annotation");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField CREATE_USERNAME = new ParseField("create_username");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField END_TIMESTAMP = new ParseField("end_timestamp");
    public static final ParseField MODIFIED_TIME = new ParseField("modified_time");
    public static final ParseField MODIFIED_USERNAME = new ParseField("modified_username");
    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField EVENT = new ParseField("event");
    public static final ParseField DETECTOR_INDEX = new ParseField("detector_index");
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField OVER_FIELD_NAME = new ParseField("over_field_name");
    public static final ParseField OVER_FIELD_VALUE = new ParseField("over_field_value");
    public static final ParseField BY_FIELD_NAME = new ParseField("by_field_name");
    public static final ParseField BY_FIELD_VALUE = new ParseField("by_field_value");

    /**
     * Parses {@link Annotation} using a strict parser.
     */
    public static Annotation fromXContent(XContentParser parser, Void context) {
        return STRICT_PARSER.apply(parser, context).build();
    }

    /**
     * Strict parser for cases when {@link Annotation} is returned from C++ as an ML result.
     */
    private static final ObjectParser<Builder, Void> STRICT_PARSER =
        new ObjectParser<>(RESULTS_FIELD.getPreferredName(), false, Builder::new);

    static {
        STRICT_PARSER.declareString(Builder::setAnnotation, ANNOTATION);
        STRICT_PARSER.declareField(Builder::setCreateTime,
            p -> TimeUtils.parseTimeField(p, CREATE_TIME.getPreferredName()), CREATE_TIME, ObjectParser.ValueType.VALUE);
        STRICT_PARSER.declareString(Builder::setCreateUsername, CREATE_USERNAME);
        STRICT_PARSER.declareField(Builder::setTimestamp,
            p -> TimeUtils.parseTimeField(p, TIMESTAMP.getPreferredName()), TIMESTAMP, ObjectParser.ValueType.VALUE);
        STRICT_PARSER.declareField(Builder::setEndTimestamp,
            p -> TimeUtils.parseTimeField(p, END_TIMESTAMP.getPreferredName()), END_TIMESTAMP, ObjectParser.ValueType.VALUE);
        STRICT_PARSER.declareString(Builder::setJobId, Job.ID);
        STRICT_PARSER.declareField(Builder::setModifiedTime,
            p -> TimeUtils.parseTimeField(p, MODIFIED_TIME.getPreferredName()), MODIFIED_TIME, ObjectParser.ValueType.VALUE);
        STRICT_PARSER.declareString(Builder::setModifiedUsername, MODIFIED_USERNAME);
        STRICT_PARSER.declareString(Builder::setType, Type::fromString, TYPE);
        STRICT_PARSER.declareString(Builder::setEvent, Event::fromString, EVENT);
        STRICT_PARSER.declareInt(Builder::setDetectorIndex, DETECTOR_INDEX);
        STRICT_PARSER.declareString(Builder::setPartitionFieldName, PARTITION_FIELD_NAME);
        STRICT_PARSER.declareString(Builder::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        STRICT_PARSER.declareString(Builder::setOverFieldName, OVER_FIELD_NAME);
        STRICT_PARSER.declareString(Builder::setOverFieldValue, OVER_FIELD_VALUE);
        STRICT_PARSER.declareString(Builder::setByFieldName, BY_FIELD_NAME);
        STRICT_PARSER.declareString(Builder::setByFieldValue, BY_FIELD_VALUE);
    }

    private final String annotation;
    private final Date createTime;
    private final String createUsername;
    private final Date timestamp;
    private final Date endTimestamp;
    /**
     * Unlike most ML classes, this may be <code>null</code> or wildcarded
     */
    private final String jobId;
    private final Date modifiedTime;
    private final String modifiedUsername;
    private final Type type;
    private final Event event;
    /**
     * Scope-related fields.
     */
    private final Integer detectorIndex;
    private final String partitionFieldName;
    private final String partitionFieldValue;
    private final String overFieldName;
    private final String overFieldValue;
    private final String byFieldName;
    private final String byFieldValue;

    private Annotation(String annotation, Date createTime, String createUsername, Date timestamp, Date endTimestamp, String jobId,
                       Date modifiedTime, String modifiedUsername, Type type, Event event, Integer detectorIndex,
                       String partitionFieldName, String partitionFieldValue, String overFieldName, String overFieldValue,
                       String byFieldName, String byFieldValue) {
        this.annotation = Objects.requireNonNull(annotation);
        this.createTime = Objects.requireNonNull(createTime);
        this.createUsername = Objects.requireNonNull(createUsername);
        this.timestamp = Objects.requireNonNull(timestamp);
        this.endTimestamp = endTimestamp;
        this.jobId = jobId;
        this.modifiedTime = modifiedTime;
        this.modifiedUsername = modifiedUsername;
        this.type = Objects.requireNonNull(type);
        this.event = event;
        this.detectorIndex = detectorIndex;
        this.partitionFieldName = partitionFieldName;
        this.partitionFieldValue = partitionFieldValue;
        this.overFieldName = overFieldName;
        this.overFieldValue = overFieldValue;
        this.byFieldName = byFieldName;
        this.byFieldValue = byFieldValue;
    }

    public Annotation(StreamInput in) throws IOException {
        annotation = in.readString();
        createTime = new Date(in.readLong());
        createUsername = in.readString();
        timestamp = new Date(in.readLong());
        if (in.readBoolean()) {
            endTimestamp = new Date(in.readLong());
        } else {
            endTimestamp = null;
        }
        jobId = in.readOptionalString();
        if (in.readBoolean()) {
            modifiedTime = new Date(in.readLong());
        } else {
            modifiedTime = null;
        }
        modifiedUsername = in.readOptionalString();
        type = Type.fromString(in.readString());
        event = in.readBoolean() ? in.readEnum(Event.class) : null;
        detectorIndex = in.readOptionalInt();
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        overFieldName = in.readOptionalString();
        overFieldValue = in.readOptionalString();
        byFieldName = in.readOptionalString();
        byFieldValue = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(annotation);
        out.writeLong(createTime.getTime());
        out.writeString(createUsername);
        out.writeLong(timestamp.getTime());
        if (endTimestamp != null) {
            out.writeBoolean(true);
            out.writeLong(endTimestamp.getTime());
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(jobId);
        if (modifiedTime != null) {
            out.writeBoolean(true);
            out.writeLong(modifiedTime.getTime());
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(modifiedUsername);
        out.writeString(type.toString());
        if (event != null) {
            out.writeBoolean(true);
            out.writeEnum(event);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalInt(detectorIndex);
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeOptionalString(overFieldName);
        out.writeOptionalString(overFieldValue);
        out.writeOptionalString(byFieldName);
        out.writeOptionalString(byFieldValue);
    }

    public String getAnnotation() {
        return annotation;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public String getCreateUsername() {
        return createUsername;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Date getEndTimestamp() {
        return endTimestamp;
    }

    public String getJobId() {
        return jobId;
    }

    public Date getModifiedTime() {
        return modifiedTime;
    }

    public String getModifiedUsername() {
        return modifiedUsername;
    }

    public Type getType() {
        return type;
    }

    public Event getEvent() {
        return event;
    }

    public Integer getDetectorIndex() {
        return detectorIndex;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
    }

    public String getOverFieldName() {
        return overFieldName;
    }

    public String getOverFieldValue() {
        return overFieldValue;
    }

    public String getByFieldName() {
        return byFieldName;
    }

    public String getByFieldValue() {
        return byFieldValue;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ANNOTATION.getPreferredName(), annotation);
        builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.getTime());
        builder.field(CREATE_USERNAME.getPreferredName(), createUsername);
        builder.timeField(TIMESTAMP.getPreferredName(), TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        if (endTimestamp != null) {
            builder.timeField(END_TIMESTAMP.getPreferredName(), END_TIMESTAMP.getPreferredName() + "_string", endTimestamp.getTime());
        }
        if (jobId != null) {
            builder.field(Job.ID.getPreferredName(), jobId);
        }
        if (modifiedTime != null) {
            builder.timeField(MODIFIED_TIME.getPreferredName(), MODIFIED_TIME.getPreferredName() + "_string", modifiedTime.getTime());
        }
        if (modifiedUsername != null) {
            builder.field(MODIFIED_USERNAME.getPreferredName(), modifiedUsername);
        }
        builder.field(TYPE.getPreferredName(), type);
        if (event != null) {
            builder.field(EVENT.getPreferredName(), event);
        }
        if (detectorIndex != null) {
            builder.field(DETECTOR_INDEX.getPreferredName(), detectorIndex);
        }
        if (partitionFieldName != null) {
            builder.field(PARTITION_FIELD_NAME.getPreferredName(), partitionFieldName);
        }
        if (partitionFieldValue != null) {
            builder.field(PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
        }
        if (overFieldName != null) {
            builder.field(OVER_FIELD_NAME.getPreferredName(), overFieldName);
        }
        if (overFieldValue != null) {
            builder.field(OVER_FIELD_VALUE.getPreferredName(), overFieldValue);
        }
        if (byFieldName != null) {
            builder.field(BY_FIELD_NAME.getPreferredName(), byFieldName);
        }
        if (byFieldValue != null) {
            builder.field(BY_FIELD_VALUE.getPreferredName(), byFieldValue);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            annotation, createTime, createUsername, timestamp, endTimestamp, jobId, modifiedTime, modifiedUsername, type, event,
            detectorIndex, partitionFieldName, partitionFieldValue, overFieldName, overFieldValue, byFieldName, byFieldValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Annotation other = (Annotation) obj;
        return Objects.equals(annotation, other.annotation) &&
            Objects.equals(createTime, other.createTime) &&
            Objects.equals(createUsername, other.createUsername) &&
            Objects.equals(timestamp, other.timestamp) &&
            Objects.equals(endTimestamp, other.endTimestamp) &&
            Objects.equals(jobId, other.jobId) &&
            Objects.equals(modifiedTime, other.modifiedTime) &&
            Objects.equals(modifiedUsername, other.modifiedUsername) &&
            Objects.equals(type, other.type) &&
            Objects.equals(event, other.event) &&
            Objects.equals(detectorIndex, other.detectorIndex) &&
            Objects.equals(partitionFieldName, other.partitionFieldName) &&
            Objects.equals(partitionFieldValue, other.partitionFieldValue) &&
            Objects.equals(overFieldName, other.overFieldName) &&
            Objects.equals(overFieldValue, other.overFieldValue) &&
            Objects.equals(byFieldName, other.byFieldName) &&
            Objects.equals(byFieldValue, other.byFieldValue);
    }


    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private String annotation;
        private Date createTime;
        private String createUsername;
        private Date timestamp;
        private Date endTimestamp;
        /**
         * Unlike most ML classes, this may be <code>null</code> or wildcarded
         */
        private String jobId;
        private Date modifiedTime;
        private String modifiedUsername;
        private Type type;
        private Event event;
        private Integer detectorIndex;
        private String partitionFieldName;
        private String partitionFieldValue;
        private String overFieldName;
        private String overFieldValue;
        private String byFieldName;
        private String byFieldValue;

        public Builder() {}

        public Builder(Annotation other) {
            Objects.requireNonNull(other);
            this.annotation = other.annotation;
            this.createTime = new Date(other.createTime.getTime());
            this.createUsername = other.createUsername;
            this.timestamp = new Date(other.timestamp.getTime());
            this.endTimestamp = other.endTimestamp == null ? null : new Date(other.endTimestamp.getTime());
            this.jobId = other.jobId;
            this.modifiedTime = other.modifiedTime == null ? null : new Date(other.modifiedTime.getTime());
            this.modifiedUsername = other.modifiedUsername;
            this.type = other.type;
            this.event = other.event;
            this.detectorIndex = other.detectorIndex;
            this.partitionFieldName = other.partitionFieldName;
            this.partitionFieldValue = other.partitionFieldValue;
            this.overFieldName = other.overFieldName;
            this.overFieldValue = other.overFieldValue;
            this.byFieldName = other.byFieldName;
            this.byFieldValue = other.byFieldValue;
        }

        public Builder setAnnotation(String annotation) {
            this.annotation = Objects.requireNonNull(annotation);
            return this;
        }

        public Builder setCreateTime(Date createTime) {
            this.createTime = Objects.requireNonNull(createTime);
            return this;
        }

        public Builder setCreateUsername(String createUsername) {
            this.createUsername = Objects.requireNonNull(createUsername);
            return this;
        }

        public Builder setTimestamp(Date timestamp) {
            this.timestamp = Objects.requireNonNull(timestamp);
            return this;
        }

        public Builder setEndTimestamp(Date endTimestamp) {
            this.endTimestamp = endTimestamp;
            return this;
        }

        public Builder setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setModifiedTime(Date modifiedTime) {
            this.modifiedTime = modifiedTime;
            return this;
        }

        public Builder setModifiedUsername(String modifiedUsername) {
            this.modifiedUsername = modifiedUsername;
            return this;
        }

        public Builder setType(Type type) {
            this.type = Objects.requireNonNull(type);
            return this;
        }

        public Builder setEvent(Event event) {
            this.event = event;
            return this;
        }

        public Builder setDetectorIndex(Integer index) {
            this.detectorIndex = index;
            return this;
        }

        public Builder setPartitionFieldName(String name) {
            this.partitionFieldName = name;
            return this;
        }

        public Builder setPartitionFieldValue(String value) {
            this.partitionFieldValue = value;
            return this;
        }

        public Builder setOverFieldName(String name) {
            this.overFieldName = name;
            return this;
        }

        public Builder setOverFieldValue(String value) {
            this.overFieldValue = value;
            return this;
        }

        public Builder setByFieldName(String name) {
            this.byFieldName = name;
            return this;
        }

        public Builder setByFieldValue(String value) {
            this.byFieldValue = value;
            return this;
        }

        public Annotation build() {
            return new Annotation(
                annotation, createTime, createUsername, timestamp, endTimestamp, jobId, modifiedTime, modifiedUsername, type, event,
                detectorIndex, partitionFieldName, partitionFieldValue, overFieldName, overFieldValue, byFieldName, byFieldValue);
        }
    }
}
