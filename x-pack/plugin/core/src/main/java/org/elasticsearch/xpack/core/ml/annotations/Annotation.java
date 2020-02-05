/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.annotations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class Annotation implements ToXContentObject, Writeable {

    public static final ParseField ANNOTATION = new ParseField("annotation");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField CREATE_USERNAME = new ParseField("create_username");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField END_TIMESTAMP = new ParseField("end_timestamp");
    public static final ParseField MODIFIED_TIME = new ParseField("modified_time");
    public static final ParseField MODIFIED_USERNAME = new ParseField("modified_username");
    public static final ParseField TYPE = new ParseField("type");

    public static final ObjectParser<Annotation, Void> PARSER = new ObjectParser<>(TYPE.getPreferredName(), true, Annotation::new);

    static {
        PARSER.declareString(Annotation::setAnnotation, ANNOTATION);
        PARSER.declareField(Annotation::setCreateTime,
            p -> TimeUtils.parseTimeField(p, CREATE_TIME.getPreferredName()), CREATE_TIME, ObjectParser.ValueType.VALUE);
        PARSER.declareString(Annotation::setCreateUsername, CREATE_USERNAME);
        PARSER.declareField(Annotation::setTimestamp,
            p -> TimeUtils.parseTimeField(p, TIMESTAMP.getPreferredName()), TIMESTAMP, ObjectParser.ValueType.VALUE);
        PARSER.declareField(Annotation::setEndTimestamp,
            p -> TimeUtils.parseTimeField(p, END_TIMESTAMP.getPreferredName()), END_TIMESTAMP, ObjectParser.ValueType.VALUE);
        PARSER.declareString(Annotation::setJobId, Job.ID);
        PARSER.declareField(Annotation::setModifiedTime,
            p -> TimeUtils.parseTimeField(p, MODIFIED_TIME.getPreferredName()), MODIFIED_TIME, ObjectParser.ValueType.VALUE);
        PARSER.declareString(Annotation::setModifiedUsername, MODIFIED_USERNAME);
        PARSER.declareString(Annotation::setType, TYPE);
    }

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
    private String type;

    private Annotation() {
    }

    public Annotation(String annotation, Date createTime, String createUsername, Date timestamp, Date endTimestamp, String jobId,
                      Date modifiedTime, String modifiedUsername, String type) {
        this.annotation = Objects.requireNonNull(annotation);
        this.createTime = Objects.requireNonNull(createTime);
        this.createUsername = Objects.requireNonNull(createUsername);
        this.timestamp = Objects.requireNonNull(timestamp);
        this.endTimestamp = endTimestamp;
        this.jobId = jobId;
        this.modifiedTime = modifiedTime;
        this.modifiedUsername = modifiedUsername;
        this.type = Objects.requireNonNull(type);
    }

    public Annotation(Annotation other) {
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
        type = in.readString();
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
        out.writeString(type);

    }

    public String getAnnotation() {
        return annotation;
    }

    public void setAnnotation(String annotation) {
        this.annotation = Objects.requireNonNull(annotation);
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = Objects.requireNonNull(createTime);
    }

    public String getCreateUsername() {
        return createUsername;
    }

    public void setCreateUsername(String createUsername) {
        this.createUsername = Objects.requireNonNull(createUsername);
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = Objects.requireNonNull(timestamp);
    }

    public Date getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(Date endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Date getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(Date modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    public String getModifiedUsername() {
        return modifiedUsername;
    }

    public void setModifiedUsername(String modifiedUsername) {
        this.modifiedUsername = modifiedUsername;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = Objects.requireNonNull(type);
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
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(annotation, createTime, createUsername, timestamp, endTimestamp, jobId, modifiedTime, modifiedUsername, type);
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
            Objects.equals(type, other.type);
    }
}
