/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class AuditMessage implements ToXContentObject, Writeable {
    public static final ParseField TYPE = new ParseField("audit_message");

    public static final ParseField MESSAGE = new ParseField("message");
    public static final ParseField LEVEL = new ParseField("level");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField NODE_NAME = new ParseField("node_name");

    public static final ObjectParser<AuditMessage, Void> PARSER = new ObjectParser<>(TYPE.getPreferredName(), true, AuditMessage::new);

    static {
        PARSER.declareString(AuditMessage::setJobId, Job.ID);
        PARSER.declareString(AuditMessage::setMessage, MESSAGE);
        PARSER.declareField(AuditMessage::setLevel, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Level.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, LEVEL, ValueType.STRING);
        PARSER.declareField(AuditMessage::setTimestamp,
                p -> TimeUtils.parseTimeField(p, TIMESTAMP.getPreferredName()), TIMESTAMP, ValueType.VALUE);
        PARSER.declareString(AuditMessage::setNodeName, NODE_NAME);
    }

    private String jobId;
    private String message;
    private Level level;
    private Date timestamp;
    private String nodeName;

    private AuditMessage() {

    }

     AuditMessage(String jobId, String message, Level level, String nodeName) {
        this.jobId = jobId;
        this.message = message;
        this.level = level;
        timestamp = new Date();
        this.nodeName = nodeName;
    }

    public AuditMessage(StreamInput in) throws IOException {
        jobId = in.readOptionalString();
        message = in.readOptionalString();
        if (in.readBoolean()) {
            level = Level.readFromStream(in);
        }
        if (in.readBoolean()) {
            timestamp = new Date(in.readLong());
        }
        nodeName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(jobId);
        out.writeOptionalString(message);
        boolean hasLevel = level != null;
        out.writeBoolean(hasLevel);
        if (hasLevel) {
            level.writeTo(out);
        }
        boolean hasTimestamp = timestamp != null;
        out.writeBoolean(hasTimestamp);
        if (hasTimestamp) {
            out.writeLong(timestamp.getTime());
        }
        out.writeOptionalString(nodeName);
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Level getLevel() {
        return level;
    }

    public void setLevel(Level level) {
        this.level = level;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public static AuditMessage newInfo(String jobId, String message, String nodeName) {
        return new AuditMessage(jobId, message, Level.INFO, nodeName);
    }

    public static AuditMessage newWarning(String jobId, String message, String nodeName) {
        return new AuditMessage(jobId, message, Level.WARNING, nodeName);
    }

    public static AuditMessage newActivity(String jobId, String message, String nodeName) {
        return new AuditMessage(jobId, message, Level.ACTIVITY, nodeName);
    }

    public static AuditMessage newError(String jobId, String message, String nodeName) {
        return new AuditMessage(jobId, message, Level.ERROR, nodeName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (jobId != null) {
            builder.field(Job.ID.getPreferredName(), jobId);
        }
        if (message != null) {
            builder.field(MESSAGE.getPreferredName(), message);
        }
        if (level != null) {
            builder.field(LEVEL.getPreferredName(), level);
        }
        if (timestamp != null) {
            builder.field(TIMESTAMP.getPreferredName(), timestamp.getTime());
        }
        if (nodeName != null) {
            builder.field(NODE_NAME.getPreferredName(), nodeName);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, message, level, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AuditMessage other = (AuditMessage) obj;
        return Objects.equals(jobId, other.jobId) &&
                Objects.equals(message, other.message) &&
                Objects.equals(level, other.level) &&
                Objects.equals(timestamp, other.timestamp);
    }
}
