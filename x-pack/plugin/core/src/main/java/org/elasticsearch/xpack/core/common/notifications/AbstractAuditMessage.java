/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class AbstractAuditMessage implements ToXContentObject, Writeable {
    public static final ParseField TYPE = new ParseField("audit_message");

    public static final ParseField MESSAGE = new ParseField("message");
    public static final ParseField LEVEL = new ParseField("level");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField NODE_NAME = new ParseField("node_name");

    private String resourceId;
    private String message;
    private Level level;
    private Date timestamp;
    private String nodeName;

    protected AbstractAuditMessage() {
        this.timestamp = new Date();
    }

    AbstractAuditMessage(String resourceId, String message, Level level, String nodeName) {
        this.resourceId = resourceId;
        this.message = message;
        this.level = level;
        this.timestamp = new Date();
        this.nodeName = nodeName;
    }

    public AbstractAuditMessage(StreamInput in) throws IOException {
        resourceId = in.readOptionalString();
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
        out.writeOptionalString(resourceId);
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

    public final String getResourceId() {
        return resourceId;
    }

    public final void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public final String getMessage() {
        return message;
    }

    public final void setMessage(String message) {
        this.message = message;
    }

    public final Level getLevel() {
        return level;
    }

    public final void setLevel(Level level) {
        this.level = level;
    }

    public final Date getTimestamp() {
        return timestamp;
    }

    public final void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public final String getNodeName() {
        return nodeName;
    }

    public final void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (resourceId != null) {
            builder.field(getResourceField(), resourceId);
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
        return Objects.hash(resourceId, message, level, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof AbstractAuditMessage == false) {
            return false;
        }

        AbstractAuditMessage other = (AbstractAuditMessage) obj;
        return Objects.equals(resourceId, other.resourceId) &&
            Objects.equals(message, other.message) &&
            Objects.equals(level, other.level) &&
            Objects.equals(timestamp, other.timestamp);
    }

    protected abstract String getResourceField();

    public static final class AuditMessageBuilder<T extends AbstractAuditMessage> {

        private final Supplier<T> auditSupplier;

        public AuditMessageBuilder(Supplier<T> auditSupplier) {
            this.auditSupplier = Objects.requireNonNull(auditSupplier);
        }

        public T activity(String resourceId, String message, String nodeName) {
            return newMessage(Level.ACTIVITY, resourceId, message, nodeName);
        }

        public T info(String resourceId, String message, String nodeName) {
            return newMessage(Level.INFO, resourceId, message, nodeName);
        }

        public T warning(String resourceId, String message, String nodeName) {
            return newMessage(Level.WARNING, resourceId, message, nodeName);
        }

        public T error(String resourceId, String message, String nodeName) {
            return newMessage(Level.ERROR, resourceId, message, nodeName);
        }

        private T newMessage(Level level, String resourceId, String message, String nodeName) {
            T audit = auditSupplier.get();
            audit.setLevel(level);
            audit.setResourceId(resourceId);
            audit.setMessage(message);
            audit.setNodeName(nodeName);
            return audit;
        }

    }
}
