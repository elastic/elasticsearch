/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public abstract class AbstractAuditMessage implements ToXContentObject {
    public static final ParseField TYPE = new ParseField("audit_message");

    public static final ParseField MESSAGE = new ParseField("message");
    public static final ParseField LEVEL = new ParseField("level");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField NODE_NAME = new ParseField("node_name");

    private final String resourceId;
    private final String message;
    private final Level level;
    private final Date timestamp;
    private final String nodeName;

    public AbstractAuditMessage(String resourceId, String message, Level level, String nodeName) {
        this.resourceId = resourceId;
        this.message = Objects.requireNonNull(message);
        this.level = Objects.requireNonNull(level);
        this.timestamp = new Date();
        this.nodeName = nodeName;
    }

    protected AbstractAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
        this.resourceId = resourceId;
        this.message = Objects.requireNonNull(message);
        this.level = Objects.requireNonNull(level);
        this.timestamp = Objects.requireNonNull(timestamp);
        this.nodeName = nodeName;
    }

    public final String getResourceId() {
        return resourceId;
    }

    public final String getMessage() {
        return message;
    }

    public final Level getLevel() {
        return level;
    }

    public final Date getTimestamp() {
        return timestamp;
    }

    public final String getNodeName() {
        return nodeName;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (resourceId != null) {
            builder.field(getResourceField(), resourceId);
        }
        builder.field(MESSAGE.getPreferredName(), message);
        builder.field(LEVEL.getPreferredName(), level);
        builder.field(TIMESTAMP.getPreferredName(), timestamp.getTime());
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

    public abstract static class AbstractBuilder<T extends AbstractAuditMessage> {

        public T info(String resourceId, String message, String nodeName) {
            return newMessage(Level.INFO, resourceId, message, nodeName);
        }

        public T warning(String resourceId, String message, String nodeName) {
            return newMessage(Level.WARNING, resourceId, message, nodeName);
        }

        public T error(String resourceId, String message, String nodeName) {
            return newMessage(Level.ERROR, resourceId, message, nodeName);
        }

        protected abstract T newMessage(Level level, String resourceId, String message, String nodeName);
    }
}
