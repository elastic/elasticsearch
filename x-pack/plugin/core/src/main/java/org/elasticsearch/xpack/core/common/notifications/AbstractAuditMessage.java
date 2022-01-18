/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public abstract class AbstractAuditMessage implements ToXContentObject {

    public static final ParseField MESSAGE = new ParseField("message");
    public static final ParseField LEVEL = new ParseField("level");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField NODE_NAME = new ParseField("node_name");
    public static final ParseField JOB_TYPE = new ParseField("job_type");

    private static final String TRUNCATED_SUFFIX = "... (truncated)";
    /**
     * The max length of an audit message in characters is 32766 / 4 = 8191
     * where 32766 is the limit in bytes Lucene sets for a term field
     * and 4 is the max number of bytes required to represent a UTF8 character.
     */
    public static final int MAX_AUDIT_MESSAGE_CHARS = 8191;

    protected static final <T extends AbstractAuditMessage> ConstructingObjectParser<T, Void> createParser(
        String name,
        AbstractAuditMessageFactory<T> messageFactory,
        ParseField resourceField
    ) {

        ConstructingObjectParser<T, Void> PARSER = new ConstructingObjectParser<>(
            name,
            true,
            a -> messageFactory.newMessage((String) a[0], (String) a[1], (Level) a[2], (Date) a[3], (String) a[4])
        );

        PARSER.declareString(optionalConstructorArg(), resourceField);
        PARSER.declareString(constructorArg(), MESSAGE);
        PARSER.declareString(constructorArg(), Level::fromString, LEVEL);
        PARSER.declareField(
            constructorArg(),
            p -> TimeUtils.parseTimeField(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE
        );
        PARSER.declareString(optionalConstructorArg(), NODE_NAME);

        return PARSER;
    }

    private final String resourceId;
    private final String message;
    private final Level level;
    private final Date timestamp;
    private final String nodeName;

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

        if (message.length() > MAX_AUDIT_MESSAGE_CHARS) {
            assert message.length() > MAX_AUDIT_MESSAGE_CHARS : "Audit message is unexpectedly large";
            builder.field(MESSAGE.getPreferredName(), truncateMessage(message, MAX_AUDIT_MESSAGE_CHARS));
        } else {
            builder.field(MESSAGE.getPreferredName(), message);
        }
        builder.field(LEVEL.getPreferredName(), level);
        builder.field(TIMESTAMP.getPreferredName(), timestamp.getTime());
        if (nodeName != null) {
            builder.field(NODE_NAME.getPreferredName(), nodeName);
        }
        String jobType = getJobType();
        if (jobType != null) {
            builder.field(JOB_TYPE.getPreferredName(), jobType);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resourceId, message, level, timestamp, nodeName, getJobType());
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
        return Objects.equals(resourceId, other.resourceId)
            && Objects.equals(message, other.message)
            && Objects.equals(level, other.level)
            && Objects.equals(timestamp, other.timestamp)
            && Objects.equals(nodeName, other.nodeName)
            && Objects.equals(getJobType(), other.getJobType());
    }

    /**
     * @return job type string used to tell apart jobs of different types stored in the same index
     */
    public abstract String getJobType();

    /**
     * @return resource id field name used when storing a new message
     */
    protected abstract String getResourceField();

    /**
     * Truncate the message and append {@value #TRUNCATED_SUFFIX} so
     * that the resulting string does not exceed {@code maxLength} characters
     *
     * {@code message} must be at least {@code maxLength} long
     *
     * @param message The message to truncate. Must have length of at least maxLength
     * @param maxLength The length to truncate to
     * @return The truncated string ending int {@value #TRUNCATED_SUFFIX}
     */
    static String truncateMessage(String message, int maxLength) {
        StringBuilder sb = new StringBuilder(maxLength);
        sb.append(message, 0, maxLength - TRUNCATED_SUFFIX.length());
        int lastWhitespace = sb.lastIndexOf(" ");
        if (lastWhitespace < 0) {
            // no space char
            lastWhitespace = maxLength - TRUNCATED_SUFFIX.length();
        } else {
            lastWhitespace++; // point to next char which is a non-space char
        }
        sb.replace(lastWhitespace, lastWhitespace + TRUNCATED_SUFFIX.length(), TRUNCATED_SUFFIX);
        sb.delete(lastWhitespace + TRUNCATED_SUFFIX.length(), sb.length());
        return sb.toString();
    }
}
