/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.notifications;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.time.TimeUtils;

import java.util.Date;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class AuditMessage extends AbstractAuditMessage {

    public static final ConstructingObjectParser<AuditMessage, Void> PARSER = new ConstructingObjectParser<>(
        "ml_audit_message",
        true,
        a -> new AuditMessage((String)a[0], (String)a[1], (Level)a[2], (Date)a[3], (String)a[4]));

    static {
        PARSER.declareString(optionalConstructorArg(), Job.ID);
        PARSER.declareString(constructorArg(), MESSAGE);
        PARSER.declareField(constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Level.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, LEVEL, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(),
            p -> TimeUtils.parseTimeField(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
        PARSER.declareString(optionalConstructorArg(), NODE_NAME);
    }

    public AuditMessage(String resourceId, String message, Level level, String nodeName) {
        super(resourceId, message, level, nodeName);
    }

    protected AuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
        super(resourceId, message, level, timestamp, nodeName);
    }

    @Override
    protected String getResourceField() {
        return Job.ID.getPreferredName();
    }

    public static AbstractBuilder<AuditMessage> builder() {
        return new AbstractBuilder<AuditMessage>() {
            @Override
            protected AuditMessage newMessage(Level level, String resourceId, String message, String nodeName) {
                return new AuditMessage(resourceId, message, level, nodeName);
            }
        };
    }
}
