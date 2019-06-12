/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.utils.TimeUtils;

import java.util.Date;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameAuditMessage extends AbstractAuditMessage {

    private static final ParseField TRANSFORM_ID = new ParseField(DataFrameField.TRANSFORM_ID);
    public static final ConstructingObjectParser<DataFrameAuditMessage, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_audit_message",
        true,
        a -> new DataFrameAuditMessage((String)a[0], (String)a[1], (Level)a[2], (Date)a[3], (String)a[4]));

    static {
        PARSER.declareString(optionalConstructorArg(), TRANSFORM_ID);
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

    public DataFrameAuditMessage(String resourceId, String message, Level level, String nodeName) {
        super(resourceId, message, level, nodeName);
    }

    protected DataFrameAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
        super(resourceId, message, level, timestamp, nodeName);
    }

    @Override
    protected String getResourceField() {
        return TRANSFORM_ID.getPreferredName();
    }

    public static AbstractAuditMessage.AbstractBuilder<DataFrameAuditMessage> builder() {
        return new AbstractBuilder<DataFrameAuditMessage>() {
            @Override
            protected DataFrameAuditMessage newMessage(Level level, String resourceId, String message, String nodeName) {
                return new DataFrameAuditMessage(resourceId, message, level, nodeName);
            }
        };
    }
}
