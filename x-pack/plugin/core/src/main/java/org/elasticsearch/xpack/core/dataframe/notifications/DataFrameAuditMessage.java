/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.ml.utils.time.TimeUtils;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.xpack.core.dataframe.DataFrameField.DATA_FRAME_TRANSFORM_AUDIT_ID_FIELD;

public class DataFrameAuditMessage extends AbstractAuditMessage {

    private static final ParseField TRANSFORM_ID = new ParseField(DATA_FRAME_TRANSFORM_AUDIT_ID_FIELD);
    public static final ObjectParser<DataFrameAuditMessage, Void> PARSER = new ObjectParser<>("data_frame_audit_message",
        true,
        DataFrameAuditMessage::new);

    static {
        PARSER.declareString(AbstractAuditMessage::setResourceId, TRANSFORM_ID);
        PARSER.declareString(AbstractAuditMessage::setMessage, MESSAGE);
        PARSER.declareField(AbstractAuditMessage::setLevel, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return Level.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, LEVEL, ObjectParser.ValueType.STRING);
        PARSER.declareField(AbstractAuditMessage::setTimestamp, parser -> {
            if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return new Date(parser.longValue());
            } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new Date(TimeUtils.dateStringToEpoch(parser.text()));
            }
            throw new IllegalArgumentException(
                "unexpected token [" + parser.currentToken() + "] for [" + TIMESTAMP.getPreferredName() + "]");
        }, TIMESTAMP, ObjectParser.ValueType.VALUE);
        PARSER.declareString(AbstractAuditMessage::setNodeName, NODE_NAME);
    }

    public DataFrameAuditMessage(StreamInput in) throws IOException {
        super(in);
    }

    public DataFrameAuditMessage(){
        super();
    }

    @Override
    protected String getResourceField() {
        return TRANSFORM_ID.getPreferredName();
    }

    public static AbstractAuditMessage.AuditMessageBuilder<DataFrameAuditMessage> messageBuilder() {
        return new AuditMessageBuilder<>(DataFrameAuditMessage::new);
    }
}
