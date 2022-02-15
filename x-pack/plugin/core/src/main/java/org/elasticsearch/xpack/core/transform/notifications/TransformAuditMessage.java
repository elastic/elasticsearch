/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.notifications;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.util.Date;

public class TransformAuditMessage extends AbstractAuditMessage {

    private static final ParseField TRANSFORM_ID = new ParseField(TransformField.TRANSFORM_ID);
    public static final ConstructingObjectParser<TransformAuditMessage, Void> PARSER = createParser(
        "data_frame_audit_message",
        TransformAuditMessage::new,
        TRANSFORM_ID
    );

    public TransformAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
        super(resourceId, message, level, timestamp, nodeName);
    }

    @Override
    public final String getJobType() {
        return null;
    }

    @Override
    protected String getResourceField() {
        return TRANSFORM_ID.getPreferredName();
    }
}
