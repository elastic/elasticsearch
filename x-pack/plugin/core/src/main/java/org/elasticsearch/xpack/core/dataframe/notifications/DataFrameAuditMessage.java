/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.notifications;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Level;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.util.Date;

public class DataFrameAuditMessage extends AbstractAuditMessage {

    private static final ParseField TRANSFORM_ID = new ParseField(DataFrameField.TRANSFORM_ID);
    public static final ConstructingObjectParser<DataFrameAuditMessage, Void> PARSER =
        createParser("data_frame_audit_message", DataFrameAuditMessage::new, TRANSFORM_ID);

    public DataFrameAuditMessage(String resourceId, String message, Level level, Date timestamp, String nodeName) {
        super(resourceId, message, level, timestamp, nodeName);
    }

    @Override
    protected String getResourceField() {
        return TRANSFORM_ID.getPreferredName();
    }
}
