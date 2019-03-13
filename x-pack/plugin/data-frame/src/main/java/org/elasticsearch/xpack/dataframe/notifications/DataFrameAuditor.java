/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.dataframe.notifications.DataFrameAuditMessage;

import static org.elasticsearch.xpack.core.ClientHelper.DATA_FRAME_ORIGIN;
import static org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex.AUDIT_INDEX;

public class DataFrameAuditor extends AbstractAuditor<DataFrameAuditMessage> {

    private static final Logger logger = LogManager.getLogger(DataFrameAuditor.class);

    public DataFrameAuditor(Client client, String nodeName){
        super(client, nodeName, DataFrameAuditMessage.messageBuilder());
    }

    @Override
    protected String getExecutionOrigin() {
        return DATA_FRAME_ORIGIN;
    }

    @Override
    protected String getAuditIndex() {
        return AUDIT_INDEX;
    }

    @Override
    protected void onIndexResponse(IndexResponse response) {
        logger.trace("Successfully wrote audit message");
    }

    @Override
    protected void onIndexFailure(Exception exception) {
        logger.debug("Failed to write audit message", exception);
    }
}
