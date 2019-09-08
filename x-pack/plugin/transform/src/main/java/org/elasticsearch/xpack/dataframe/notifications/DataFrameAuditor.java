/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.notifications;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.dataframe.notifications.DataFrameAuditMessage;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;

import static org.elasticsearch.xpack.core.ClientHelper.DATA_FRAME_ORIGIN;

/**
 * DataFrameAuditor class that abstracts away generic templating for easier injection
 */
public class DataFrameAuditor extends AbstractAuditor<DataFrameAuditMessage> {

    public DataFrameAuditor(Client client, String nodeName) {
        super(client, nodeName, DataFrameInternalIndex.AUDIT_INDEX, DATA_FRAME_ORIGIN, DataFrameAuditMessage::new);
    }
}
