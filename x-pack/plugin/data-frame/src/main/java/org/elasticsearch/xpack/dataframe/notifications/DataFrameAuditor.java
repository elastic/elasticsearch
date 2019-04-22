/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.notifications;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.Auditor;
import org.elasticsearch.xpack.core.dataframe.notifications.DataFrameAuditMessage;

/**
 * DataFrameAuditor class that abstracts away generic templating for easier injection
 */
public class DataFrameAuditor extends Auditor<DataFrameAuditMessage> {
    public DataFrameAuditor(Client client,
                   String nodeName,
                   String auditIndex,
                   String executionOrigin,
                   AbstractAuditMessage.AbstractBuilder<DataFrameAuditMessage> messageBuilder) {
        super(client, nodeName, auditIndex, executionOrigin, messageBuilder);
    }
}
