/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.transform.notifications;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.transform.notifications.TransformAuditMessage;
import org.elasticsearch.xpack.transform.persistence.DataFrameInternalIndex;

import static org.elasticsearch.xpack.core.ClientHelper.DATA_FRAME_ORIGIN;

/**
 * DataFrameAuditor class that abstracts away generic templating for easier injection
 */
public class DataFrameAuditor extends AbstractAuditor<TransformAuditMessage> {

    public DataFrameAuditor(Client client, String nodeName) {
        super(client, nodeName, DataFrameInternalIndex.AUDIT_INDEX, DATA_FRAME_ORIGIN, TransformAuditMessage::new);
    }
}
