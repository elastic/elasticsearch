/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.ml.notifications.InferenceAuditMessage;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class InferenceAuditor extends AbstractAuditor<InferenceAuditMessage> {

    public InferenceAuditor(Client client, String nodeName) {
        super(client, nodeName, NotificationsIndex.NOTIFICATIONS_INDEX, ML_ORIGIN, InferenceAuditMessage::new);
    }
}
