/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.transform.notifications;

import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.transform.notifications.TransformAuditMessage;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;

/**
 * TransformAuditor class that abstracts away generic templating for easier injection
 */
public class TransformAuditor extends AbstractAuditor<TransformAuditMessage> {

    public TransformAuditor(Client client, String nodeName) {
        super(client, nodeName, TransformInternalIndexConstants.AUDIT_INDEX, TRANSFORM_ORIGIN, TransformAuditMessage::new);
    }
}
