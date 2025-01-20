/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.ml.notifications.InferenceAuditMessage;

public class InferenceAuditor extends AbstractMlAuditor<InferenceAuditMessage> {

    private final boolean includeNodeInfo;

    public InferenceAuditor(Client client, ClusterService clusterService, boolean includeNodeInfo) {
        super(client, InferenceAuditMessage::new, clusterService);
        this.includeNodeInfo = includeNodeInfo;
    }

    public boolean includeNodeInfo() {
        return includeNodeInfo;
    }
}
