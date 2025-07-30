/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.ml.notifications.AnomalyDetectionAuditMessage;

public class AnomalyDetectionAuditor extends AbstractMlAuditor<AnomalyDetectionAuditMessage> {

    private final boolean includeNodeInfo;

    public AnomalyDetectionAuditor(
        Client client,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        boolean includeNodeInfo
    ) {
        super(client, AnomalyDetectionAuditMessage::new, clusterService, indexNameExpressionResolver);
        this.includeNodeInfo = includeNodeInfo;
    }

    public boolean includeNodeInfo() {
        return this.includeNodeInfo;
    }
}
