/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.ml.notifications.AnomalyDetectionAuditMessage;
import org.elasticsearch.xpack.ml.MlIndexTemplateRegistry;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class AnomalyDetectionAuditor extends AbstractAuditor<AnomalyDetectionAuditMessage> {

    public AnomalyDetectionAuditor(Client client, ClusterService clusterService) {
        super(new OriginSettingClient(client, ML_ORIGIN), NotificationsIndex.NOTIFICATIONS_INDEX,
            MlIndexTemplateRegistry.NOTIFICATIONS_TEMPLATE,
            clusterService.getNodeName(),
            AnomalyDetectionAuditMessage::new, clusterService);
    }
}
