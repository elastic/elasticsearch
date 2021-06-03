/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessageFactory;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.ml.MlIndexTemplateRegistry;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

abstract class AbstractMlAuditor<T extends AbstractAuditMessage> extends AbstractAuditor<T> {

    private static final Logger logger = LogManager.getLogger(AbstractMlAuditor.class);
    private volatile boolean isResetMode;

    protected AbstractMlAuditor(Client client, AbstractAuditMessageFactory<T> messageFactory, ClusterService clusterService) {
        super(
            new OriginSettingClient(client, ML_ORIGIN),
            NotificationsIndex.NOTIFICATIONS_INDEX,
            MlIndexTemplateRegistry.COMPOSABLE_TEMPLATE_SWITCH_VERSION,
            MlIndexTemplateRegistry.NOTIFICATIONS_LEGACY_TEMPLATE,
            MlIndexTemplateRegistry.NOTIFICATIONS_TEMPLATE,
            clusterService.getNodeName(),
            messageFactory,
            clusterService
        );
        clusterService.addListener(event -> {
            if (event.metadataChanged()) {
                setResetMode(MlMetadata.getMlMetadata(event.state()).isResetMode());
            }
        });
    }

    private void setResetMode(boolean value) {
        isResetMode = value;
    }

    @Override
    protected void indexDoc(ToXContent toXContent) {
        if (isResetMode) {
            logger.trace("Skipped writing the audit message backlog as reset_mode is enabled");
        } else {
            super.indexDoc(toXContent);
        }
    }

    @Override
    protected void writeBacklog() {
        if (isResetMode) {
            logger.trace("Skipped writing the audit message backlog as reset_mode is enabled");
            clearBacklog();
        } else {
            super.writeBacklog();
        }
    }
}
