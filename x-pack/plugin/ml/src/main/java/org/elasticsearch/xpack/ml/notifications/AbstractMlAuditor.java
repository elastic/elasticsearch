/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessageFactory;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.ml.MlIndexTemplateRegistry;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

abstract class AbstractMlAuditor<T extends AbstractAuditMessage> extends AbstractAuditor<T> {

    private static final Logger logger = LogManager.getLogger(AbstractMlAuditor.class);

    protected AbstractMlAuditor(
        Client client,
        AbstractAuditMessageFactory<T> messageFactory,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            new OriginSettingClient(client, ML_ORIGIN),
            NotificationsIndex.NOTIFICATIONS_INDEX_WRITE_ALIAS,
            clusterService.getNodeName(),
            messageFactory,
            clusterService,
            indexNameExpressionResolver,
            clusterService.threadPool().generic()
        );
    }

    @Override
    protected TransportPutComposableIndexTemplateAction.Request putTemplateRequest() {
        var templateConfig = MlIndexTemplateRegistry.NOTIFICATIONS_TEMPLATE;
        try {
            return new TransportPutComposableIndexTemplateAction.Request(templateConfig.getTemplateName()).indexTemplate(
                templateConfig.load(ComposableIndexTemplate::parse)
            ).masterNodeTimeout(MASTER_TIMEOUT);
        } catch (IOException e) {
            throw new ElasticsearchParseException("unable to parse composable template " + templateConfig.getTemplateName(), e);
        }
    }

    protected int templateVersion() {
        return MlIndexTemplateRegistry.NOTIFICATIONS_TEMPLATE.getVersion();
    }

    protected IndexDetails indexDetails() {
        return new IndexDetails(NotificationsIndex.NOTIFICATIONS_INDEX_PREFIX, NotificationsIndex.NOTIFICATIONS_INDEX_VERSION);
    }
}
