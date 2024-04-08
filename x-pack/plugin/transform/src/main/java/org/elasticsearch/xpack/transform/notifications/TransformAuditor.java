/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.notifications;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.notifications.TransformAuditMessage;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;

/**
 * TransformAuditor class that abstracts away generic templating for easier injection
 */
public class TransformAuditor extends AbstractAuditor<TransformAuditMessage> {

    private static final Logger logger = LogManager.getLogger(TransformAuditor.class);

    private volatile boolean isResetMode = false;

    private final boolean includeNodeInfo;

    public TransformAuditor(Client client, String nodeName, ClusterService clusterService, boolean includeNodeInfo) {
        super(
            new OriginSettingClient(client, TRANSFORM_ORIGIN),
            TransformInternalIndexConstants.AUDIT_INDEX,
            TransformInternalIndexConstants.AUDIT_INDEX,
            () -> {
                try {
                    return new TransportPutComposableIndexTemplateAction.Request(TransformInternalIndexConstants.AUDIT_INDEX).indexTemplate(
                        ComposableIndexTemplate.builder()
                            .template(TransformInternalIndex.getAuditIndexTemplate())
                            .version((long) TransformConfigVersion.CURRENT.id())
                            .indexPatterns(Collections.singletonList(TransformInternalIndexConstants.AUDIT_INDEX_PATTERN))
                            .priority(Long.MAX_VALUE)
                            .build()
                    );
                } catch (IOException e) {
                    throw new ElasticsearchException("Failure creating transform notification index template request", e);
                }
            },
            nodeName,
            TransformAuditMessage::new,
            clusterService
        );
        clusterService.addListener(event -> {
            if (event.metadataChanged()) {
                boolean oldIsResetMode = isResetMode;
                boolean newIsResetMode = TransformMetadata.getTransformMetadata(event.state()).isResetMode();
                if (oldIsResetMode != newIsResetMode) {
                    logger.debug("TransformAuditor has noticed change of isResetMode bit from {} to {}", oldIsResetMode, newIsResetMode);
                }
                isResetMode = newIsResetMode;
            }
        });
        this.includeNodeInfo = includeNodeInfo;
    }

    public boolean isIncludeNodeInfo() {
        return includeNodeInfo;
    }

    @Override
    protected void indexDoc(ToXContent toXContent) {
        if (isResetMode == false) {
            super.indexDoc(toXContent);
        }
    }

    @Override
    protected void writeBacklog() {
        if (isResetMode) {
            clearBacklog();
        } else {
            super.writeBacklog();
        }
    }
}
