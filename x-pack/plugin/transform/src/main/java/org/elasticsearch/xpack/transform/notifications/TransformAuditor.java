/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.notifications;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.notifications.TransformAuditMessage;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;

/**
 * TransformAuditor class that abstracts away generic templating for easier injection
 */
public class TransformAuditor extends AbstractAuditor<TransformAuditMessage> {

    private volatile boolean isResetMode = false;

    public TransformAuditor(Client client, String nodeName, ClusterService clusterService) {
        super(new OriginSettingClient(client, TRANSFORM_ORIGIN), TransformInternalIndexConstants.AUDIT_INDEX,
            TransformInternalIndexConstants.AUDIT_INDEX, null,
            () -> {
                try {
                    IndexTemplateMetadata templateMeta = TransformInternalIndex.getAuditIndexTemplateMetadata();

                    PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateMeta.name())
                        .patterns(templateMeta.patterns())
                        .version(templateMeta.version())
                        .settings(templateMeta.settings())
                        .mapping(templateMeta.mappings().uncompressed(), XContentType.JSON);

                    for (ObjectObjectCursor<String, AliasMetadata> cursor : templateMeta.getAliases()) {
                        AliasMetadata meta = cursor.value;
                        Alias alias = new Alias(meta.alias())
                            .indexRouting(meta.indexRouting())
                            .searchRouting(meta.searchRouting())
                            .isHidden(meta.isHidden())
                            .writeIndex(meta.writeIndex());
                        if (meta.filter() != null) {
                            alias.filter(meta.getFilter().string());
                        }

                        request.alias(alias);
                    }

                    return request;
                } catch (IOException e) {
                    return null;
                }
            },
            () -> null, nodeName, TransformAuditMessage::new, clusterService);
        clusterService.addListener(event -> {
            if (event.metadataChanged()) {
                isResetMode = TransformMetadata.getTransformMetadata(event.state()).isResetMode();
            }
        });
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
