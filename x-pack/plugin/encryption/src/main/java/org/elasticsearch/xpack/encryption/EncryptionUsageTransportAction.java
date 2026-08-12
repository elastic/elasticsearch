/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.encryption.EncryptionFeatureSetUsage;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

public class EncryptionUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final ProjectResolver projectResolver;

    @Inject
    public EncryptionUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(XPackUsageFeatureAction.ENCRYPTION.name(), transportService, clusterService, threadPool, actionFilters);
        this.projectResolver = projectResolver;
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final boolean enabled = EncryptionServiceRegistry.getEncryptionService().isEncryptionRequired();
        final boolean hasEncryptedData = clusterHasEncryptedData(projectResolver.getProjectMetadata(state));
        listener.onResponse(new XPackUsageFeatureResponse(new EncryptionFeatureSetUsage(enabled, hasEncryptedData)));
    }

    /**
     * A handler's custom holds encrypted data iff re-encrypting with the clearing function would change it (see the
     * identity contract on {@link EncryptedDataHandler#reEncrypt}); the probe result is discarded.
     */
    @SuppressWarnings("unchecked")
    static boolean clusterHasEncryptedData(ProjectMetadata project) {
        for (EncryptedDataHandler<?> rawHandler : EncryptedDataHandlerRegistry.getInstance().handlers()) {
            final EncryptedDataHandler<Metadata.ProjectCustom> handler = (EncryptedDataHandler<Metadata.ProjectCustom>) rawHandler;
            final Metadata.ProjectCustom custom = project.custom(handler.customName());
            if (custom != null && handler.reEncrypt(custom, existing -> null) != custom) {
                return true;
            }
        }
        return false;
    }
}
