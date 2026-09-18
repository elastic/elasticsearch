/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.tasks.Task;

/**
 * Emits a Warning response header and log message when a snapshot is requested and the cluster
 * contains data source credentials encrypted under the project encryption key (PEK). Those
 * credentials are excluded from snapshots until full snapshot/restore support for encrypted
 * credentials is available, and must be reconfigured after a restore.
 */
class SnapshotEncryptedDataWarningFilter implements MappedActionFilter {

    static final String WARNING_MESSAGE =
        "Encrypted data source credentials cannot be included in this snapshot and must be reconfigured after restore.";

    private static final Logger logger = LogManager.getLogger(SnapshotEncryptedDataWarningFilter.class);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final EncryptedDataHandlerRegistry handlerRegistry;

    SnapshotEncryptedDataWarningFilter(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        EncryptedDataHandlerRegistry handlerRegistry
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.handlerRegistry = handlerRegistry;
    }

    @Override
    public String actionName() {
        return TransportCreateSnapshotAction.TYPE.name();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (((CreateSnapshotRequest) request).includeGlobalState()) {
            warnIfEncryptedDataPresent();
        }
        chain.proceed(task, action, request, listener);
    }

    private void warnIfEncryptedDataPresent() {
        var projectMetadata = projectResolver.getProjectState(clusterService.state()).metadata();
        boolean hasEncryptedData = handlerRegistry.handlers().stream().anyMatch(h -> projectMetadata.custom(h.customName()) != null);
        if (hasEncryptedData) {
            logger.warn(WARNING_MESSAGE);
            HeaderWarning.addWarning(WARNING_MESSAGE);
        }
    }
}
