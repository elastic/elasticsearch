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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;

import java.util.function.Supplier;

/**
 * Emits a Warning response header and log message when a snapshot is requested and the cluster
 * contains data encrypted under the project encryption key (PEK) that the snapshot will exclude:
 * all of it while the snapshot encryption feature flag is disabled, otherwise the data of customs
 * not written into snapshots, plus everything else when no {@code encrypted_data} is supplied.
 *
 * <p>Rejects a snapshot with {@code encrypted_data} up front if that data cannot be decrypted for re-wrapping because the encryption
 * service is not {@link EncryptionServiceState#READY}, rather than failing at finalization after all shards have been copied.
 */
class SnapshotEncryptedDataFilter implements MappedActionFilter {

    static final String WARNING_MESSAGE =
        "Encrypted credentials cannot be included in this snapshot and must be reconfigured after restore.";

    private static final Logger logger = LogManager.getLogger(SnapshotEncryptedDataFilter.class);

    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final EncryptedDataHandlerRegistry handlerRegistry;
    private final Supplier<EncryptionServiceState> encryptionServiceState;

    SnapshotEncryptedDataFilter(
        ClusterService clusterService,
        ProjectResolver projectResolver,
        EncryptedDataHandlerRegistry handlerRegistry,
        Supplier<EncryptionServiceState> encryptionServiceState
    ) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.handlerRegistry = handlerRegistry;
        this.encryptionServiceState = encryptionServiceState;
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
        final CreateSnapshotRequest createSnapshotRequest = (CreateSnapshotRequest) request;
        if (createSnapshotRequest.includeGlobalState()) {
            if (createSnapshotRequest.encryptedData() != null) {
                final EncryptionServiceState state = encryptionServiceState.get();
                if (state != EncryptionServiceState.READY && hasIncludableData()) {
                    listener.onFailure(new EncryptionServiceUnavailableException(state));
                    return;
                }
            }
            warnIfEncryptedDataExcluded(createSnapshotRequest.encryptedData() != null);
        }
        chain.proceed(task, action, request, listener);
    }

    private boolean hasIncludableData() {
        var projectMetadata = projectResolver.getProjectState(clusterService.state()).metadata();
        return handlerRegistry.handlers()
            .stream()
            .anyMatch(h -> EncryptedDataHandlerRegistry.hasData(h, projectMetadata) && includable(h, projectMetadata));
    }

    private void warnIfEncryptedDataExcluded(boolean encryptedDataSupplied) {
        try {
            var projectMetadata = projectResolver.getProjectState(clusterService.state()).metadata();
            boolean excluded = handlerRegistry.handlers()
                .stream()
                .anyMatch(
                    h -> EncryptedDataHandlerRegistry.hasData(h, projectMetadata)
                        && (encryptedDataSupplied == false || includable(h, projectMetadata) == false)
                );
            if (excluded) {
                logger.warn(WARNING_MESSAGE);
                HeaderWarning.addWarning(WARNING_MESSAGE);
            }
        } catch (Exception e) {
            logger.warn("Could not check for encrypted data before snapshot", e);
        }
    }

    private static boolean includable(EncryptedDataHandler<?> handler, ProjectMetadata projectMetadata) {
        return SnapshotEncryptedData.FEATURE_FLAG.isEnabled() && EncryptedDataHandlerRegistry.writtenToSnapshot(handler, projectMetadata);
    }
}
