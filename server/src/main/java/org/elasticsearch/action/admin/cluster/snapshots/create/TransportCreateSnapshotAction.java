/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.snapshots.SnapshotGlobalStateTransformer;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Transport action for create snapshot operation
 */
public class TransportCreateSnapshotAction extends TransportMasterNodeAction<CreateSnapshotRequest, CreateSnapshotResponse> {
    public static final ActionType<CreateSnapshotResponse> TYPE = new ActionType<>("cluster:admin/snapshot/create");
    private final SnapshotsService snapshotsService;
    private final ProjectResolver projectResolver;
    private List<SnapshotGlobalStateTransformer> snapshotGlobalStateTransformers = List.of();

    @Inject
    public TransportCreateSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SnapshotsService snapshotsService,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
    ) {
        super(
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateSnapshotRequest::new,
            CreateSnapshotResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.snapshotsService = snapshotsService;
        this.projectResolver = projectResolver;
    }

    public void setSnapshotGlobalStateTransformers(List<SnapshotGlobalStateTransformer> transformers) {
        this.snapshotGlobalStateTransformers = List.copyOf(transformers);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSnapshotRequest request, ClusterState state) {
        // We only check metadata block, as we want to snapshot closed indices (which have a read block)
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(
        Task task,
        final CreateSnapshotRequest request,
        ClusterState state,
        final ActionListener<CreateSnapshotResponse> listener
    ) {
        final ProjectId projectId = projectResolver.getProjectId();
        final Metadata metadata = state.metadata();

        if (SnapshotEncryptedData.FEATURE_FLAG.isEnabled() && request.encryptedData() == null) {
            // No encrypted_data supplied — check whether the cluster has encrypted data and warn or reject.
            if (request.includeGlobalState()) {
                boolean hasEncryptedData = false;
                for (SnapshotGlobalStateTransformer transformer : snapshotGlobalStateTransformers) {
                    if (transformer.containsEncryptedData(projectId, metadata)) {
                        hasEncryptedData = true;
                        break;
                    }
                }
                if (hasEncryptedData) {
                    if (snapshotsService.isEncryptedDataRequired()) {
                        listener.onFailure(
                            new IllegalArgumentException(
                                "Snapshot request rejected: the cluster contains encrypted project data and "
                                    + "[snapshot.encrypted_data.required] is true. "
                                    + "Supply [encrypted_data] in the request to include the data in the snapshot."
                            )
                        );
                        return;
                    }
                    HeaderWarning.addWarning(
                        "This snapshot will not include encrypted project data because no [encrypted_data] was supplied. "
                            + "The data can only be restored if it is re-configured manually after restore."
                    );
                }
            }
        }

        if (request.waitForCompletion()) {
            snapshotsService.executeSnapshot(projectId, request, listener.map(CreateSnapshotResponse::new));
        } else {
            snapshotsService.createSnapshot(projectId, request, listener.map(snapshot -> new CreateSnapshotResponse((SnapshotInfo) null)));
        }
    }
}
