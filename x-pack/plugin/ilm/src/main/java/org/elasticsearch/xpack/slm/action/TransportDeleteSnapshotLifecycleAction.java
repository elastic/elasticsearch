/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.action.DeleteSnapshotLifecycleAction;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportDeleteSnapshotLifecycleAction extends
    TransportMasterNodeAction<DeleteSnapshotLifecycleAction.Request, DeleteSnapshotLifecycleAction.Response> {

    @Inject
    public TransportDeleteSnapshotLifecycleAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(DeleteSnapshotLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters,
            DeleteSnapshotLifecycleAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteSnapshotLifecycleAction.Response read(StreamInput in) throws IOException {
        return new DeleteSnapshotLifecycleAction.Response(in);
    }

    @Override
    protected void masterOperation(Task task, DeleteSnapshotLifecycleAction.Request request,
                                   ClusterState state,
                                   ActionListener<DeleteSnapshotLifecycleAction.Response> listener) throws Exception {
        clusterService.submitStateUpdateTask("delete-snapshot-lifecycle-" + request.getLifecycleId(),
            new AckedClusterStateUpdateTask<>(request, listener) {
                @Override
                protected DeleteSnapshotLifecycleAction.Response newResponse(boolean acknowledged) {
                    return new DeleteSnapshotLifecycleAction.Response(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    SnapshotLifecycleMetadata snapMeta = currentState.metadata().custom(SnapshotLifecycleMetadata.TYPE);
                    if (snapMeta == null) {
                        throw new ResourceNotFoundException("snapshot lifecycle policy not found: {}", request.getLifecycleId());
                    }
                    // Check that the policy exists in the first place
                    snapMeta.getSnapshotConfigurations().entrySet().stream()
                        .filter(e -> e.getValue().getPolicy().getId().equals(request.getLifecycleId()))
                        .findAny()
                        .orElseThrow(() -> new ResourceNotFoundException("snapshot lifecycle policy not found: {}",
                            request.getLifecycleId()));

                    Map<String, SnapshotLifecyclePolicyMetadata> newConfigs = snapMeta.getSnapshotConfigurations().entrySet().stream()
                        .filter(e -> e.getKey().equals(request.getLifecycleId()) == false)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    Metadata metadata = currentState.metadata();
                    return ClusterState.builder(currentState)
                        .metadata(Metadata.builder(metadata)
                            .putCustom(SnapshotLifecycleMetadata.TYPE,
                                new SnapshotLifecycleMetadata(newConfigs,
                                    snapMeta.getOperationMode(), snapMeta.getStats().removePolicy(request.getLifecycleId()))))
                        .build();
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteSnapshotLifecycleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
