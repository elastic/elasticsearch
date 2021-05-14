/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction.Request;

import java.util.List;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_NAME_SETTING;

public class TransportDeleteLifecycleAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    @Inject
    public TransportDeleteLifecycleAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(DeleteLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters,
            Request::new, indexNameExpressionResolver, AcknowledgedResponse::readFrom, ThreadPool.Names.SAME);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete-lifecycle-" + request.getPolicyName(),
                new AckedClusterStateUpdateTask(request, listener) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        Spliterator<ObjectCursor<IndexMetadata>> indicesIt = currentState.metadata().indices().values().spliterator();
                        String policyToDelete = request.getPolicyName();
                        List<String> indicesUsingPolicy = StreamSupport.stream(indicesIt, false)
                            .map(idxMeta -> idxMeta.value)
                            .filter((idxMeta) -> LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()).equals(policyToDelete))
                            .map((idxMeta) -> idxMeta.getIndex().getName())
                            .collect(Collectors.toList());
                        if (indicesUsingPolicy.isEmpty() == false) {
                            throw new IllegalArgumentException("Cannot delete policy [" + request.getPolicyName()
                                + "]. It is in use by one or more indices: " + indicesUsingPolicy);
                        }
                        ClusterState.Builder newState = ClusterState.builder(currentState);
                        IndexLifecycleMetadata currentMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE);
                        if (currentMetadata == null
                                || currentMetadata.getPolicyMetadatas().containsKey(request.getPolicyName()) == false) {
                            throw new ResourceNotFoundException("Lifecycle policy not found: {}", request.getPolicyName());
                        }
                        SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());
                        newPolicies.remove(request.getPolicyName());
                        IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentMetadata.getOperationMode());
                        newState.metadata(Metadata.builder(currentState.getMetadata())
                                .putCustom(IndexLifecycleMetadata.TYPE, newMetadata).build());
                        return newState.build();
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
