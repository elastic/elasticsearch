/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportResetFeatureStateAction extends TransportMasterNodeAction<ResetFeatureStateRequest, ResetFeatureStateResponse> {

    private final SystemIndices systemIndices;
    private final MetadataDeleteIndexService deleteIndexService;

    @Inject
    public TransportResetFeatureStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        MetadataDeleteIndexService deleteIndexService
    ) {
        super(ResetFeatureStateAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ResetFeatureStateRequest::new, indexNameExpressionResolver, ResetFeatureStateResponse::new,
            ThreadPool.Names.SAME);
        this.systemIndices = systemIndices;
        this.deleteIndexService = deleteIndexService;
    }

    @Override
    protected void masterOperation(
        Task task,
        ResetFeatureStateRequest request,
        ClusterState state,
        ActionListener<ResetFeatureStateResponse> listener
    ) throws Exception {
        // TODO[wrb] - temporary implementation, not how we want to do things.
        Set<Index> concreteSystemIndices = systemIndices.getFeatures().values().stream()
            .flatMap(feature -> feature.getIndexDescriptors().stream())
            .map(SystemIndexDescriptor::getIndexPattern)
            .flatMap(pattern -> Arrays.stream(
                indexNameExpressionResolver.concreteIndices(
                    state,
                    IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN,
                    pattern)))
            .collect(Collectors.toSet());

        Set<Index> concreteAssociatedIndices = systemIndices.getFeatures().values().stream()
            .flatMap(feature -> feature.getAssociatedIndexPatterns().stream())
            .flatMap(pattern -> Arrays.stream(
                indexNameExpressionResolver.concreteIndices(
                    state,
                    IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN,
                    pattern)))
            .collect(Collectors.toSet());

        clusterService.submitStateUpdateTask("reset_feature_states", new ClusterStateUpdateTask(request.masterNodeTimeout()) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return deleteIndexService.deleteIndices(currentState, Sets.union(concreteSystemIndices, concreteAssociatedIndices));
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new ResetFeatureStateResponse());
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(ResetFeatureStateRequest request, ClusterState state) {
        // TODO[wrb] - what if there is a block on a system index?
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }
}
