/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                ClusterState state = currentState;
                for (SystemIndices.Feature feature : systemIndices.getFeatures().values()) {

                    ClusterState intermediateState = state;
                    Set<Index> concreteSystemIndices = Stream.concat(
                        feature.getIndexDescriptors().stream().map(SystemIndexDescriptor::getIndexPattern),
                        feature.getAssociatedIndexPatterns().stream())
                        .flatMap(pattern -> Arrays.stream(
                            indexNameExpressionResolver.concreteIndices(
                                intermediateState,
                                IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN,
                                pattern)))
                        .collect(Collectors.toSet());

                    state = feature.getCleanUpFunction().apply(
                        Sets.union(concreteSystemIndices, concreteAssociatedIndices),
                        deleteIndexService, state);
                }
                return state;
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
