/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateObserver.Listener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Predicate;

public class TransportClearVotingConfigExclusionsAction
    extends TransportMasterNodeAction<ClearVotingConfigExclusionsRequest, ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportClearVotingConfigExclusionsAction.class);

    @Inject
    public TransportClearVotingConfigExclusionsAction(TransportService transportService, ClusterService clusterService,
                                                      ThreadPool threadPool, ActionFilters actionFilters,
                                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ClearVotingConfigExclusionsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            ClearVotingConfigExclusionsRequest::new, indexNameExpressionResolver, in -> ActionResponse.Empty.INSTANCE,
                ThreadPool.Names.SAME);
    }

    @Override
    protected void masterOperation(Task task, ClearVotingConfigExclusionsRequest request, ClusterState initialState,
                                   ActionListener<ActionResponse.Empty> listener) throws Exception {

        final long startTimeMillis = threadPool.relativeTimeInMillis();

        final Predicate<ClusterState> allExclusionsRemoved = newState -> {
            for (VotingConfigExclusion tombstone : initialState.getVotingConfigExclusions()) {
                // NB checking for the existence of any node with this persistent ID, because persistent IDs are how votes are counted.
                if (newState.nodes().nodeExists(tombstone.getNodeId())) {
                    return false;
                }
            }
            return true;
        };

        if (request.getWaitForRemoval() && allExclusionsRemoved.test(initialState) == false) {
            final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(initialState, clusterService, request.getTimeout(),
                logger, threadPool.getThreadContext());

            clusterStateObserver.waitForNextChange(new Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    submitClearVotingConfigExclusionsTask(request, startTimeMillis, listener);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new ElasticsearchException("cluster service closed while waiting for removal of nodes "
                        + initialState.getVotingConfigExclusions()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new ElasticsearchTimeoutException(
                        "timed out waiting for removal of nodes; if nodes should not be removed, set waitForRemoval to false. "
                        + initialState.getVotingConfigExclusions()));
                }
            }, allExclusionsRemoved);
        } else {
            submitClearVotingConfigExclusionsTask(request, startTimeMillis, listener);
        }
    }

    private void submitClearVotingConfigExclusionsTask(ClearVotingConfigExclusionsRequest request, long startTimeMillis,
                                                       ActionListener<ActionResponse.Empty> listener) {
        clusterService.submitStateUpdateTask("clear-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT,
                TimeValue.timeValueMillis(
                        Math.max(0, request.getTimeout().millis() + startTimeMillis - threadPool.relativeTimeInMillis()))) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final CoordinationMetadata newCoordinationMetadata =
                        CoordinationMetadata.builder(currentState.coordinationMetadata()).clearVotingConfigExclusions().build();
                final Metadata newMetadata = Metadata.builder(currentState.metadata()).
                        coordinationMetadata(newCoordinationMetadata).build();
                return ClusterState.builder(currentState).metadata(newMetadata).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(ActionResponse.Empty.INSTANCE);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(ClearVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
