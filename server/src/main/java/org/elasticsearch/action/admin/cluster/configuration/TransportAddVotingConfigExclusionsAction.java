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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TransportAddVotingConfigExclusionsAction extends TransportMasterNodeAction<AddVotingConfigExclusionsRequest,
        ActionResponse.Empty> {

    private static final Logger logger = LogManager.getLogger(TransportAddVotingConfigExclusionsAction.class);

    public static final Setting<Integer> MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING
        = Setting.intSetting("cluster.max_voting_config_exclusions", 10, 1, Property.Dynamic, Property.NodeScope);

    private volatile int maxVotingConfigExclusions;

    @Inject
    public TransportAddVotingConfigExclusionsAction(Settings settings, ClusterSettings clusterSettings, TransportService transportService,
                                                    ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AddVotingConfigExclusionsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            AddVotingConfigExclusionsRequest::new, indexNameExpressionResolver, in -> ActionResponse.Empty.INSTANCE,
                ThreadPool.Names.SAME);

        maxVotingConfigExclusions = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING, this::setMaxVotingConfigExclusions);
    }

    private void setMaxVotingConfigExclusions(int maxVotingConfigExclusions) {
        this.maxVotingConfigExclusions = maxVotingConfigExclusions;
    }

    @Override
    protected void masterOperation(Task task, AddVotingConfigExclusionsRequest request, ClusterState state,
                                   ActionListener<ActionResponse.Empty> listener) throws Exception {

        resolveVotingConfigExclusionsAndCheckMaximum(request, state, maxVotingConfigExclusions);
        // throws IAE if no nodes matched or maximum exceeded

        clusterService.submitStateUpdateTask("add-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT) {

            private Set<VotingConfigExclusion> resolvedExclusions;

            @Override
            public ClusterState execute(ClusterState currentState) {
                assert resolvedExclusions == null : resolvedExclusions;
                final int finalMaxVotingConfigExclusions = TransportAddVotingConfigExclusionsAction.this.maxVotingConfigExclusions;
                resolvedExclusions = resolveVotingConfigExclusionsAndCheckMaximum(request, currentState, finalMaxVotingConfigExclusions);

                final CoordinationMetadata.Builder builder = CoordinationMetadata.builder(currentState.coordinationMetadata());
                resolvedExclusions.forEach(builder::addVotingConfigExclusion);
                final Metadata newMetadata = Metadata.builder(currentState.metadata()).coordinationMetadata(builder.build()).build();
                final ClusterState newState = ClusterState.builder(currentState).metadata(newMetadata).build();
                assert newState.getVotingConfigExclusions().size() <= finalMaxVotingConfigExclusions;
                return newState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

                final ClusterStateObserver observer
                    = new ClusterStateObserver(clusterService, request.getTimeout(), logger, threadPool.getThreadContext());

                final Set<String> excludedNodeIds = resolvedExclusions.stream().map(VotingConfigExclusion::getNodeId)
                    .collect(Collectors.toSet());

                final Predicate<ClusterState> allNodesRemoved = clusterState -> {
                    final Set<String> votingConfigNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
                    return excludedNodeIds.stream().noneMatch(votingConfigNodeIds::contains);
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(ActionResponse.Empty.INSTANCE);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new ElasticsearchException("cluster service closed while waiting for voting config exclusions " +
                            resolvedExclusions + " to take effect"));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(new ElasticsearchTimeoutException("timed out waiting for voting config exclusions "
                            + resolvedExclusions + " to take effect"));
                    }
                };

                if (allNodesRemoved.test(newState)) {
                    clusterStateListener.onNewClusterState(newState);
                } else {
                    observer.waitForNextChange(clusterStateListener, allNodesRemoved);
                }
            }
        });
    }

    private static Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(AddVotingConfigExclusionsRequest request,
                                                                                           ClusterState state,
                                                                                           int maxVotingConfigExclusions) {
        return request.resolveVotingConfigExclusionsAndCheckMaximum(state, maxVotingConfigExclusions,
            MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey());
    }

    @Override
    protected ClusterBlockException checkBlock(AddVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
