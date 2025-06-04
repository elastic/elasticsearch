/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
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
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

public class TransportAddVotingConfigExclusionsAction extends TransportMasterNodeAction<
    AddVotingConfigExclusionsRequest,
    ActionResponse.Empty> {

    public static final ActionType<ActionResponse.Empty> TYPE = new ActionType<>("cluster:admin/voting_config/add_exclusions");
    private static final Logger logger = LogManager.getLogger(TransportAddVotingConfigExclusionsAction.class);

    public static final Setting<Integer> MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING = Setting.intSetting(
        "cluster.max_voting_config_exclusions",
        10,
        1,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile int maxVotingConfigExclusions;
    private final Reconfigurator reconfigurator;

    @Inject
    public TransportAddVotingConfigExclusionsAction(
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Reconfigurator reconfigurator
    ) {
        super(
            TYPE.name(),
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AddVotingConfigExclusionsRequest::new,
            in -> ActionResponse.Empty.INSTANCE,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        maxVotingConfigExclusions = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING, this::setMaxVotingConfigExclusions);
        this.reconfigurator = reconfigurator;
    }

    private void setMaxVotingConfigExclusions(int maxVotingConfigExclusions) {
        this.maxVotingConfigExclusions = maxVotingConfigExclusions;
    }

    @Override
    protected void masterOperation(
        Task task,
        AddVotingConfigExclusionsRequest request,
        ClusterState state,
        ActionListener<ActionResponse.Empty> listener
    ) throws Exception {
        reconfigurator.ensureVotingConfigCanBeModified();

        resolveVotingConfigExclusionsAndCheckMaximum(request, state, maxVotingConfigExclusions);
        // throws IAE if no nodes matched or maximum exceeded

        submitUnbatchedTask("add-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT) {

            @Override
            public ClusterState execute(ClusterState currentState) {
                final int finalMaxVotingConfigExclusions = TransportAddVotingConfigExclusionsAction.this.maxVotingConfigExclusions;
                final var resolvedExclusions = resolveVotingConfigExclusionsAndCheckMaximum(
                    request,
                    currentState,
                    finalMaxVotingConfigExclusions
                );

                final CoordinationMetadata.Builder builder = CoordinationMetadata.builder(currentState.coordinationMetadata());
                resolvedExclusions.forEach(builder::addVotingConfigExclusion);
                final Metadata newMetadata = Metadata.builder(currentState.metadata()).coordinationMetadata(builder.build()).build();
                final ClusterState newState = ClusterState.builder(currentState).metadata(newMetadata).build();
                assert newState.getVotingConfigExclusions().size() <= finalMaxVotingConfigExclusions;
                return newState;
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {

                final ClusterStateObserver observer = new ClusterStateObserver(
                    clusterService,
                    request.getTimeout(),
                    logger,
                    threadPool.getThreadContext()
                );

                final Predicate<ClusterState> allNodesRemoved = clusterState -> {
                    final Set<String> votingConfigNodeIds = new HashSet<>();
                    votingConfigNodeIds.addAll(clusterState.getLastCommittedConfiguration().getNodeIds());
                    votingConfigNodeIds.addAll(clusterState.getLastAcceptedConfiguration().getNodeIds());
                    return clusterState.getVotingConfigExclusions()
                        .stream()
                        .noneMatch(votingConfigExclusion -> votingConfigNodeIds.contains(votingConfigExclusion.getNodeId()));
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(ActionResponse.Empty.INSTANCE);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(
                            new ElasticsearchException("cluster service closed while waiting for voting config exclusions to take effect")
                        );
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(
                            new ElasticsearchTimeoutException("timed out waiting for voting config exclusions to take effect")
                        );
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

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private static Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(
        AddVotingConfigExclusionsRequest request,
        ClusterState state,
        int maxVotingConfigExclusions
    ) {
        return request.resolveVotingConfigExclusionsAndCheckMaximum(
            state,
            maxVotingConfigExclusions,
            MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(AddVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
