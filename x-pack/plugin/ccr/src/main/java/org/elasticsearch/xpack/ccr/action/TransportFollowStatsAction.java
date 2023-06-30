/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.Ccr;
import org.elasticsearch.xpack.ccr.CcrLicenseChecker;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportFollowStatsAction extends TransportTasksAction<
    ShardFollowNodeTask,
    FollowStatsAction.StatsRequest,
    FollowStatsAction.StatsResponses,
    FollowStatsAction.StatsResponse> {

    private final CcrLicenseChecker ccrLicenseChecker;

    @Inject
    public TransportFollowStatsAction(
        final ClusterService clusterService,
        final TransportService transportService,
        final ActionFilters actionFilters,
        final CcrLicenseChecker ccrLicenseChecker
    ) {
        super(
            FollowStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            FollowStatsAction.StatsRequest::new,
            FollowStatsAction.StatsResponses::new,
            FollowStatsAction.StatsResponse::new,
            Ccr.CCR_THREAD_POOL_NAME
        );
        this.ccrLicenseChecker = Objects.requireNonNull(ccrLicenseChecker);
    }

    @Override
    protected void doExecute(
        final Task task,
        final FollowStatsAction.StatsRequest request,
        final ActionListener<FollowStatsAction.StatsResponses> listener
    ) {
        if (ccrLicenseChecker.isCcrAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException("ccr"));
            return;
        }

        if (Strings.isAllOrWildcard(request.indices()) == false) {
            final ClusterState state = clusterService.state();
            Set<String> shardFollowTaskFollowerIndices = findFollowerIndicesFromShardFollowTasks(state, request.indices());
            if (shardFollowTaskFollowerIndices.isEmpty()) {
                String resources = String.join(",", request.indices());
                throw new ResourceNotFoundException("No shard follow tasks for follower indices [{}]", resources);
            }
        }
        super.doExecute(task, request, new ActionListener<FollowStatsAction.StatsResponses>() {
            @Override
            public void onResponse(FollowStatsAction.StatsResponses statsResponses) {

                statsResponses.getStatsResponses().forEach(stats -> {

                    try {
                        String leaderIdx = stats.status().leaderIndex();
                        String followerIdx = stats.status().followerIndex();
                        String remoteCluster = stats.status().getRemoteCluster();

                        SearchRequest countRequest1 = new SearchRequest(followerIdx);
                        countRequest1.source(new SearchSourceBuilder().size(0).trackTotalHits(true));

                        transportService.sendRequest(
                            transportService.getLocalNodeConnection(),
                            SearchAction.NAME,
                            countRequest1,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(new ActionListener<>() {
                                @Override
                                public void onResponse(SearchResponse response) {
                                    System.out.println(">>>>>>>>>>> Leader index doc count >>>>>>>>>>>>>>");
                                    System.out.println(response.getHits().getTotalHits().value);
                                }
                                @Override
                                public void onFailure(Exception e) {
                                    e.printStackTrace(System.err);
                                }
                            }, SearchResponse::new)
                        );

                        SearchRequest countRequest2 = new SearchRequest(leaderIdx);
                        countRequest2.source(new SearchSourceBuilder().size(0).trackTotalHits(true));

                        transportService.sendRequest(
                            transportService.getRemoteClusterService().getConnection(remoteCluster),
                            SearchAction.NAME,
                            countRequest2,
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(new ActionListener<>() {
                                @Override
                                public void onResponse(SearchResponse response) {
                                    System.out.println(">>>>>>>>>>> Follower index doc count >>>>>>>>>>>>>>");
                                    System.out.println(response.getHits().getTotalHits().value);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    e.printStackTrace(System.err);
                                }
                            }, SearchResponse::new)
                        );

                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }

                });



                listener.onResponse(statsResponses);
            }
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected FollowStatsAction.StatsResponses newResponse(
        final FollowStatsAction.StatsRequest request,
        final List<FollowStatsAction.StatsResponse> statsRespons,
        final List<TaskOperationFailure> taskOperationFailures,
        final List<FailedNodeException> failedNodeExceptions
    ) {
        return new FollowStatsAction.StatsResponses(taskOperationFailures, failedNodeExceptions, statsRespons);
    }

    @Override
    protected List<ShardFollowNodeTask> processTasks(final FollowStatsAction.StatsRequest request) {
        final ClusterState state = clusterService.state();
        final Set<String> followerIndices = findFollowerIndicesFromShardFollowTasks(state, request.indices());

        final var tasks = new ArrayList<ShardFollowNodeTask>();
        for (final Task task : taskManager.getTasks().values()) {
            if (task instanceof final ShardFollowNodeTask shardFollowNodeTask) {
                if (followerIndices.contains(shardFollowNodeTask.getFollowShardId().getIndexName())) {
                    tasks.add(shardFollowNodeTask);
                }
            }
        }
        return tasks;
    }

    @Override
    protected void taskOperation(
        final CancellableTask actionTask,
        final FollowStatsAction.StatsRequest request,
        final ShardFollowNodeTask task,
        final ActionListener<FollowStatsAction.StatsResponse> listener
    ) {
        listener.onResponse(new FollowStatsAction.StatsResponse(task.getStatus()));
    }

    static Set<String> findFollowerIndicesFromShardFollowTasks(ClusterState state, String[] indices) {
        final PersistentTasksCustomMetadata persistentTasksMetadata = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (persistentTasksMetadata == null) {
            return Collections.emptySet();
        }
        final Metadata metadata = state.metadata();
        final Set<String> requestedFollowerIndices = indices != null ? new HashSet<>(Arrays.asList(indices)) : Collections.emptySet();
        return persistentTasksMetadata.tasks()
            .stream()
            .filter(persistentTask -> persistentTask.getTaskName().equals(ShardFollowTask.NAME))
            .map(persistentTask -> {
                ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
                return shardFollowTask.getFollowShardId().getIndex();
            })
            .filter(followerIndex -> metadata.index(followerIndex) != null) // hide tasks that are orphaned (see ShardFollowTaskCleaner)
            .map(Index::getName)
            .filter(followerIndex -> Strings.isAllOrWildcard(indices) || requestedFollowerIndices.contains(followerIndex))
            .collect(Collectors.toSet());
    }

}
