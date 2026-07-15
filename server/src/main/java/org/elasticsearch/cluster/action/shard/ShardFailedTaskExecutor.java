/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.action.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.action.shard.ShardStateAction.NoLongerPrimaryShardException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.StaleShard;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.logging.log4j.Level.DEBUG;
import static org.apache.logging.log4j.Level.ERROR;
import static org.elasticsearch.cluster.service.MasterService.isPublishFailureException;
import static org.elasticsearch.core.Strings.format;

public class ShardFailedTaskExecutor implements ClusterStateTaskExecutor<ShardFailedTaskExecutor.Task> {

    private static final Logger logger = LogManager.getLogger(ShardFailedTaskExecutor.class);

    private final AllocationService allocationService;
    private final RerouteService rerouteService;

    public ShardFailedTaskExecutor(AllocationService allocationService, RerouteService rerouteService) {
        this.allocationService = allocationService;
        this.rerouteService = rerouteService;
    }

    @Override
    public ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) throws Exception {
        List<TaskContext<Task>> tasksToBeApplied = new ArrayList<>();
        List<FailedShard> failedShardsToBeApplied = new ArrayList<>();
        List<StaleShard> staleShardsToBeApplied = new ArrayList<>();
        final ClusterState initialState = batchExecutionContext.initialState();
        for (final var taskContext : batchExecutionContext.taskContexts()) {
            final var task = taskContext.getTask();
            FailedShardEntry entry = task.entry();
            final Optional<ProjectMetadata> project = initialState.metadata().lookupProject(entry.getShardId().getIndex());
            IndexMetadata indexMetadata = project.map(proj -> proj.index(entry.getShardId().getIndex())).orElse(null);
            if (indexMetadata == null) {
                logger.debug(
                    "{} ignoring shard failed task [{}] (unknown index {})",
                    entry.getShardId(),
                    entry,
                    entry.getShardId().getIndex()
                );
                taskContext.success(task::onSuccess);
            } else {
                if (entry.primaryTerm > 0) {
                    long currentPrimaryTerm = indexMetadata.primaryTerm(entry.getShardId().id());
                    if (currentPrimaryTerm != entry.primaryTerm) {
                        assert currentPrimaryTerm > entry.primaryTerm
                            : "received a primary term with a higher term than in the "
                                + "current cluster state (received ["
                                + entry.primaryTerm
                                + "] but current is ["
                                + currentPrimaryTerm
                                + "])";
                        logger.debug(
                            "{} failing shard failed task [{}] (primary term {} does not match current term {})",
                            entry.getShardId(),
                            entry,
                            entry.primaryTerm,
                            indexMetadata.primaryTerm(entry.getShardId().id())
                        );
                        taskContext.onFailure(
                            new NoLongerPrimaryShardException(
                                entry.getShardId(),
                                "primary term [" + entry.primaryTerm + "] did not match current primary term [" + currentPrimaryTerm + "]"
                            )
                        );
                        continue;
                    }
                }

                ShardRouting matched = initialState.routingTable(project.get().id())
                    .getByAllocationId(entry.getShardId(), entry.getAllocationId());
                if (matched == null) {
                    Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(entry.getShardId().id());
                    if (entry.primaryTerm > 0 && inSyncAllocationIds.contains(entry.getAllocationId())) {
                        logger.debug(
                            "{} marking shard {} as stale (shard failed task: [{}])",
                            entry.getShardId(),
                            entry.getAllocationId(),
                            entry
                        );
                        tasksToBeApplied.add(taskContext);
                        staleShardsToBeApplied.add(new StaleShard(entry.getShardId(), entry.getAllocationId()));
                    } else {
                        logger.debug("{} ignoring shard failed task [{}] (shard does not exist anymore)", entry.getShardId(), entry);
                        taskContext.success(task::onSuccess);
                    }
                } else {
                    logger.debug("{} failing shard {} (shard failed task: [{}])", entry.getShardId(), matched, task);
                    tasksToBeApplied.add(taskContext);
                    failedShardsToBeApplied.add(new FailedShard(matched, entry.message, entry.failure, entry.markAsStale));
                }
            }
        }
        assert tasksToBeApplied.size() == failedShardsToBeApplied.size() + staleShardsToBeApplied.size();

        ClusterState maybeUpdatedState = initialState;
        try (var ignored = batchExecutionContext.dropHeadersContext()) {
            maybeUpdatedState = applyFailedShards(initialState, failedShardsToBeApplied, staleShardsToBeApplied);
            for (final var taskContext : tasksToBeApplied) {
                final var task = taskContext.getTask();
                taskContext.success(task::onSuccess);
            }
        } catch (Exception e) {
            logger.warn(() -> format("failed to apply failed shards %s", failedShardsToBeApplied), e);
            for (final var taskContext : tasksToBeApplied) {
                taskContext.onFailure(e);
            }
        }

        return maybeUpdatedState;
    }

    ClusterState applyFailedShards(ClusterState currentState, List<FailedShard> failedShards, List<StaleShard> staleShards) {
        return allocationService.applyFailedShards(currentState, failedShards, staleShards);
    }

    @Override
    public void clusterStatePublished(ClusterState newClusterState) {
        if (rerouteService == null) {
            return;
        }
        int numberOfUnassignedShards = newClusterState.getRoutingNodes().unassigned().size();
        if (numberOfUnassignedShards > 0) {
            final String reason = Strings.format("[%d] unassigned shards after failing shards", numberOfUnassignedShards);
            logger.trace("{}, scheduling a reroute", reason);
            rerouteService.reroute(
                reason,
                Priority.NORMAL,
                ActionListener.wrap(
                    r -> logger.trace("{}, reroute completed", reason),
                    e -> logger.debug(() -> format("%s, reroute failed", reason), e)
                )
            );
        }
    }

    public static class Task implements ClusterStateTaskListener {

        private static final Logger logger = LogManager.getLogger(Task.class);

        private final FailedShardEntry entry;
        private final ActionListener<Void> listener;

        public Task(FailedShardEntry entry, ActionListener<Void> listener) {
            this.entry = entry;
            this.listener = listener;
        }

        public FailedShardEntry entry() {
            return entry;
        }

        public void onSuccess() {
            listener.onResponse(null);
        }

        @Override
        public void onFailure(Exception e) {
            logger.log(
                isPublishFailureException(e) ? DEBUG : ERROR,
                () -> format("%s unexpected failure while failing shard [%s]", entry.shardId, entry),
                e
            );
            listener.onFailure(e);
        }
    }
}
