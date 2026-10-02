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
                // tasks that correspond to non-existent indices are marked as successful
                logger.debug(
                    "{} ignoring shard failed task [{}] (unknown index {})",
                    entry.getShardId(),
                    entry,
                    entry.getShardId().getIndex()
                );
                taskContext.success(task::onSuccess);
            } else {
                // The primary term is 0 if the shard failed itself. It is > 0 if a write was done on a primary but was failed to be
                // replicated to the shard copy with the provided allocation id. In case where the shard failed itself, it's ok to just
                // remove the corresponding routing entry from the routing table. In case where a write could not be replicated,
                // however, it is important to ensure that the shard copy with the missing write is considered as stale from that point
                // on, which is implemented by removing the allocation id of the shard copy from the in-sync allocations set.
                // We check here that the primary to which the write happened was not already failed in an earlier cluster state update.
                // This prevents situations where a new primary has already been selected and replication failures from an old stale
                // primary unnecessarily fail currently active shards.
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
                    // mark shard copies without routing entries that are in in-sync allocations set only as stale if the reason why
                    // they were failed is because a write made it into the primary but not to this copy (which corresponds to
                    // the check "primaryTerm > 0").
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
                        // tasks that correspond to non-existent shards are marked as successful
                        logger.debug("{} ignoring shard failed task [{}] (shard does not exist anymore)", entry.getShardId(), entry);
                        taskContext.success(task::onSuccess);
                    }
                } else {
                    // failing a shard also possibly marks it as stale (see IndexMetadataUpdater)
                    logger.debug("{} failing shard {} (shard failed task: [{}])", entry.getShardId(), matched, task);
                    tasksToBeApplied.add(taskContext);
                    failedShardsToBeApplied.add(new FailedShard(matched, entry.message, entry.failure, entry.markAsStale));
                }
            }
        }
        assert tasksToBeApplied.size() == failedShardsToBeApplied.size() + staleShardsToBeApplied.size();

        ClusterState maybeUpdatedState = initialState;
        try (var ignored = batchExecutionContext.dropHeadersContext()) {
            // drop deprecation warnings arising from the computation (reroute etc).
            maybeUpdatedState = applyFailedShards(initialState, failedShardsToBeApplied, staleShardsToBeApplied);
            for (final var taskContext : tasksToBeApplied) {
                final var task = taskContext.getTask();
                taskContext.success(task::onSuccess);
            }
        } catch (Exception e) {
            logger.warn(() -> format("failed to apply failed shards %s", failedShardsToBeApplied), e);
            // failures are communicated back to the requester
            // cluster state will not be updated in this case
            for (final var taskContext : tasksToBeApplied) {
                taskContext.onFailure(e);
            }
        }

        return maybeUpdatedState;
    }

    // visible for testing
    ClusterState applyFailedShards(ClusterState currentState, List<FailedShard> failedShards, List<StaleShard> staleShards) {
        return allocationService.applyFailedShards(currentState, failedShards, staleShards);
    }

    @Override
    public void clusterStatePublished(ClusterState newClusterState) {
        int numberOfUnassignedShards = newClusterState.getRoutingNodes().unassigned().size();
        if (numberOfUnassignedShards > 0) {
            // The reroute called after failing some shards will not assign any shard back to the node on which it failed. If there were
            // no other options for a failed shard then it is left unassigned. However, absent other options it's better to try and
            // assign it again, even if that means putting it back on the node on which it previously failed:
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

    public record Task(FailedShardEntry entry, ActionListener<Void> listener) implements ClusterStateTaskListener {
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
