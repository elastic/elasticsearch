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
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexLongFieldRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public class ShardStartedTaskExecutor implements ClusterStateTaskExecutor<ShardStartedTaskExecutor.Task> {

    private static final Logger logger = LogManager.getLogger(ShardStartedTaskExecutor.class);

    // Deliberately not registered so it can only be set in tests/plugins.
    public static final Setting<Priority> SHARD_STARTED_REROUTE_SOME_UNASSIGNED_PRIORITY = Setting.enumSetting(
        Priority.class,
        "cluster.service.shard_started_reroute.some_unassigned.priority",
        Priority.HIGH,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Deliberately not registered so it can only be set in tests/plugins.
    public static final Setting<Priority> SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY = Setting.enumSetting(
        Priority.class,
        "cluster.service.shard_started_reroute.all_assigned.priority",
        Priority.HIGH,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final AllocationService allocationService;
    private final RerouteService rerouteService;

    private volatile Priority rerouteSomeUnassignedPriority;
    private volatile Priority rerouteAllAssignedPriority;

    public ShardStartedTaskExecutor(ClusterSettings clusterSettings, AllocationService allocationService, RerouteService rerouteService) {
        this.allocationService = allocationService;
        this.rerouteService = rerouteService;

        // setting only registered in some tests today
        clusterSettings.initializeAndWatchIfRegistered(
            SHARD_STARTED_REROUTE_SOME_UNASSIGNED_PRIORITY,
            v -> rerouteSomeUnassignedPriority = v
        );
        clusterSettings.initializeAndWatchIfRegistered(SHARD_STARTED_REROUTE_ALL_ASSIGNED_PRIORITY, v -> rerouteAllAssignedPriority = v);
    }

    @Override
    public ClusterState execute(BatchExecutionContext<Task> batchExecutionContext) throws Exception {
        List<TaskContext<Task>> tasksToBeApplied = new ArrayList<>();
        List<ShardRouting> shardRoutingsToBeApplied = new ArrayList<>(batchExecutionContext.taskContexts().size());
        Set<ShardRouting> seenShardRoutings = new HashSet<>(); // to prevent duplicates
        final Map<Index, ClusterStateTimeRanges> updatedTimestampRanges = new HashMap<>();

        final ClusterState initialState = batchExecutionContext.initialState();
        for (var taskContext : batchExecutionContext.taskContexts()) {
            final Task task = taskContext.getTask();
            final StartedShardEntry startedShardEntry = task.getStartedShardEntry();
            final Optional<ProjectMetadata> project = initialState.metadata().lookupProject(startedShardEntry.shardId.getIndex());
            final ShardRouting matched = project.map(ProjectMetadata::id)
                .map(id -> initialState.routingTable(id).getByAllocationId(startedShardEntry.shardId, startedShardEntry.allocationId))
                .orElse(null);
            if (matched == null) {
                // tasks that correspond to non-existent shards are marked as successful. The reason is that we resend shard started
                // events on every cluster state publishing that does not contain the shard as started yet. This means that old stale
                // requests might still be in flight even after the shard has already been started or failed on the master. We just
                // ignore these requests for now.
                logger.debug(
                    "{} ignoring shard started task [{}] (shard does not exist anymore)",
                    startedShardEntry.shardId,
                    startedShardEntry
                );
                taskContext.success(task::onSuccess);
            } else {
                final ProjectId projectId = project.get().id();
                if (matched.primary() && startedShardEntry.primaryTerm > 0) {
                    final IndexMetadata indexMetadata = initialState.metadata()
                        .getProject(projectId)
                        .index(startedShardEntry.shardId.getIndex());
                    assert indexMetadata != null;
                    final long currentPrimaryTerm = indexMetadata.primaryTerm(startedShardEntry.shardId.id());
                    if (currentPrimaryTerm != startedShardEntry.primaryTerm) {
                        assert currentPrimaryTerm > startedShardEntry.primaryTerm
                            : "received a primary term with a higher term than in the "
                                + "current cluster state (received ["
                                + startedShardEntry.primaryTerm
                                + "] but current is ["
                                + currentPrimaryTerm
                                + "])";
                        logger.debug(
                            "{} ignoring shard started task [{}] (primary term {} does not match current term {})",
                            startedShardEntry.shardId,
                            startedShardEntry,
                            startedShardEntry.primaryTerm,
                            currentPrimaryTerm
                        );
                        taskContext.success(task::onSuccess);
                        continue;
                    }
                }
                if (matched.initializing() == false) {
                    assert matched.active() : "expected active shard routing for task " + startedShardEntry + " but found " + matched;
                    // same as above, this might have been a stale in-flight request, so we just ignore.
                    logger.debug(
                        "{} ignoring shard started task [{}] (shard exists but is not initializing: {})",
                        startedShardEntry.shardId,
                        startedShardEntry,
                        matched
                    );
                    taskContext.success(task::onSuccess);
                } else {
                    // remove duplicate actions as allocation service expects a clean list without duplicates
                    if (seenShardRoutings.contains(matched)) {
                        logger.trace(
                            "{} ignoring shard started task [{}] (already scheduled to start {})",
                            startedShardEntry.shardId,
                            startedShardEntry,
                            matched
                        );
                        tasksToBeApplied.add(taskContext);
                    } else {
                        logger.debug(
                            "{} starting shard {} (shard started task: [{}])",
                            startedShardEntry.shardId,
                            matched,
                            startedShardEntry
                        );
                        tasksToBeApplied.add(taskContext);
                        shardRoutingsToBeApplied.add(matched);
                        seenShardRoutings.add(matched);

                        // expand the timestamp range(s) recorded in the index metadata if needed
                        final Index index = startedShardEntry.shardId.getIndex();
                        ClusterStateTimeRanges clusterStateTimeRanges = updatedTimestampRanges.get(index);
                        IndexLongFieldRange currentTimestampMillisRange = clusterStateTimeRanges == null
                            ? null
                            : clusterStateTimeRanges.timestampRange();
                        IndexLongFieldRange currentEventIngestedMillisRange = clusterStateTimeRanges == null
                            ? null
                            : clusterStateTimeRanges.eventIngestedRange();

                        final IndexMetadata indexMetadata = initialState.metadata().getProject(projectId).index(index);
                        if (currentTimestampMillisRange == null) {
                            currentTimestampMillisRange = indexMetadata.getTimestampRange();
                        }
                        if (currentEventIngestedMillisRange == null) {
                            currentEventIngestedMillisRange = indexMetadata.getEventIngestedRange();
                        }

                        final IndexLongFieldRange newTimestampMillisRange = currentTimestampMillisRange.extendWithShardRange(
                            startedShardEntry.shardId.id(),
                            indexMetadata.getNumberOfShards(),
                            startedShardEntry.timestampRange
                        );
                        IndexLongFieldRange newEventIngestedMillisRange = currentEventIngestedMillisRange.extendWithShardRange(
                            startedShardEntry.shardId.id(),
                            indexMetadata.getNumberOfShards(),
                            startedShardEntry.eventIngestedRange
                        );

                        if (newTimestampMillisRange != currentTimestampMillisRange
                            || newEventIngestedMillisRange != currentEventIngestedMillisRange) {
                            updatedTimestampRanges.put(
                                index,
                                new ClusterStateTimeRanges(newTimestampMillisRange, newEventIngestedMillisRange)
                            );
                        }
                    }
                }
            }
        }
        assert tasksToBeApplied.size() >= shardRoutingsToBeApplied.size();

        ClusterState maybeUpdatedState = initialState;
        try {
            maybeUpdatedState = allocationService.applyStartedShards(initialState, shardRoutingsToBeApplied);

            if (updatedTimestampRanges.isEmpty() == false) {
                final Metadata.Builder metadataBuilder = Metadata.builder(maybeUpdatedState.metadata());
                for (Map.Entry<Index, ClusterStateTimeRanges> updatedTimeRangesEntry : updatedTimestampRanges.entrySet()) {
                    ClusterStateTimeRanges timeRanges = updatedTimeRangesEntry.getValue();
                    Index index = updatedTimeRangesEntry.getKey();
                    var projectId = maybeUpdatedState.metadata().projectFor(index).id();
                    var projectMetadataBuilder = metadataBuilder.getProject(projectId);
                    projectMetadataBuilder.put(
                        IndexMetadata.builder(projectMetadataBuilder.getSafe(index))
                            .timestampRange(timeRanges.timestampRange())
                            .eventIngestedRange(timeRanges.eventIngestedRange())
                    );
                }
                maybeUpdatedState = ClusterState.builder(maybeUpdatedState).metadata(metadataBuilder).build();
            }

            assert assertStartedIndicesHaveCompleteTimestampRanges(maybeUpdatedState);

            for (final var taskContext : tasksToBeApplied) {
                final var task = taskContext.getTask();
                taskContext.success(task::onSuccess);
            }
        } catch (Exception e) {
            logger.warn(() -> format("failed to apply started shards %s", shardRoutingsToBeApplied), e);
            for (final var taskContext : tasksToBeApplied) {
                taskContext.onFailure(e);
            }
        }

        return maybeUpdatedState;
    }

    private static boolean assertStartedIndicesHaveCompleteTimestampRanges(ClusterState clusterState) {
        for (ProjectId projectId : clusterState.metadata().projects().keySet()) {
            for (Map.Entry<String, IndexRoutingTable> cursor : clusterState.routingTable(projectId).getIndicesRouting().entrySet()) {
                assert cursor.getValue().allPrimaryShardsActive() == false
                    || clusterState.metadata().getProject(projectId).index(cursor.getKey()).getTimestampRange().isComplete()
                    : "index ["
                        + cursor.getKey()
                        + "] should have complete timestamp range, but got "
                        + clusterState.metadata().getProject(projectId).index(cursor.getKey()).getTimestampRange()
                        + " for "
                        + cursor.getValue().prettyPrint();

                assert cursor.getValue().allPrimaryShardsActive() == false
                    || clusterState.metadata().getProject(projectId).index(cursor.getKey()).getEventIngestedRange().isComplete()
                    : "index ["
                        + cursor.getKey()
                        + "] should have complete event.ingested range, but got "
                        + clusterState.metadata().getProject(projectId).index(cursor.getKey()).getEventIngestedRange()
                        + " for "
                        + cursor.getValue().prettyPrint();
            }
        }
        return true;
    }

    @Override
    public void clusterStatePublished(ClusterState newClusterState) {
        final String reason;
        final Priority priority;

        final var rerouteSomeUnassignedPriority = this.rerouteSomeUnassignedPriority; // single volatile read
        final var rerouteAllAssignedPriority = this.rerouteAllAssignedPriority; // single volatile read

        if (rerouteSomeUnassignedPriority == rerouteAllAssignedPriority) {
            // skip unassigned-shards check
            reason = "reroute after starting shards";
            priority = rerouteSomeUnassignedPriority;
        } else if (newClusterState.getRoutingNodes().hasUnassignedShards()) {
            reason = "reroute after starting shards with more shards to assign";
            priority = rerouteSomeUnassignedPriority;
        } else {
            reason = "reroute after starting shards with no more shards to assign";
            priority = rerouteAllAssignedPriority;
        }

        rerouteService.reroute(
            reason,
            priority,
            ActionListener.wrap(
                r -> logger.trace("reroute after starting shards succeeded"),
                e -> logger.debug("reroute after starting shards failed", e)
            )
        );
    }

    /**
     * Task that runs on the master node. Handles responding to the request listener with the result of the update request.
     * Task is created when the master node receives a data node request to mark a shard as {@link ShardRoutingState#STARTED}.
     *
     * @param entry Information about the newly started shard.
     * @param listener Channel listener with which to respond to the data node.
     */
    public record Task(StartedShardEntry entry, ActionListener<Void> listener) implements ClusterStateTaskListener {

        public StartedShardEntry getStartedShardEntry() {
            return entry;
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof NotMasterException) {
                logger.debug(() -> format("%s no longer master while starting shard [%s]", entry.shardId, entry));
            } else if (e instanceof FailedToCommitClusterStateException) {
                logger.debug(() -> format("%s unexpected failure while starting shard [%s]", entry.shardId, entry), e);
            } else {
                logger.error(() -> format("%s unexpected failure while starting shard [%s]", entry.shardId, entry), e);
            }
            listener.onFailure(e);
        }

        public void onSuccess() {
            listener.onResponse(null);
        }

        @Override
        public String toString() {
            return "ShardStartedTaskExecutor.Task{entry=" + entry + ", listener=" + listener + "}";
        }
    }

    /**
     * Holder of the pair of time ranges needed in cluster state - one for @timestamp, the other for 'event.ingested'.
     * Since 'event.ingested' was added well after @timestamp, it can be UNKNOWN when @timestamp range is present.
     *
     * @param timestampRange     range for @timestamp
     * @param eventIngestedRange range for event.ingested
     */
    record ClusterStateTimeRanges(IndexLongFieldRange timestampRange, IndexLongFieldRange eventIngestedRange) {}
}
