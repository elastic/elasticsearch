/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequestBuilder;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.usage.NodesUsageRequest;
import org.elasticsearch.action.admin.cluster.node.usage.NodesUsageResponse;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequestBuilder;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequestBuilder;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequestBuilder;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequestBuilder;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequestBuilder;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequestBuilder;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequestBuilder;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineRequestBuilder;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentType;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#cluster()
 */
public interface ClusterAdminClient extends ElasticsearchClient {

    /**
     * The health of the cluster.
     *
     * @param request The cluster state request
     * @return The result future
     */
    ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request);

    /**
     * The health of the cluster.
     *
     * @param request  The cluster state request
     * @param listener A listener to be notified with a result
     */
    void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener);

    /**
     * The health of the cluster.
     */
    ClusterHealthRequestBuilder prepareHealth(String... indices);

    /**
     * The state of the cluster.
     *
     * @param request The cluster state request.
     * @return The result future
     */
    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request);

    /**
     * The state of the cluster.
     *
     * @param request  The cluster state request.
     * @param listener A listener to be notified with a result
     */
    void state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener);

    /**
     * The state of the cluster.
     */
    ClusterStateRequestBuilder prepareState();

    /**
     * Updates settings in the cluster.
     */
    ActionFuture<ClusterUpdateSettingsResponse> updateSettings(ClusterUpdateSettingsRequest request);

    /**
     * Update settings in the cluster.
     */
    void updateSettings(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterUpdateSettingsRequestBuilder prepareUpdateSettings();

    /**
     * Reroutes allocation of shards. Advance API.
     */
    ActionFuture<ClusterRerouteResponse> reroute(ClusterRerouteRequest request);

    /**
     * Reroutes allocation of shards. Advance API.
     */
    void reroute(ClusterRerouteRequest request, ActionListener<ClusterRerouteResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterRerouteRequestBuilder prepareReroute();

    /**
     * Nodes info of the cluster.
     *
     * @param request The nodes info request
     * @return The result future
     */
    ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request);

    /**
     * Nodes info of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     */
    void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);

    /**
     * Nodes info of the cluster.
     */
    NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds);

    /**
     * Cluster wide aggregated stats
     *
     * @param request  The cluster stats request
     * @param listener A listener to be notified with a result
     */
    void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener);

    ClusterStatsRequestBuilder prepareClusterStats();

    /**
     * Nodes stats of the cluster.
     *
     * @param request The nodes stats request
     * @return The result future
     */
    ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request);

    /**
     * Nodes stats of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     */
    void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);

    /**
     * Nodes stats of the cluster.
     */
    NodesStatsRequestBuilder prepareNodesStats(String... nodesIds);

    /**
     * Nodes usage of the cluster.
     *
     * @param request
     *            The nodes usage request
     * @param listener
     *            A listener to be notified with a result
     */
    void nodesUsage(NodesUsageRequest request, ActionListener<NodesUsageResponse> listener);

    /**
     * List tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     */
    ActionFuture<ListTasksResponse> listTasks(ListTasksRequest request);

    /**
     * List active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     */
    void listTasks(ListTasksRequest request, ActionListener<ListTasksResponse> listener);

    /**
     * List active tasks
     */
    ListTasksRequestBuilder prepareListTasks(String... nodesIds);

    /**
     * Get a task.
     *
     * @param request the request
     * @return the result future
     */
    ActionFuture<GetTaskResponse> getTask(GetTaskRequest request);

    /**
     * Get a task.
     *
     * @param request the request
     * @param listener A listener to be notified with the result
     */
    void getTask(GetTaskRequest request, ActionListener<GetTaskResponse> listener);

    /**
     * Fetch a task by id.
     */
    GetTaskRequestBuilder prepareGetTask(String taskId);

    /**
     * Fetch a task by id.
     */
    GetTaskRequestBuilder prepareGetTask(TaskId taskId);

    /**
     * Cancel tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     */
    ActionFuture<ListTasksResponse> cancelTasks(CancelTasksRequest request);

    /**
     * Cancel active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     */
    void cancelTasks(CancelTasksRequest request, ActionListener<ListTasksResponse> listener);

    /**
     * Cancel active tasks
     */
    CancelTasksRequestBuilder prepareCancelTasks(String... nodesIds);

    /**
     * Returns list of shards the given search would be executed on.
     */
    void searchShards(ClusterSearchShardsRequest request, ActionListener<ClusterSearchShardsResponse> listener);

    /**
     * Returns list of shards the given search would be executed on.
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices);

    /**
     * Registers a snapshot repository.
     */
    void putRepository(PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Registers a snapshot repository.
     */
    PutRepositoryRequestBuilder preparePutRepository(String name);

    /**
     * Unregisters a repository.
     */
    void deleteRepository(DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Unregisters a repository.
     */
    DeleteRepositoryRequestBuilder prepareDeleteRepository(String name);

    /**
     * Gets repositories.
     */
    void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener);

    /**
     * Gets repositories.
     */
    GetRepositoriesRequestBuilder prepareGetRepositories(String... name);

    /**
     * Cleans up repository.
     */
    CleanupRepositoryRequestBuilder prepareCleanupRepository(String repository);

    /**
     * Cleans up repository.
     */
    void cleanupRepository(CleanupRepositoryRequest repository, ActionListener<CleanupRepositoryResponse> listener);

    /**
     * Verifies a repository.
     */
    void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener);

    /**
     * Verifies a repository.
     */
    VerifyRepositoryRequestBuilder prepareVerifyRepository(String name);

    /**
     * Creates a new snapshot.
     */
    ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request);

    /**
     * Creates a new snapshot.
     */
    void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener);

    /**
     * Creates a new snapshot.
     */
    CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name);

    /**
     * Clones a snapshot.
     */
    CloneSnapshotRequestBuilder prepareCloneSnapshot(String repository, String source, String target);

    /**
     * Clones a snapshot.
     */
    void cloneSnapshot(CloneSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Get snapshots.
     */
    void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener);

    /**
     * Get snapshots.
     */
    GetSnapshotsRequestBuilder prepareGetSnapshots(String... repository);

    /**
     * Delete snapshot.
     */
    void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete snapshot.
     */
    DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String... snapshot);

    /**
     * Restores a snapshot.
     */
    ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request);

    /**
     * Restores a snapshot.
     */
    void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener);

    /**
     * Restores a snapshot.
     */
    RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot);

    /**
     * Get snapshot status.
     */
    void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener);

    /**
     * Get snapshot status.
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository);

    /**
     * Get snapshot status.
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus();

    /**
     * Stores an ingest pipeline
     */
    void putPipeline(PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Stores an ingest pipeline
     */
    ActionFuture<AcknowledgedResponse> putPipeline(PutPipelineRequest request);

    /**
     * Stores an ingest pipeline
     */
    PutPipelineRequestBuilder preparePutPipeline(String id, BytesReference source, XContentType xContentType);

    /**
     * Deletes a stored ingest pipeline
     */
    void deletePipeline(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes a stored ingest pipeline
     */
    ActionFuture<AcknowledgedResponse> deletePipeline(DeletePipelineRequest request);

    /**
     * Deletes a stored ingest pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline(String id);

    /**
     * Returns a stored ingest pipeline
     */
    void getPipeline(GetPipelineRequest request, ActionListener<GetPipelineResponse> listener);

    /**
     * Returns a stored ingest pipeline
     */
    GetPipelineRequestBuilder prepareGetPipeline(String... ids);

    /**
     * Simulates an ingest pipeline
     */
    void simulatePipeline(SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener);

    /**
     * Simulates an ingest pipeline
     */
    ActionFuture<SimulatePipelineResponse> simulatePipeline(SimulatePipelineRequest request);

    /**
     * Simulates an ingest pipeline
     */
    SimulatePipelineRequestBuilder prepareSimulatePipeline(BytesReference source, XContentType xContentType);

    /**
     * Explain the allocation of a shard
     */
    void allocationExplain(ClusterAllocationExplainRequest request, ActionListener<ClusterAllocationExplainResponse> listener);

    /**
     * Explain the allocation of a shard
     */
    ActionFuture<ClusterAllocationExplainResponse> allocationExplain(ClusterAllocationExplainRequest request);

    /**
     * Explain the allocation of a shard
     */
    ClusterAllocationExplainRequestBuilder prepareAllocationExplain();

    /**
     * Store a script in the cluster state
     */
    PutStoredScriptRequestBuilder preparePutStoredScript();

    /**
     * Delete a script from the cluster state
     */
    void deleteStoredScript(DeleteStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete a script from the cluster state
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript(String id);

    /**
     * Store a script in the cluster state
     */
    void putStoredScript(PutStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Get a script from the cluster state
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript(String id);

    /**
     * Get a script from the cluster state
     */
    void getStoredScript(GetStoredScriptRequest request, ActionListener<GetStoredScriptResponse> listener);
}
