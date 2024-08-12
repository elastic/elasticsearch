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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.capabilities.NodesCapabilitiesRequest;
import org.elasticsearch.action.admin.cluster.node.capabilities.NodesCapabilitiesResponse;
import org.elasticsearch.action.admin.cluster.node.capabilities.TransportNodesCapabilitiesAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.cluster.node.usage.NodesUsageRequest;
import org.elasticsearch.action.admin.cluster.node.usage.NodesUsageResponse;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodesUsageAction;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequestBuilder;
import org.elasticsearch.action.ingest.DeletePipelineTransportAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.GetPipelineRequest;
import org.elasticsearch.action.ingest.GetPipelineRequestBuilder;
import org.elasticsearch.action.ingest.GetPipelineResponse;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequestBuilder;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineRequestBuilder;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#cluster()
 */
public class ClusterAdminClient implements ElasticsearchClient {

    protected final ElasticsearchClient client;

    public ClusterAdminClient(ElasticsearchClient client) {
        this.client = client;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
        ActionType<Response> action,
        Request request
    ) {
        return client.execute(action, request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        client.execute(action, request, listener);
    }

    @Override
    public ThreadPool threadPool() {
        return client.threadPool();
    }

    public ActionFuture<ClusterHealthResponse> health(final ClusterHealthRequest request) {
        return execute(TransportClusterHealthAction.TYPE, request);
    }

    public void health(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        execute(TransportClusterHealthAction.TYPE, request, listener);
    }

    public ClusterHealthRequestBuilder prepareHealth(String... indices) {
        return new ClusterHealthRequestBuilder(this).setIndices(indices);
    }

    public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
        return execute(ClusterStateAction.INSTANCE, request);
    }

    public void state(final ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
        execute(ClusterStateAction.INSTANCE, request, listener);
    }

    public ClusterStateRequestBuilder prepareState() {
        return new ClusterStateRequestBuilder(this);
    }

    public ActionFuture<ClusterUpdateSettingsResponse> updateSettings(final ClusterUpdateSettingsRequest request) {
        return execute(ClusterUpdateSettingsAction.INSTANCE, request);
    }

    public void updateSettings(final ClusterUpdateSettingsRequest request, final ActionListener<ClusterUpdateSettingsResponse> listener) {
        execute(ClusterUpdateSettingsAction.INSTANCE, request, listener);
    }

    public ClusterUpdateSettingsRequestBuilder prepareUpdateSettings() {
        return new ClusterUpdateSettingsRequestBuilder(this);
    }

    public ActionFuture<NodesInfoResponse> nodesInfo(final NodesInfoRequest request) {
        return execute(TransportNodesInfoAction.TYPE, request);
    }

    public void nodesInfo(final NodesInfoRequest request, final ActionListener<NodesInfoResponse> listener) {
        execute(TransportNodesInfoAction.TYPE, request, listener);
    }

    public NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds) {
        return new NodesInfoRequestBuilder(this, nodesIds);
    }

    public void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener) {
        execute(TransportClusterStatsAction.TYPE, request, listener);
    }

    public ClusterStatsRequestBuilder prepareClusterStats() {
        return new ClusterStatsRequestBuilder(this);
    }

    public ActionFuture<NodesStatsResponse> nodesStats(final NodesStatsRequest request) {
        return execute(TransportNodesStatsAction.TYPE, request);
    }

    public void nodesStats(final NodesStatsRequest request, final ActionListener<NodesStatsResponse> listener) {
        execute(TransportNodesStatsAction.TYPE, request, listener);
    }

    public NodesStatsRequestBuilder prepareNodesStats(String... nodesIds) {
        return new NodesStatsRequestBuilder(this, nodesIds);
    }

    public ActionFuture<NodesCapabilitiesResponse> nodesCapabilities(final NodesCapabilitiesRequest request) {
        return execute(TransportNodesCapabilitiesAction.TYPE, request);
    }

    public void nodesCapabilities(final NodesCapabilitiesRequest request, final ActionListener<NodesCapabilitiesResponse> listener) {
        execute(TransportNodesCapabilitiesAction.TYPE, request, listener);
    }

    public void nodesUsage(final NodesUsageRequest request, final ActionListener<NodesUsageResponse> listener) {
        execute(TransportNodesUsageAction.TYPE, request, listener);
    }

    public ActionFuture<ListTasksResponse> listTasks(final ListTasksRequest request) {
        return execute(TransportListTasksAction.TYPE, request);
    }

    public void listTasks(final ListTasksRequest request, final ActionListener<ListTasksResponse> listener) {
        execute(TransportListTasksAction.TYPE, request, listener);
    }

    public ListTasksRequestBuilder prepareListTasks(String... nodesIds) {
        return new ListTasksRequestBuilder(this).setNodesIds(nodesIds);
    }

    public ActionFuture<GetTaskResponse> getTask(final GetTaskRequest request) {
        return execute(TransportGetTaskAction.TYPE, request);
    }

    public void getTask(final GetTaskRequest request, final ActionListener<GetTaskResponse> listener) {
        execute(TransportGetTaskAction.TYPE, request, listener);
    }

    public GetTaskRequestBuilder prepareGetTask(String taskId) {
        return prepareGetTask(new TaskId(taskId));
    }

    public GetTaskRequestBuilder prepareGetTask(TaskId taskId) {
        return new GetTaskRequestBuilder(this).setTaskId(taskId);
    }

    public ActionFuture<ListTasksResponse> cancelTasks(CancelTasksRequest request) {
        return execute(TransportCancelTasksAction.TYPE, request);
    }

    public void cancelTasks(CancelTasksRequest request, ActionListener<ListTasksResponse> listener) {
        execute(TransportCancelTasksAction.TYPE, request, listener);
    }

    public CancelTasksRequestBuilder prepareCancelTasks(String... nodesIds) {
        return new CancelTasksRequestBuilder(this).setNodesIds(nodesIds);
    }

    public void putRepository(PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
        execute(TransportPutRepositoryAction.TYPE, request, listener);
    }

    public PutRepositoryRequestBuilder preparePutRepository(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name) {
        return new PutRepositoryRequestBuilder(this, masterNodeTimeout, ackTimeout, name);
    }

    public void deleteRepository(DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
        execute(TransportDeleteRepositoryAction.TYPE, request, listener);
    }

    public DeleteRepositoryRequestBuilder prepareDeleteRepository(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name) {
        return new DeleteRepositoryRequestBuilder(this, masterNodeTimeout, ackTimeout, name);
    }

    public void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener) {
        execute(GetRepositoriesAction.INSTANCE, request, listener);
    }

    public GetRepositoriesRequestBuilder prepareGetRepositories(TimeValue masterNodeTimeout, String... name) {
        return new GetRepositoriesRequestBuilder(this, masterNodeTimeout, name);
    }

    public CleanupRepositoryRequestBuilder prepareCleanupRepository(TimeValue masterNodeTimeout, TimeValue ackTimeout, String repository) {
        return new CleanupRepositoryRequestBuilder(this, masterNodeTimeout, ackTimeout, repository);
    }

    public void cleanupRepository(CleanupRepositoryRequest request, ActionListener<CleanupRepositoryResponse> listener) {
        execute(TransportCleanupRepositoryAction.TYPE, request, listener);
    }

    public void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener) {
        execute(VerifyRepositoryAction.INSTANCE, request, listener);
    }

    public VerifyRepositoryRequestBuilder prepareVerifyRepository(TimeValue masterNodeTimeout, TimeValue ackTimeout, String name) {
        return new VerifyRepositoryRequestBuilder(this, masterNodeTimeout, ackTimeout, name);
    }

    public ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request) {
        return execute(TransportCreateSnapshotAction.TYPE, request);
    }

    public void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener) {
        execute(TransportCreateSnapshotAction.TYPE, request, listener);
    }

    public CreateSnapshotRequestBuilder prepareCreateSnapshot(TimeValue masterNodeTimeout, String repository, String name) {
        return new CreateSnapshotRequestBuilder(this, masterNodeTimeout, repository, name);
    }

    public CloneSnapshotRequestBuilder prepareCloneSnapshot(TimeValue masterNodeTimeout, String repository, String source, String target) {
        return new CloneSnapshotRequestBuilder(this, masterNodeTimeout, repository, source, target);
    }

    public void cloneSnapshot(CloneSnapshotRequest request, ActionListener<AcknowledgedResponse> listener) {
        execute(TransportCloneSnapshotAction.TYPE, request, listener);
    }

    public void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener) {
        execute(TransportGetSnapshotsAction.TYPE, request, listener);
    }

    public GetSnapshotsRequestBuilder prepareGetSnapshots(TimeValue masterNodeTimeout, String... repositories) {
        return new GetSnapshotsRequestBuilder(this, masterNodeTimeout, repositories);
    }

    public void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<AcknowledgedResponse> listener) {
        execute(TransportDeleteSnapshotAction.TYPE, request, listener);
    }

    public DeleteSnapshotRequestBuilder prepareDeleteSnapshot(TimeValue masterNodeTimeout, String repository, String... names) {
        return new DeleteSnapshotRequestBuilder(this, masterNodeTimeout, repository, names);
    }

    public ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request) {
        return execute(TransportRestoreSnapshotAction.TYPE, request);
    }

    public void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener) {
        execute(TransportRestoreSnapshotAction.TYPE, request, listener);
    }

    public RestoreSnapshotRequestBuilder prepareRestoreSnapshot(TimeValue masterNodeTimeout, String repository, String snapshot) {
        return new RestoreSnapshotRequestBuilder(this, masterNodeTimeout, repository, snapshot);
    }

    public void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener) {
        execute(TransportSnapshotsStatusAction.TYPE, request, listener);
    }

    public SnapshotsStatusRequestBuilder prepareSnapshotStatus(TimeValue masterNodeTimeout, String repository) {
        return new SnapshotsStatusRequestBuilder(this, masterNodeTimeout, repository);
    }

    public SnapshotsStatusRequestBuilder prepareSnapshotStatus(TimeValue masterNodeTimeout) {
        return new SnapshotsStatusRequestBuilder(this, masterNodeTimeout);
    }

    public void putPipeline(PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
        execute(PutPipelineTransportAction.TYPE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> putPipeline(PutPipelineRequest request) {
        return execute(PutPipelineTransportAction.TYPE, request);
    }

    public PutPipelineRequestBuilder preparePutPipeline(String id, BytesReference source, XContentType xContentType) {
        return new PutPipelineRequestBuilder(this, id, source, xContentType);
    }

    public void deletePipeline(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
        execute(DeletePipelineTransportAction.TYPE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> deletePipeline(DeletePipelineRequest request) {
        return execute(DeletePipelineTransportAction.TYPE, request);
    }

    public DeletePipelineRequestBuilder prepareDeletePipeline(String id) {
        return new DeletePipelineRequestBuilder(this, id);
    }

    public void getPipeline(GetPipelineRequest request, ActionListener<GetPipelineResponse> listener) {
        execute(GetPipelineAction.INSTANCE, request, listener);
    }

    public GetPipelineRequestBuilder prepareGetPipeline(String... ids) {
        return new GetPipelineRequestBuilder(this, ids);
    }

    public void simulatePipeline(SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener) {
        execute(SimulatePipelineAction.INSTANCE, request, listener);
    }

    public ActionFuture<SimulatePipelineResponse> simulatePipeline(SimulatePipelineRequest request) {
        return execute(SimulatePipelineAction.INSTANCE, request);
    }

    public SimulatePipelineRequestBuilder prepareSimulatePipeline(BytesReference source, XContentType xContentType) {
        return new SimulatePipelineRequestBuilder(this, source, xContentType);
    }
}
