/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.support;

import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.cluster.ClusterAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequestBuilder;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.elasticsearch.client.ClusterAdminClient;

/**
 *
 */
public abstract class AbstractClusterAdminClient implements ClusterAdminClient {

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, ClusterAdminClient>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder, ClusterAdminClient> action) {
        return action.newRequestBuilder(this);
    }

    @Override
    public ActionFuture<ClusterHealthResponse> health(final ClusterHealthRequest request) {
        return execute(ClusterHealthAction.INSTANCE, request);
    }

    @Override
    public void health(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
        execute(ClusterHealthAction.INSTANCE, request, listener);
    }

    @Override
    public ClusterHealthRequestBuilder prepareHealth(String... indices) {
        return new ClusterHealthRequestBuilder(this).setIndices(indices);
    }

    @Override
    public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
        return execute(ClusterStateAction.INSTANCE, request);
    }

    @Override
    public void state(final ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
        execute(ClusterStateAction.INSTANCE, request, listener);
    }

    @Override
    public ClusterStateRequestBuilder prepareState() {
        return new ClusterStateRequestBuilder(this);
    }

    @Override
    public ActionFuture<ClusterRerouteResponse> reroute(final ClusterRerouteRequest request) {
        return execute(ClusterRerouteAction.INSTANCE, request);
    }

    @Override
    public void reroute(final ClusterRerouteRequest request, final ActionListener<ClusterRerouteResponse> listener) {
        execute(ClusterRerouteAction.INSTANCE, request, listener);
    }

    @Override
    public ClusterRerouteRequestBuilder prepareReroute() {
        return new ClusterRerouteRequestBuilder(this);
    }

    @Override
    public ActionFuture<ClusterUpdateSettingsResponse> updateSettings(final ClusterUpdateSettingsRequest request) {
        return execute(ClusterUpdateSettingsAction.INSTANCE, request);
    }

    @Override
    public void updateSettings(final ClusterUpdateSettingsRequest request, final ActionListener<ClusterUpdateSettingsResponse> listener) {
        execute(ClusterUpdateSettingsAction.INSTANCE, request, listener);
    }

    @Override
    public ClusterUpdateSettingsRequestBuilder prepareUpdateSettings() {
        return new ClusterUpdateSettingsRequestBuilder(this);
    }

    @Override
    public ActionFuture<NodesInfoResponse> nodesInfo(final NodesInfoRequest request) {
        return execute(NodesInfoAction.INSTANCE, request);
    }

    @Override
    public void nodesInfo(final NodesInfoRequest request, final ActionListener<NodesInfoResponse> listener) {
        execute(NodesInfoAction.INSTANCE, request, listener);
    }

    @Override
    public NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds) {
        return new NodesInfoRequestBuilder(this).setNodesIds(nodesIds);
    }

    @Override
    public ActionFuture<NodesStatsResponse> nodesStats(final NodesStatsRequest request) {
        return execute(NodesStatsAction.INSTANCE, request);
    }

    @Override
    public void nodesStats(final NodesStatsRequest request, final ActionListener<NodesStatsResponse> listener) {
        execute(NodesStatsAction.INSTANCE, request, listener);
    }

    @Override
    public NodesStatsRequestBuilder prepareNodesStats(String... nodesIds) {
        return new NodesStatsRequestBuilder(this).setNodesIds(nodesIds);
    }

    @Override
    public ActionFuture<ClusterStatsResponse> clusterStats(ClusterStatsRequest request) {
        return execute(ClusterStatsAction.INSTANCE, request);
    }

    @Override
    public void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener) {
        execute(ClusterStatsAction.INSTANCE, request, listener);
    }

    @Override
    public ClusterStatsRequestBuilder prepareClusterStats() {
        return new ClusterStatsRequestBuilder(this);
    }

    @Override
    public ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request) {
        return execute(NodesHotThreadsAction.INSTANCE, request);
    }

    @Override
    public void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener) {
        execute(NodesHotThreadsAction.INSTANCE, request, listener);
    }

    @Override
    public NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds) {
        return new NodesHotThreadsRequestBuilder(this).setNodesIds(nodesIds);
    }

    @Override
    public ActionFuture<ClusterSearchShardsResponse> searchShards(final ClusterSearchShardsRequest request) {
        return execute(ClusterSearchShardsAction.INSTANCE, request);
    }

    @Override
    public void searchShards(final ClusterSearchShardsRequest request, final ActionListener<ClusterSearchShardsResponse> listener) {
        execute(ClusterSearchShardsAction.INSTANCE, request, listener);
    }

    @Override
    public ClusterSearchShardsRequestBuilder prepareSearchShards() {
        return new ClusterSearchShardsRequestBuilder(this);
    }

    @Override
    public ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices) {
        return new ClusterSearchShardsRequestBuilder(this).setIndices(indices);
    }

    @Override
    public PendingClusterTasksRequestBuilder preparePendingClusterTasks() {
        return new PendingClusterTasksRequestBuilder(this);
    }

    @Override
    public ActionFuture<PendingClusterTasksResponse> pendingClusterTasks(PendingClusterTasksRequest request) {
        return execute(PendingClusterTasksAction.INSTANCE, request);
    }

    @Override
    public void pendingClusterTasks(PendingClusterTasksRequest request, ActionListener<PendingClusterTasksResponse> listener) {
        execute(PendingClusterTasksAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<PutRepositoryResponse> putRepository(PutRepositoryRequest request) {
        return execute(PutRepositoryAction.INSTANCE, request);
    }

    @Override
    public void putRepository(PutRepositoryRequest request, ActionListener<PutRepositoryResponse> listener) {
        execute(PutRepositoryAction.INSTANCE, request, listener);
    }

    @Override
    public PutRepositoryRequestBuilder preparePutRepository(String name) {
        return new PutRepositoryRequestBuilder(this, name);
    }

    @Override
    public ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request) {
        return execute(CreateSnapshotAction.INSTANCE, request);
    }

    @Override
    public void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener) {
        execute(CreateSnapshotAction.INSTANCE, request, listener);
    }

    @Override
    public CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name) {
        return new CreateSnapshotRequestBuilder(this, repository, name);
    }

    @Override
    public ActionFuture<GetSnapshotsResponse> getSnapshots(GetSnapshotsRequest request) {
        return execute(GetSnapshotsAction.INSTANCE, request);
    }

    @Override
    public void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener) {
        execute(GetSnapshotsAction.INSTANCE, request, listener);
    }

    @Override
    public GetSnapshotsRequestBuilder prepareGetSnapshots(String repository) {
        return new GetSnapshotsRequestBuilder(this, repository);
    }


    @Override
    public ActionFuture<DeleteSnapshotResponse> deleteSnapshot(DeleteSnapshotRequest request) {
        return execute(DeleteSnapshotAction.INSTANCE, request);
    }

    @Override
    public void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<DeleteSnapshotResponse> listener) {
        execute(DeleteSnapshotAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String name) {
        return new DeleteSnapshotRequestBuilder(this, repository, name);
    }


    @Override
    public ActionFuture<DeleteRepositoryResponse> deleteRepository(DeleteRepositoryRequest request) {
        return execute(DeleteRepositoryAction.INSTANCE, request);
    }

    @Override
    public void deleteRepository(DeleteRepositoryRequest request, ActionListener<DeleteRepositoryResponse> listener) {
        execute(DeleteRepositoryAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteRepositoryRequestBuilder prepareDeleteRepository(String name) {
        return new DeleteRepositoryRequestBuilder(this, name);
    }

    @Override
    public ActionFuture<VerifyRepositoryResponse> verifyRepository(VerifyRepositoryRequest request) {
        return execute(VerifyRepositoryAction.INSTANCE, request);
    }

    @Override
    public void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener) {
        execute(VerifyRepositoryAction.INSTANCE, request, listener);
    }

    @Override
    public VerifyRepositoryRequestBuilder prepareVerifyRepository(String name) {
        return new VerifyRepositoryRequestBuilder(this, name);
    }

    @Override
    public ActionFuture<GetRepositoriesResponse> getRepositories(GetRepositoriesRequest request) {
        return execute(GetRepositoriesAction.INSTANCE, request);
    }

    @Override
    public void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener) {
        execute(GetRepositoriesAction.INSTANCE, request, listener);
    }

    @Override
    public GetRepositoriesRequestBuilder prepareGetRepositories(String... name) {
        return new GetRepositoriesRequestBuilder(this, name);
    }

    @Override
    public ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request) {
        return execute(RestoreSnapshotAction.INSTANCE, request);
    }

    @Override
    public void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener) {
        execute(RestoreSnapshotAction.INSTANCE, request, listener);
    }

    @Override
    public RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot) {
        return new RestoreSnapshotRequestBuilder(this, repository, snapshot);
    }


    @Override
    public ActionFuture<SnapshotsStatusResponse> snapshotsStatus(SnapshotsStatusRequest request) {
        return execute(SnapshotsStatusAction.INSTANCE, request);
    }

    @Override
    public void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener) {
        execute(SnapshotsStatusAction.INSTANCE, request, listener);
    }

    @Override
    public SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository) {
        return new SnapshotsStatusRequestBuilder(this, repository);
    }

    @Override
    public SnapshotsStatusRequestBuilder prepareSnapshotStatus() {
        return new SnapshotsStatusRequestBuilder(this);
    }
}
