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

package org.elasticsearch.client;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.usage.NodesUsageRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * A handy one stop shop for creating requests (make sure to import static this class).
 */
public class Requests {

    /**
     * The content type used to generate request builders (query / search).
     */
    public static XContentType CONTENT_TYPE = XContentType.SMILE;

    /**
     * The default content type to use to generate source documents when indexing.
     */
    public static XContentType INDEX_CONTENT_TYPE = XContentType.JSON;

    public static IndexRequest indexRequest() {
        return new IndexRequest();
    }

    /**
     * Create an index request against a specific index. Note the {@link IndexRequest#type(String)} must be
     * set as well and optionally the {@link IndexRequest#id(String)}.
     *
     * @param index The index name to index the request against
     * @return The index request
     * @see org.elasticsearch.client.Client#index(org.elasticsearch.action.index.IndexRequest)
     */
    public static IndexRequest indexRequest(String index) {
        return new IndexRequest(index);
    }

    /**
     * Creates a delete request against a specific index. Note the {@link DeleteRequest#type(String)} and
     * {@link DeleteRequest#id(String)} must be set.
     *
     * @param index The index name to delete from
     * @return The delete request
     * @see org.elasticsearch.client.Client#delete(org.elasticsearch.action.delete.DeleteRequest)
     */
    public static DeleteRequest deleteRequest(String index) {
        return new DeleteRequest(index);
    }

    /**
     * Creates a new bulk request.
     */
    public static BulkRequest bulkRequest() {
        return new BulkRequest();
    }

    /**
     * Creates a get request to get the JSON source from an index based on a type and id. Note, the
     * {@link GetRequest#type(String)} and {@link GetRequest#id(String)} must be set.
     *
     * @param index The index to get the JSON source from
     * @return The get request
     * @see org.elasticsearch.client.Client#get(org.elasticsearch.action.get.GetRequest)
     */
    public static GetRequest getRequest(String index) {
        return new GetRequest(index);
    }

    /**
     * Creates a search request against one or more indices. Note, the search source must be set either using the
     * actual JSON search source, or the {@link org.elasticsearch.search.builder.SearchSourceBuilder}.
     *
     * @param indices The indices to search against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The search request
     * @see org.elasticsearch.client.Client#search(org.elasticsearch.action.search.SearchRequest)
     */
    public static SearchRequest searchRequest(String... indices) {
        return new SearchRequest(indices);
    }

    /**
     * Creates a search scroll request allowing to continue searching a previous search request.
     *
     * @param scrollId The scroll id representing the scrollable search
     * @return The search scroll request
     * @see org.elasticsearch.client.Client#searchScroll(org.elasticsearch.action.search.SearchScrollRequest)
     */
    public static SearchScrollRequest searchScrollRequest(String scrollId) {
        return new SearchScrollRequest(scrollId);
    }

    public static IndicesSegmentsRequest indicesSegmentsRequest(String... indices) {
        return new IndicesSegmentsRequest(indices);
    }

    /**
     * Creates an indices shard stores info request.
     * @param indices The indices to get shard store information on
     * @return The indices shard stores request
     * @see org.elasticsearch.client.IndicesAdminClient#shardStores(IndicesShardStoresRequest)
     */
    public static IndicesShardStoresRequest indicesShardStoresRequest(String... indices) {
        return new IndicesShardStoresRequest(indices);
    }
    /**
     * Creates an indices exists request.
     *
     * @param indices The indices to check if they exists or not.
     * @return The indices exists request
     * @see org.elasticsearch.client.IndicesAdminClient#exists(org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest)
     */
    public static IndicesExistsRequest indicesExistsRequest(String... indices) {
        return new IndicesExistsRequest(indices);
    }

    /**
     * Creates a create index request.
     *
     * @param index The index to create
     * @return The index create request
     * @see org.elasticsearch.client.IndicesAdminClient#create(org.elasticsearch.action.admin.indices.create.CreateIndexRequest)
     */
    public static CreateIndexRequest createIndexRequest(String index) {
        return new CreateIndexRequest(index);
    }

    /**
     * Creates a delete index request.
     *
     * @param index The index to delete
     * @return The delete index request
     * @see org.elasticsearch.client.IndicesAdminClient#delete(org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest)
     */
    public static DeleteIndexRequest deleteIndexRequest(String index) {
        return new DeleteIndexRequest(index);
    }

    /**
     * Creates a close index request.
     *
     * @param index The index to close
     * @return The delete index request
     * @see org.elasticsearch.client.IndicesAdminClient#close(org.elasticsearch.action.admin.indices.close.CloseIndexRequest)
     */
    public static CloseIndexRequest closeIndexRequest(String index) {
        return new CloseIndexRequest(index);
    }

    /**
     * Creates an open index request.
     *
     * @param index The index to open
     * @return The delete index request
     * @see org.elasticsearch.client.IndicesAdminClient#open(org.elasticsearch.action.admin.indices.open.OpenIndexRequest)
     */
    public static OpenIndexRequest openIndexRequest(String index) {
        return new OpenIndexRequest(index);
    }

    /**
     * Create a create mapping request against one or more indices.
     *
     * @param indices The indices to create mapping. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The create mapping request
     * @see org.elasticsearch.client.IndicesAdminClient#putMapping(org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest)
     */
    public static PutMappingRequest putMappingRequest(String... indices) {
        return new PutMappingRequest(indices);
    }

    /**
     * Creates an index aliases request allowing to add and remove aliases.
     *
     * @return The index aliases request
     */
    public static IndicesAliasesRequest indexAliasesRequest() {
        return new IndicesAliasesRequest();
    }

    /**
     * Creates a refresh indices request.
     *
     * @param indices The indices to refresh. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The refresh request
     * @see org.elasticsearch.client.IndicesAdminClient#refresh(org.elasticsearch.action.admin.indices.refresh.RefreshRequest)
     */
    public static RefreshRequest refreshRequest(String... indices) {
        return new RefreshRequest(indices);
    }

    /**
     * Creates a flush indices request.
     *
     * @param indices The indices to flush. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The flush request
     * @see org.elasticsearch.client.IndicesAdminClient#flush(org.elasticsearch.action.admin.indices.flush.FlushRequest)
     */
    public static FlushRequest flushRequest(String... indices) {
        return new FlushRequest(indices);
    }

    /**
     * Creates a synced flush indices request.
     *
     * @param indices The indices to sync flush. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The synced flush request
     * @see org.elasticsearch.client.IndicesAdminClient#syncedFlush(SyncedFlushRequest)
     */
    public static SyncedFlushRequest syncedFlushRequest(String... indices) {
        return new SyncedFlushRequest(indices);
    }

    /**
     * Creates a force merge request.
     *
     * @param indices The indices to force merge. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The force merge request
     * @see org.elasticsearch.client.IndicesAdminClient#forceMerge(org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest)
     */
    public static ForceMergeRequest forceMergeRequest(String... indices) {
        return new ForceMergeRequest(indices);
    }

    /**
     * Creates an upgrade request.
     *
     * @param indices The indices to upgrade. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The upgrade request
     * @see org.elasticsearch.client.IndicesAdminClient#upgrade(UpgradeRequest)
     */
    public static UpgradeRequest upgradeRequest(String... indices) {
        return new UpgradeRequest(indices);
    }

    /**
     * Creates a clean indices cache request.
     *
     * @param indices The indices to clean their caches. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The request
     */
    public static ClearIndicesCacheRequest clearIndicesCacheRequest(String... indices) {
        return new ClearIndicesCacheRequest(indices);
    }

    /**
     * A request to update indices settings.
     *
     * @param indices The indices to update the settings for. Use <tt>null</tt> or <tt>_all</tt> to executed against all indices.
     * @return The request
     */
    public static UpdateSettingsRequest updateSettingsRequest(String... indices) {
        return new UpdateSettingsRequest(indices);
    }

    /**
     * Creates a cluster state request.
     *
     * @return The cluster state request.
     * @see org.elasticsearch.client.ClusterAdminClient#state(org.elasticsearch.action.admin.cluster.state.ClusterStateRequest)
     */
    public static ClusterStateRequest clusterStateRequest() {
        return new ClusterStateRequest();
    }

    public static ClusterRerouteRequest clusterRerouteRequest() {
        return new ClusterRerouteRequest();
    }

    public static ClusterUpdateSettingsRequest clusterUpdateSettingsRequest() {
        return new ClusterUpdateSettingsRequest();
    }

    /**
     * Creates a cluster health request.
     *
     * @param indices The indices to provide additional cluster health information for.
     *                Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The cluster health request
     * @see org.elasticsearch.client.ClusterAdminClient#health(org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest)
     */
    public static ClusterHealthRequest clusterHealthRequest(String... indices) {
        return new ClusterHealthRequest(indices);
    }

    /**
     * List all shards for the give search
     */
    public static ClusterSearchShardsRequest clusterSearchShardsRequest() {
        return new ClusterSearchShardsRequest();
    }

    /**
     * List all shards for the give search
     */
    public static ClusterSearchShardsRequest clusterSearchShardsRequest(String... indices) {
        return new ClusterSearchShardsRequest(indices);
    }

    /**
     * Creates a nodes info request against all the nodes.
     *
     * @return The nodes info request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesInfo(org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest)
     */
    public static NodesInfoRequest nodesInfoRequest() {
        return new NodesInfoRequest();
    }

    /**
     * Creates a nodes info request against one or more nodes. Pass <tt>null</tt> or an empty array for all nodes.
     *
     * @param nodesIds The nodes ids to get the status for
     * @return The nodes info request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesStats(org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest)
     */
    public static NodesInfoRequest nodesInfoRequest(String... nodesIds) {
        return new NodesInfoRequest(nodesIds);
    }

    /**
     * Creates a nodes stats request against one or more nodes. Pass <tt>null</tt> or an empty array for all nodes.
     *
     * @param nodesIds The nodes ids to get the stats for
     * @return The nodes info request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesStats(org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest)
     */
    public static NodesStatsRequest nodesStatsRequest(String... nodesIds) {
        return new NodesStatsRequest(nodesIds);
    }

    /**
     * Creates a nodes usage request against one or more nodes. Pass
     * <tt>null</tt> or an empty array for all nodes.
     *
     * @param nodesIds
     *            The nodes ids to get the usage for
     * @return The nodes usage request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesUsage(org.elasticsearch.action.admin.cluster.node.usage.NodesUsageRequest)
     */
    public static NodesUsageRequest nodesUsageRequest(String... nodesIds) {
        return new NodesUsageRequest(nodesIds);
    }

    /**
     * Creates a cluster stats request.
     *
     * @return The cluster stats request
     * @see org.elasticsearch.client.ClusterAdminClient#clusterStats(org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest)
     */
    public static ClusterStatsRequest clusterStatsRequest() {
        return new ClusterStatsRequest();
    }

    /**
     * Creates a nodes tasks request against all the nodes.
     *
     * @return The nodes tasks request
     * @see org.elasticsearch.client.ClusterAdminClient#listTasks(ListTasksRequest)
     */
    public static ListTasksRequest listTasksRequest() {
        return new ListTasksRequest();
    }

    /**
     * Creates a get task request.
     *
     * @return The nodes tasks request
     * @see org.elasticsearch.client.ClusterAdminClient#getTask(GetTaskRequest)
     */
    public static GetTaskRequest getTaskRequest() {
        return new GetTaskRequest();
    }

    /**
     * Creates a nodes tasks request against one or more nodes. Pass <tt>null</tt> or an empty array for all nodes.
     *
     * @return The nodes tasks request
     * @see org.elasticsearch.client.ClusterAdminClient#cancelTasks(CancelTasksRequest)
     */
    public static CancelTasksRequest cancelTasksRequest() {
        return new CancelTasksRequest();
    }

    /**
     * Registers snapshot repository
     *
     * @param name repository name
     * @return repository registration request
     */
    public static PutRepositoryRequest putRepositoryRequest(String name) {
        return new PutRepositoryRequest(name);
    }

    /**
     * Gets snapshot repository
     *
     * @param repositories names of repositories
     * @return get repository request
     */
    public static GetRepositoriesRequest getRepositoryRequest(String... repositories) {
        return new GetRepositoriesRequest(repositories);
    }

    /**
     * Deletes registration for snapshot repository
     *
     * @param name repository name
     * @return delete repository request
     */
    public static DeleteRepositoryRequest deleteRepositoryRequest(String name) {
        return new DeleteRepositoryRequest(name);
    }

    /**
     * Verifies snapshot repository
     *
     * @param name repository name
     * @return repository verification request
     */
    public static VerifyRepositoryRequest verifyRepositoryRequest(String name) {
        return new VerifyRepositoryRequest(name);
    }


    /**
     * Creates new snapshot
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     * @return create snapshot request
     */
    public static CreateSnapshotRequest createSnapshotRequest(String repository, String snapshot) {
        return new CreateSnapshotRequest(repository, snapshot);
    }

    /**
     * Gets snapshots from repository
     *
     * @param repository repository name
     * @return get snapshot  request
     */
    public static GetSnapshotsRequest getSnapshotsRequest(String repository) {
        return new GetSnapshotsRequest(repository);
    }

    /**
     * Restores new snapshot
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     * @return snapshot creation request
     */
    public static RestoreSnapshotRequest restoreSnapshotRequest(String repository, String snapshot) {
        return new RestoreSnapshotRequest(repository, snapshot);
    }

    /**
     * Deletes a snapshot
     *
     * @param snapshot   snapshot name
     * @param repository repository name
     * @return delete snapshot request
     */
    public static DeleteSnapshotRequest deleteSnapshotRequest(String repository, String snapshot) {
        return new DeleteSnapshotRequest(repository, snapshot);
    }

    /**
     *  Get status of snapshots
     *
     * @param repository repository name
     * @return snapshot status request
     */
    public static SnapshotsStatusRequest snapshotsStatusRequest(String repository) {
        return new SnapshotsStatusRequest(repository);
    }

}
