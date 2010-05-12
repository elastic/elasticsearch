/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.action.admin.cluster.node.restart.NodesRestartRequest;
import org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.ping.broadcast.BroadcastPingRequest;
import org.elasticsearch.action.admin.cluster.ping.replication.ReplicationPingRequest;
import org.elasticsearch.action.admin.cluster.ping.single.SinglePingRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.terms.TermsRequest;
import org.elasticsearch.util.xcontent.XContentType;

/**
 * A handy one stop shop for creating requests (make sure to import static this class).
 *
 * @author kimchy (shay.banon)
 */
public class Requests {

    /**
     * The content type used to generate request builders (query / search).
     */
    public static XContentType CONTENT_TYPE = XContentType.XSON;

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
     * Creates a delete by query request. Note, the query itself must be set either by setting the JSON source
     * of the query, or by using a {@link org.elasticsearch.index.query.QueryBuilder} (using {@link org.elasticsearch.index.query.xcontent.QueryBuilders}).
     *
     * @param indices The indices the delete by query against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The delete by query request
     * @see org.elasticsearch.client.Client#deleteByQuery(org.elasticsearch.action.deletebyquery.DeleteByQueryRequest)
     */
    public static DeleteByQueryRequest deleteByQueryRequest(String... indices) {
        return new DeleteByQueryRequest(indices);
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
     * Creates a count request which counts the hits matched against a query. Note, the query itself must be set
     * either using the JSON source of the query, or using a {@link org.elasticsearch.index.query.QueryBuilder} (using {@link org.elasticsearch.index.query.xcontent.QueryBuilders}).
     *
     * @param indices The indices the delete by query against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The count request
     * @see org.elasticsearch.client.Client#count(org.elasticsearch.action.count.CountRequest)
     */
    public static CountRequest countRequest(String... indices) {
        return new CountRequest(indices);
    }

    /**
     * Terms request represent a request to get terms in one or more indices of specific fields and their
     * document frequencies (in how many document each term exists).
     *
     * @param indices The indices the delete by query against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The terms request
     * @see org.elasticsearch.client.Client#terms(org.elasticsearch.action.terms.TermsRequest)
     */
    public static TermsRequest termsRequest(String... indices) {
        return new TermsRequest(indices);
    }

    /**
     * More like this request represents a request to search for documents that are "like" the provided (fetched)
     * document.
     *
     * @param index The index to load the document from
     * @return The more like this request
     * @see org.elasticsearch.client.Client#moreLikeThis(org.elasticsearch.action.mlt.MoreLikeThisRequest)
     */
    public static MoreLikeThisRequest moreLikeThisRequest(String index) {
        return new MoreLikeThisRequest(index);
    }

    /**
     * Creates a search request against one or more indices. Note, the search source must be set either using the
     * actual JSON search source, or the {@link org.elasticsearch.search.builder.SearchSourceBuilder}.
     *
     * @param indices The indices the delete by query against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
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

    /**
     * Creates an indices status request.
     *
     * @param indices The indices the delete by query against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The indices status request
     * @see org.elasticsearch.client.IndicesAdminClient#status(org.elasticsearch.action.admin.indices.status.IndicesStatusRequest)
     */
    public static IndicesStatusRequest indicesStatus(String... indices) {
        return new IndicesStatusRequest(indices);
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
     * Create a create mapping request against one or more indices.
     *
     * @param indices The indices the delete by query against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
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
     * @param indices The indices the delete by query against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The refresh request
     * @see org.elasticsearch.client.IndicesAdminClient#refresh(org.elasticsearch.action.admin.indices.refresh.RefreshRequest)
     */
    public static RefreshRequest refreshRequest(String... indices) {
        return new RefreshRequest(indices);
    }

    /**
     * Creates a flush indices request.
     *
     * @param indices The indices the delete by query against. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The flush request
     * @see org.elasticsearch.client.IndicesAdminClient#flush(org.elasticsearch.action.admin.indices.flush.FlushRequest)
     */
    public static FlushRequest flushRequest(String... indices) {
        return new FlushRequest(indices);
    }

    /**
     * Creates an optimize request.
     *
     * @param indices The indices to optimize. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The optimize request
     * @see org.elasticsearch.client.IndicesAdminClient#optimize(org.elasticsearch.action.admin.indices.optimize.OptimizeRequest)
     */
    public static OptimizeRequest optimizeRequest(String... indices) {
        return new OptimizeRequest(indices);
    }

    /**
     * Creates a gateway snapshot indices request.
     *
     * @param indices The indices the gateway snapshot will be performed on. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The gateway snapshot request
     * @see org.elasticsearch.client.IndicesAdminClient#gatewaySnapshot(org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest)
     */
    public static GatewaySnapshotRequest gatewaySnapshotRequest(String... indices) {
        return new GatewaySnapshotRequest(indices);
    }

    /**
     * Creates a clean indices cache request.
     *
     * @param indices The indices the gateway snapshot will be performed on. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The request
     */
    public static ClearIndicesCacheRequest clearIndicesCache(String... indices) {
        return new ClearIndicesCacheRequest(indices);
    }

    /**
     * Creates a cluster state request.
     *
     * @return The cluster state request.
     * @see org.elasticsearch.client.ClusterAdminClient#state(org.elasticsearch.action.admin.cluster.state.ClusterStateRequest)
     */
    public static ClusterStateRequest clusterState() {
        return new ClusterStateRequest();
    }

    /**
     * Creates a cluster health request.
     *
     * @param indices The indices to provide additional cluster health information for. Use <tt>null</tt> or <tt>_all</tt> to execute against all indices
     * @return The cluster health request
     * @see org.elasticsearch.client.ClusterAdminClient#health(org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest)
     */
    public static ClusterHealthRequest clusterHealth(String... indices) {
        return new ClusterHealthRequest(indices);
    }

    /**
     * Creates a nodes info request against all the nodes.
     *
     * @return The nodes info request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesInfo(org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest)
     */
    public static NodesInfoRequest nodesInfo() {
        return new NodesInfoRequest();
    }

    /**
     * Creates a nodes info request against one or more nodes. Pass <tt>null</tt> or an empty array for all nodes.
     *
     * @param nodesIds The nodes ids to get the status for
     * @return The nodes info request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesStats(org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest)
     */
    public static NodesInfoRequest nodesInfo(String... nodesIds) {
        return new NodesInfoRequest(nodesIds);
    }

    /**
     * Creates a nodes stats request against one or more nodes. Pass <tt>null</tt> or an empty array for all nodes.
     *
     * @param nodesIds The nodes ids to get the stats for
     * @return The nodes info request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesStats(org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest)
     */
    public static NodesStatsRequest nodesStats(String... nodesIds) {
        return new NodesStatsRequest(nodesIds);
    }

    /**
     * Shuts down all nodes in the cluster.
     */
    public static NodesShutdownRequest nodesShutdown() {
        return new NodesShutdownRequest();
    }

    /**
     * Shuts down the specified nodes in the cluster.
     *
     * @param nodesIds The nodes ids to get the status for
     * @return The nodes info request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesShutdown(org.elasticsearch.action.admin.cluster.node.shutdown.NodesShutdownRequest)
     */
    public static NodesShutdownRequest nodesShutdown(String... nodesIds) {
        return new NodesShutdownRequest(nodesIds);
    }

    /**
     * Restarts all nodes in the cluster.
     */
    public static NodesRestartRequest nodesRestart() {
        return new NodesRestartRequest();
    }

    /**
     * Restarts specific nodes in the cluster.
     *
     * @param nodesIds The nodes ids to restart
     * @return The nodes info request
     * @see org.elasticsearch.client.ClusterAdminClient#nodesRestart(org.elasticsearch.action.admin.cluster.node.restart.NodesRestartRequest)
     */
    public static NodesRestartRequest nodesRestart(String... nodesIds) {
        return new NodesRestartRequest(nodesIds);
    }

    public static SinglePingRequest pingSingleRequest(String index) {
        return new SinglePingRequest(index);
    }

    public static BroadcastPingRequest pingBroadcastRequest(String... indices) {
        return new BroadcastPingRequest(indices);
    }

    public static ReplicationPingRequest pingReplicationRequest(String... indices) {
        return new ReplicationPingRequest(indices);
    }
}
