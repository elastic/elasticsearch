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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

/** use transport bulk action directly */
@Deprecated
public abstract class TransportSingleItemBulkWriteAction<
    Request extends ReplicatedWriteRequest<Request>,
    Response extends ReplicationResponse & WriteResponse
    > extends TransportWriteAction<Request, Request, Response> {

    private final TransportBulkAction bulkAction;
    private final TransportShardBulkAction shardBulkAction;


    protected TransportSingleItemBulkWriteAction(Settings settings, String actionName, TransportService transportService,
                                                 ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                                 ShardStateAction shardStateAction, ActionFilters actionFilters,
                                                 IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
                                                 Supplier<Request> replicaRequest, String executor,
                                                 TransportBulkAction bulkAction, TransportShardBulkAction shardBulkAction) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            indexNameExpressionResolver, request, replicaRequest, executor);
        this.bulkAction = bulkAction;
        this.shardBulkAction = shardBulkAction;
    }


    @Override
    protected void doExecute(Task task, final Request request, final ActionListener<Response> listener) {
        bulkAction.execute(task, toSingleItemBulkRequest(request), wrapBulkResponse(listener));
    }

    @Override
    protected WritePrimaryResult<Request, Response> shardOperationOnPrimary(
        Request request, final IndexShard primary) throws Exception {
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        WriteRequest.RefreshPolicy refreshPolicy = request.getRefreshPolicy();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        itemRequests[0] = new BulkItemRequest(0, ((DocWriteRequest) request));
        BulkShardRequest bulkShardRequest = new BulkShardRequest(request.shardId(), refreshPolicy, itemRequests);
        WritePrimaryResult<BulkShardRequest, BulkShardResponse> bulkResult =
            shardBulkAction.shardOperationOnPrimary(bulkShardRequest, primary);
        assert bulkResult.finalResponseIfSuccessful.getResponses().length == 1 : "expected only one bulk shard response";
        BulkItemResponse itemResponse = bulkResult.finalResponseIfSuccessful.getResponses()[0];
        final Response response;
        final Exception failure;
        if (itemResponse.isFailed()) {
            failure = itemResponse.getFailure().getCause();
            response = null;
        } else {
            response = (Response) itemResponse.getResponse();
            failure = null;
        }
        return new WritePrimaryResult<>(request, response, bulkResult.location, failure, primary, logger);
    }

    @Override
    protected WriteReplicaResult<Request> shardOperationOnReplica(
        Request replicaRequest, IndexShard replica) throws Exception {
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        WriteRequest.RefreshPolicy refreshPolicy = replicaRequest.getRefreshPolicy();
        itemRequests[0] = new BulkItemRequest(0, ((DocWriteRequest) replicaRequest));
        BulkShardRequest bulkShardRequest = new BulkShardRequest(replicaRequest.shardId(), refreshPolicy, itemRequests);
        WriteReplicaResult<BulkShardRequest> result = shardBulkAction.shardOperationOnReplica(bulkShardRequest, replica);
        // a replica operation can never throw a document-level failure,
        // as the same document has been already indexed successfully in the primary
        return new WriteReplicaResult<>(replicaRequest, result.location, null, replica, logger);
    }


    public static <Response extends ReplicationResponse & WriteResponse>
    ActionListener<BulkResponse> wrapBulkResponse(ActionListener<Response> listener) {
        return ActionListener.wrap(bulkItemResponses -> {
            assert bulkItemResponses.getItems().length == 1 : "expected only one item in bulk request";
            BulkItemResponse bulkItemResponse = bulkItemResponses.getItems()[0];
            if (bulkItemResponse.isFailed() == false) {
                final DocWriteResponse response = bulkItemResponse.getResponse();
                listener.onResponse((Response) response);
            } else {
                listener.onFailure(bulkItemResponse.getFailure().getCause());
            }
        }, listener::onFailure);
    }

    public static BulkRequest toSingleItemBulkRequest(ReplicatedWriteRequest request) {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(((DocWriteRequest) request));
        bulkRequest.setRefreshPolicy(request.getRefreshPolicy());
        bulkRequest.timeout(request.timeout());
        bulkRequest.waitForActiveShards(request.waitForActiveShards());
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        return bulkRequest;
    }
}
