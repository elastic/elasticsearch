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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.translog.Translog;

public class SingleWriteOperationUtility {

    public static <T extends DocWriteResponse> ActionListener<BulkResponse> wrapBulkResponse(ActionListener<T> listener) {
        return ActionListener.wrap(bulkItemResponses -> {
            assert bulkItemResponses.getItems().length == 1 : "expected only one item in bulk request";
            BulkItemResponse bulkItemResponse = bulkItemResponses.getItems()[0];
            if (bulkItemResponse.isFailed() == false) {
                listener.onResponse(bulkItemResponse.getResponse());
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

    public static final class ResultHolder<Response extends ReplicationResponse & WriteResponse> {
        public final Response response;
        public final Translog.Location location;
        public final Exception failure;

        public ResultHolder(Response response, Translog.Location location, Exception failure) {
            this.response = response;
            this.location = location;
            this.failure = failure;
        }
    }

    public static <Response extends ReplicationResponse & WriteResponse> ResultHolder<Response>
    executeSingleItemBulkRequestOnPrimary(ReplicatedWriteRequest request,
                                          ThrowableFunction<BulkShardRequest, Tuple<BulkShardResponse,
                                                  Translog.Location>> executeShardBulkAction) throws Exception {
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        WriteRequest.RefreshPolicy refreshPolicy = request.getRefreshPolicy();
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        itemRequests[0] = new BulkItemRequest(0, ((DocWriteRequest) request));
        BulkShardRequest bulkShardRequest = new BulkShardRequest(request.shardId(), refreshPolicy, itemRequests);
        Tuple<BulkShardResponse, Translog.Location> responseLocationTuple = executeShardBulkAction.apply(bulkShardRequest);
        BulkShardResponse bulkShardResponse = responseLocationTuple.v1();
        assert bulkShardResponse.getResponses().length == 1: "expected only one bulk shard response";
        BulkItemResponse itemResponse = bulkShardResponse.getResponses()[0];
        final Response response;
        final Exception failure;
        if (itemResponse.isFailed()) {
            failure = itemResponse.getFailure().getCause();
            response = null;
        } else {
            response = (Response) itemResponse.getResponse();
            failure = null;
        }
        return new ResultHolder<>(response, responseLocationTuple.v2(), failure);
    }

    public static ResultHolder executeSingleItemBulkRequestOnReplica(ReplicatedWriteRequest replicaRequest,
                                                                     ThrowableFunction<BulkShardRequest,
                                                                             Translog.Location> executeShardBulkAction) throws Exception {
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        WriteRequest.RefreshPolicy refreshPolicy = replicaRequest.getRefreshPolicy();
        itemRequests[0] = new BulkItemRequest(0, ((DocWriteRequest) replicaRequest));
        BulkShardRequest bulkShardRequest = new BulkShardRequest(replicaRequest.shardId(), refreshPolicy, itemRequests);
        Translog.Location location = executeShardBulkAction.apply(bulkShardRequest);
        return new ResultHolder<>(null, location, null);
    }

    public interface ThrowableFunction<T, R> {
        R apply(T t) throws Exception;
    }
}
