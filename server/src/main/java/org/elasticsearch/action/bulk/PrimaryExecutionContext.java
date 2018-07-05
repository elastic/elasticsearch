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

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;

class PrimaryExecutionContext {

    enum ItemProcessingState {
        INITIAL,
        TRANSLATED,
        REQUIRES_WAITING_FOR_MAPPING_UPDATE,
        IMMEDIATE_RETRY,
        OPERATION_COMPLETED,
        FINALIZED
    }

    private final BulkShardRequest request;
    private final IndexShard primary;
    private Translog.Location locationToSync = null;
    private int currentIndex = -1;

    private ItemProcessingState currentItemState = ItemProcessingState.FINALIZED;
    private DocWriteRequest requestToExecute;
    private BulkItemResponse executionResult;
    private int retryCounter;


    PrimaryExecutionContext(BulkShardRequest request, IndexShard primary) {
        this.request = request;
        this.primary = primary;
    }


    private int findNextNonAborted(int startIndex) {
        final int length = request.items().length;
        while (startIndex < length && isAborted(request.items()[startIndex].getPrimaryResponse())) {
            startIndex++;
        }
        return startIndex;
    }

    public void advance() {
        assert currentItemState == ItemProcessingState.FINALIZED :
            "moving to next but current item wasn't completed (state: " + currentItemState + ")";
        currentItemState = ItemProcessingState.INITIAL;
        currentIndex =  findNextNonAborted(currentIndex + 1);
        retryCounter = 0;
        requestToExecute = null;
        executionResult = null;
    }

    public DocWriteRequest<?> getCurrent() {
        return getCurrentItem().request();
    }

    public BulkItemResponse getExecutionResult() {
        assert currentItemState == ItemProcessingState.OPERATION_COMPLETED : currentItemState;
        return executionResult;
    }

    public int getRetryCounter() {
        return retryCounter;
    }

    public boolean isOperationCompleted() {
        return currentItemState == ItemProcessingState.OPERATION_COMPLETED;
    }

    public boolean requiresWaitingForMappingUpdate() {
        return currentItemState == ItemProcessingState.REQUIRES_WAITING_FOR_MAPPING_UPDATE;
    }

    public boolean requiresImmediateRetry() {
        return currentItemState == ItemProcessingState.IMMEDIATE_RETRY;
    }

    public boolean isFinalized() {
        return currentItemState == ItemProcessingState.FINALIZED;
    }

    public boolean isAllFinalized() {
        return currentIndex == request.items().length;
    }


    public String getConcreteIndex() {
        return getCurrentItem().index();
    }

    public BulkItemResponse getPreviousPrimaryResponse() {
        return getCurrentItem().getPrimaryResponse();
    }


    public Translog.Location getLocationToSync() {
        return locationToSync;
    }

    private BulkItemRequest getCurrentItem() {
        return request.items()[currentIndex];
    }

    public IndexShard getPrimary() {
        return primary;
    }


    public void setRequestToExecute(DocWriteRequest writeRequest) {
        assert currentItemState != ItemProcessingState.TRANSLATED &&
            currentItemState != ItemProcessingState.OPERATION_COMPLETED &&
            currentItemState != ItemProcessingState.FINALIZED :
            currentItemState;
        assert requestToExecute == null : requestToExecute;
        requestToExecute = writeRequest;
        currentItemState = ItemProcessingState.TRANSLATED;
    }

    public <T extends DocWriteRequest<T>> T getRequestToExecute() {
        assert currentItemState == ItemProcessingState.TRANSLATED : currentItemState;
        return (T) requestToExecute;
    }

    public void markAsRequiringMappingUpdate() {
        assert currentItemState == ItemProcessingState.TRANSLATED: currentItemState;
        currentItemState = ItemProcessingState.REQUIRES_WAITING_FOR_MAPPING_UPDATE;
        requestToExecute = null;
        executionResult = null;
    }

    public void markOperationAsNoOp() {
        assert currentItemState != ItemProcessingState.OPERATION_COMPLETED &&
            currentItemState != ItemProcessingState.FINALIZED: currentItemState;
        assert executionResult == null;
        executionResult = new BulkItemResponse(getCurrentItem().id(), getCurrentItem().request().opType(), (BulkItemResponse.Failure)null);
        currentItemState = ItemProcessingState.OPERATION_COMPLETED;
    }

    public void failOnMappingUpdateTimeout() {
        assert currentItemState == ItemProcessingState.REQUIRES_WAITING_FOR_MAPPING_UPDATE : currentItemState;
        assert executionResult == null : executionResult;
        currentItemState = ItemProcessingState.OPERATION_COMPLETED;
        final DocWriteRequest docWriteRequest = getCurrentItem().request();
        finalize(new BulkItemResponse(getCurrentItem().id(), docWriteRequest.opType(),
            // Make sure to use request.index() here, if you
            // use docWriteRequest.index() it will use the
            // concrete index instead of an alias if used!
            new BulkItemResponse.Failure(getCurrentItem().index(), docWriteRequest.type(), docWriteRequest.id(),
                new MapperException("timed out while waiting for a dynamic mapping update"))));

    }

    public void markOperationAsCompleted(Engine.Result result) {
        assert currentItemState == ItemProcessingState.TRANSLATED: currentItemState;
        assert executionResult == null : executionResult;
        final BulkItemRequest current = getCurrentItem();
        DocWriteRequest docWriteRequest = current.request();
        switch (result.getResultType()) {
            case SUCCESS:
                final DocWriteResponse response;
                if (result.getOperationType() == Engine.Operation.TYPE.INDEX) {
                    Engine.IndexResult indexResult = (Engine.IndexResult) result;
                    response = new IndexResponse(primary.shardId(), requestToExecute.type(), requestToExecute.id(),
                        result.getSeqNo(), primary.getPrimaryTerm(), indexResult.getVersion(), indexResult.isCreated());
                } else if (result.getOperationType() == Engine.Operation.TYPE.DELETE) {
                    Engine.DeleteResult deleteResult = (Engine.DeleteResult) result;
                    response = new DeleteResponse(primary.shardId(), requestToExecute.type(), requestToExecute.id(),
                        deleteResult.getSeqNo(), primary.getPrimaryTerm(), deleteResult.getVersion(), deleteResult.isFound());

                } else {
                    throw new AssertionError("unknown result type :" + result.getResultType());
                }
                executionResult = new BulkItemResponse(current.id(), current.request().opType(), response);
                // set a blank ShardInfo so we can safely send it to the replicas. We won't use it in the real response though.
                executionResult.getResponse().setShardInfo(new ReplicationResponse.ShardInfo());
                locationToSync = TransportWriteAction.locationToSync(locationToSync, result.getTranslogLocation());
                break;
            case FAILURE:
                executionResult = new BulkItemResponse(current.id(), docWriteRequest.opType(),
                    // Make sure to use request.index() here, if you
                    // use docWriteRequest.index() it will use the
                    // concrete index instead of an alias if used!
                    new BulkItemResponse.Failure(request.index(), docWriteRequest.type(), docWriteRequest.id(),
                        result.getFailure(), result.getSeqNo()));
                break;
            default:
                throw new AssertionError("unknown result type for " + getCurrentItem() + ": " + result.getResultType());
        }
        currentItemState = ItemProcessingState.OPERATION_COMPLETED;
    }

    public void markForImmediateRetry() {
        assert currentItemState == ItemProcessingState.OPERATION_COMPLETED: currentItemState;
        currentItemState = ItemProcessingState.IMMEDIATE_RETRY;
        retryCounter++;
        requestToExecute = null;
        executionResult = null;
    }

    public void finalize(BulkItemResponse translatedResponse) {
        assert currentItemState == ItemProcessingState.OPERATION_COMPLETED : currentItemState;
        if (translatedResponse.isFailed() == false && requestToExecute != getCurrent())  {
            request.items()[currentIndex] = new BulkItemRequest(request.items()[currentIndex].id(), requestToExecute);
        }
        getCurrentItem().setPrimaryResponse(translatedResponse);
        currentItemState = ItemProcessingState.FINALIZED;
    }

    public BulkShardResponse buildShardResponse() {
        assert isAllFinalized();
        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        BulkItemRequest[] items = request.items();
        for (int i = 0; i < items.length; i++) {
            responses[i] = items[i].getPrimaryResponse();
        }
        return new BulkShardResponse(request.shardId(), responses);
    }

    private static boolean isAborted(BulkItemResponse response) {
        return response != null && response.isFailed() && response.getFailure().isAborted();
    }
}
