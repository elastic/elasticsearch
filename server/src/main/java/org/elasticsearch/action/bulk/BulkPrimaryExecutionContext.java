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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;

/**
 * This is a utility class that holds the per request state needed to perform bulk operations on the primary.
 * More specifically, it maintains an index to the current executing bulk item, which allows execution
 * to stop and wait for external events such as mapping updates.
 */
class BulkPrimaryExecutionContext {

    enum ItemProcessingState {
        /** Item execution is ready to start, no operations have been performed yet */
        INITIAL,
        /**
         * The incoming request has been translated to a request that can be executed on the shard.
         * This is used to convert update requests to a fully specified index or delete requests.
         */
        TRANSLATED,
        /**
         * the request can not execute with the current mapping and should wait for a new mapping
         * to arrive from the master. A mapping request for the needed changes has already been
         * submitted
         */
        WAIT_FOR_MAPPING_UPDATE,
        /**
         * The request should be executed again, but there is no need to wait for an external event.
         * This is needed to support retry on conflicts during updates.
         */
        IMMEDIATE_RETRY,
        /** The request has been executed on the primary shard (successfully or not) */
        EXECUTED,
        /**
         * No further handling of current request is needed. The result has been converted to a user response
         * and execution can continue to the next item (if available).
         */
        COMPLETED
    }

    private final BulkShardRequest request;
    private final IndexShard primary;
    private Translog.Location locationToSync = null;
    private int currentIndex = -1;

    private ItemProcessingState currentItemState;
    private DocWriteRequest requestToExecute;
    private BulkItemResponse executionResult;
    private int retryCounter;


    BulkPrimaryExecutionContext(BulkShardRequest request, IndexShard primary) {
        this.request = request;
        this.primary = primary;
        advance();
    }


    private int findNextNonAborted(int startIndex) {
        final int length = request.items().length;
        while (startIndex < length && isAborted(request.items()[startIndex].getPrimaryResponse())) {
            startIndex++;
        }
        return startIndex;
    }

    /** move to the next item to execute */
    private void advance() {
        assert currentItemState == ItemProcessingState.COMPLETED || currentIndex == -1 :
            "moving to next but current item wasn't completed (state: " + currentItemState + ")";
        currentItemState = ItemProcessingState.INITIAL;
        currentIndex =  findNextNonAborted(currentIndex + 1);
        retryCounter = 0;
        requestToExecute = null;
        executionResult = null;
    }

    /** gets the current, untranslated item request */
    public DocWriteRequest<?> getCurrent() {
        return getCurrentItem().request();
    }

    public BulkShardRequest getBulkShardRequest() {
        return request;
    }

    /** returns the result of the request that has been executed on the shard */
    public BulkItemResponse getExecutionResult() {
        assert currentItemState == ItemProcessingState.EXECUTED : currentItemState;
        return executionResult;
    }

    /** returns the number of times the current operation has been retried */
    public int getRetryCounter() {
        return retryCounter;
    }

    /** returns true if the current request has been executed on the primary */
    public boolean isOperationExecuted() {
        return currentItemState == ItemProcessingState.EXECUTED;
    }

    /** returns true if the request needs to wait for a mapping update to arrive from the master */
    public boolean requiresWaitingForMappingUpdate() {
        return currentItemState == ItemProcessingState.WAIT_FOR_MAPPING_UPDATE;
    }

    /** returns true if the current request should be retried without waiting for an external event */
    public boolean requiresImmediateRetry() {
        return currentItemState == ItemProcessingState.IMMEDIATE_RETRY;
    }

    /**
     * returns true if the current request has been completed and it's result translated to a user
     * facing response
     */
    public boolean isCompleted() {
        return currentItemState == ItemProcessingState.COMPLETED;
    }

    /**
     * returns true if the current request is in INITIAL state
     */
    public boolean isInitial() {
        return currentItemState == ItemProcessingState.INITIAL;
    }

    /**
     * returns true if {@link #advance()} has moved the current item beyond the
     * end of the {@link BulkShardRequest#items()} array.
     */
    public boolean hasMoreOperationsToExecute() {
        return currentIndex < request.items().length;
    }


    /** returns the name of the index the current request used */
    public String getConcreteIndex() {
        return getCurrentItem().index();
    }

    /** returns any primary response that was set by a previous primary */
    public BulkItemResponse getPreviousPrimaryResponse() {
        return getCurrentItem().getPrimaryResponse();
    }

    /** returns a translog location that is needed to be synced in order to persist all operations executed so far */
    public Translog.Location getLocationToSync() {
        assert hasMoreOperationsToExecute() == false;
        return locationToSync;
    }

    private BulkItemRequest getCurrentItem() {
        return request.items()[currentIndex];
    }

    /** returns the primary shard */
    public IndexShard getPrimary() {
        return primary;
    }

    /**
     * sets the request that should actually be executed on the primary. This can be different then the request
     * received from the user (specifically, an update request is translated to an indexing or delete request).
     */
    public void setRequestToExecute(DocWriteRequest writeRequest) {
        assert currentItemState != ItemProcessingState.TRANSLATED &&
            currentItemState != ItemProcessingState.EXECUTED &&
            currentItemState != ItemProcessingState.COMPLETED :
            currentItemState;
        assert requestToExecute == null : requestToExecute;
        requestToExecute = writeRequest;
        currentItemState = ItemProcessingState.TRANSLATED;
    }

    /** returns the request that should be executed on the shard. */
    public <T extends DocWriteRequest<T>> T getRequestToExecute() {
        assert currentItemState == ItemProcessingState.TRANSLATED : currentItemState;
        return (T) requestToExecute;
    }

    /** indicates that the current operation can not be completed and needs to wait for a new mapping from the master */
    public void markAsRequiringMappingUpdate() {
        assert currentItemState == ItemProcessingState.TRANSLATED: currentItemState;
        currentItemState = ItemProcessingState.WAIT_FOR_MAPPING_UPDATE;
        requestToExecute = null;
        executionResult = null;
    }

    /** resets the current item state, prepare for a new execution */
    public void resetForExecutionAfterRetry() {
        assert currentItemState == ItemProcessingState.WAIT_FOR_MAPPING_UPDATE ||
            currentItemState == ItemProcessingState.IMMEDIATE_RETRY: currentItemState;
        currentItemState = ItemProcessingState.INITIAL;
    }

    /** completes the operation without doing anything on the primary */
    public void markOperationAsNoOp(DocWriteResponse response) {
        assert currentItemState != ItemProcessingState.EXECUTED &&
            currentItemState != ItemProcessingState.COMPLETED : currentItemState;
        assert executionResult == null;
        executionResult = new BulkItemResponse(getCurrentItem().id(), getCurrentItem().request().opType(), response);
        currentItemState = ItemProcessingState.EXECUTED;
    }

    /** indicates that the operation needs to be failed as the required mapping didn't arrive in time */
    public void failOnMappingUpdate(Exception cause) {
        assert currentItemState == ItemProcessingState.WAIT_FOR_MAPPING_UPDATE : currentItemState;
        assert executionResult == null : executionResult;
        currentItemState = ItemProcessingState.EXECUTED;
        final DocWriteRequest docWriteRequest = getCurrentItem().request();
        markAsCompleted(new BulkItemResponse(getCurrentItem().id(), docWriteRequest.opType(),
            // Make sure to use request.index() here, if you use docWriteRequest.index() it will use the
            // concrete index instead of an alias if used!
            new BulkItemResponse.Failure(getCurrentItem().index(), docWriteRequest.type(), docWriteRequest.id(), cause)));
    }

    /** the current operation has been executed on the primary with the specified result */
    public void markOperationAsExecuted(Engine.Result result) {
        assert currentItemState == ItemProcessingState.TRANSLATED: currentItemState;
        assert executionResult == null : executionResult;
        final BulkItemRequest current = getCurrentItem();
        DocWriteRequest docWriteRequest = getRequestToExecute();
        switch (result.getResultType()) {
            case SUCCESS:
                final DocWriteResponse response;
                if (result.getOperationType() == Engine.Operation.TYPE.INDEX) {
                    Engine.IndexResult indexResult = (Engine.IndexResult) result;
                    response = new IndexResponse(primary.shardId(), requestToExecute.type(), requestToExecute.id(),
                        result.getSeqNo(), result.getTerm(), indexResult.getVersion(), indexResult.isCreated());
                } else if (result.getOperationType() == Engine.Operation.TYPE.DELETE) {
                    Engine.DeleteResult deleteResult = (Engine.DeleteResult) result;
                    response = new DeleteResponse(primary.shardId(), requestToExecute.type(), requestToExecute.id(),
                        deleteResult.getSeqNo(), result.getTerm(), deleteResult.getVersion(), deleteResult.isFound());

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
        currentItemState = ItemProcessingState.EXECUTED;
    }

    /** indicates that the current requests needs to be executed again without any need to wait on an external event */
    public void markForImmediateRetry() {
        assert currentItemState == ItemProcessingState.EXECUTED : currentItemState;
        currentItemState = ItemProcessingState.IMMEDIATE_RETRY;
        retryCounter++;
        requestToExecute = null;
        executionResult = null;
    }

    /** finishes the execution of the current request, with the response that should be returned to the user */
    public void markAsCompleted(BulkItemResponse translatedResponse) {
        assert currentItemState == ItemProcessingState.EXECUTED : currentItemState;
        assert translatedResponse.getItemId() == executionResult.getItemId();
        assert translatedResponse.getItemId() == getCurrentItem().id();

        if (translatedResponse.isFailed() == false && requestToExecute != getCurrent())  {
            request.items()[currentIndex] = new BulkItemRequest(request.items()[currentIndex].id(), requestToExecute);
        }
        getCurrentItem().setPrimaryResponse(translatedResponse);
        currentItemState = ItemProcessingState.COMPLETED;
        advance();
    }

    /** builds the bulk shard response to return to the user */
    public BulkShardResponse buildShardResponse() {
        assert hasMoreOperationsToExecute() == false;
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
