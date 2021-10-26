/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

import java.util.Arrays;

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
    private DocWriteRequest<?> requestToExecute;
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

    private static boolean isAborted(BulkItemResponse response) {
        return response != null && response.isFailed() && response.getFailure().isAborted();
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
        assert assertInvariants(ItemProcessingState.INITIAL);
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
        assert assertInvariants(ItemProcessingState.EXECUTED);
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

    /** returns a translog location that is needed to be synced in order to persist all operations executed so far */
    public Translog.Location getLocationToSync() {
        assert hasMoreOperationsToExecute() == false;
        // we always get to the end of the list by using advance, which in turn sets the state to INITIAL
        assert assertInvariants(ItemProcessingState.INITIAL);
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
    public void setRequestToExecute(DocWriteRequest<?> writeRequest) {
        assert assertInvariants(ItemProcessingState.INITIAL);
        requestToExecute = writeRequest;
        currentItemState = ItemProcessingState.TRANSLATED;
        assert assertInvariants(ItemProcessingState.TRANSLATED);
    }

    /** returns the request that should be executed on the shard. */
    @SuppressWarnings("unchecked")
    public <T extends DocWriteRequest<T>> T getRequestToExecute() {
        assert assertInvariants(ItemProcessingState.TRANSLATED);
        return (T) requestToExecute;
    }

    /** indicates that the current operation can not be completed and needs to wait for a new mapping from the master */
    public void markAsRequiringMappingUpdate() {
        assert assertInvariants(ItemProcessingState.TRANSLATED);
        currentItemState = ItemProcessingState.WAIT_FOR_MAPPING_UPDATE;
        requestToExecute = null;
        assert assertInvariants(ItemProcessingState.WAIT_FOR_MAPPING_UPDATE);
    }

    /** resets the current item state, prepare for a new execution */
    public void resetForExecutionForRetry() {
        assertInvariants(ItemProcessingState.WAIT_FOR_MAPPING_UPDATE, ItemProcessingState.EXECUTED);
        currentItemState = ItemProcessingState.INITIAL;
        requestToExecute = null;
        executionResult = null;
        assertInvariants(ItemProcessingState.INITIAL);
    }

    /** completes the operation without doing anything on the primary */
    public void markOperationAsNoOp(DocWriteResponse response) {
        assertInvariants(ItemProcessingState.INITIAL);
        executionResult = BulkItemResponse.success(getCurrentItem().id(), getCurrentItem().request().opType(), response);
        currentItemState = ItemProcessingState.EXECUTED;
        assertInvariants(ItemProcessingState.EXECUTED);
    }

    /** indicates that the operation needs to be failed as the required mapping didn't arrive in time */
    public void failOnMappingUpdate(Exception cause) {
        assert assertInvariants(ItemProcessingState.WAIT_FOR_MAPPING_UPDATE);
        currentItemState = ItemProcessingState.EXECUTED;
        final DocWriteRequest<?> docWriteRequest = getCurrentItem().request();
        executionResult = BulkItemResponse.failure(getCurrentItem().id(), docWriteRequest.opType(),
            // Make sure to use getCurrentItem().index() here, if you use docWriteRequest.index() it will use the
            // concrete index instead of an alias if used!
            new BulkItemResponse.Failure(getCurrentItem().index(), docWriteRequest.id(), cause));
        markAsCompleted(executionResult);
    }

    /** the current operation has been executed on the primary with the specified result */
    public void markOperationAsExecuted(Engine.Result result) {
        assertInvariants(ItemProcessingState.TRANSLATED);
        final BulkItemRequest current = getCurrentItem();
        DocWriteRequest<?> docWriteRequest = getRequestToExecute();
        switch (result.getResultType()) {
            case SUCCESS:
                final DocWriteResponse response;
                if (result.getOperationType() == Engine.Operation.TYPE.INDEX) {
                    Engine.IndexResult indexResult = (Engine.IndexResult) result;
                    response = new IndexResponse(primary.shardId(), requestToExecute.id(),
                        result.getSeqNo(), result.getTerm(), indexResult.getVersion(), indexResult.isCreated());
                } else if (result.getOperationType() == Engine.Operation.TYPE.DELETE) {
                    Engine.DeleteResult deleteResult = (Engine.DeleteResult) result;
                    response = new DeleteResponse(primary.shardId(), requestToExecute.id(),
                        deleteResult.getSeqNo(), result.getTerm(), deleteResult.getVersion(), deleteResult.isFound());

                } else {
                    throw new AssertionError("unknown result type :" + result.getResultType());
                }
                executionResult = BulkItemResponse.success(current.id(), current.request().opType(), response);
                // set a blank ShardInfo so we can safely send it to the replicas. We won't use it in the real response though.
                executionResult.getResponse().setShardInfo(new ReplicationResponse.ShardInfo());
                locationToSync = TransportWriteAction.locationToSync(locationToSync, result.getTranslogLocation());
                break;
            case FAILURE:
                executionResult = BulkItemResponse.failure(current.id(), docWriteRequest.opType(),
                    // Make sure to use request.index() here, if you
                    // use docWriteRequest.index() it will use the
                    // concrete index instead of an alias if used!
                    new BulkItemResponse.Failure(request.index(), docWriteRequest.id(),
                        result.getFailure(), result.getSeqNo(), result.getTerm()));
                break;
            default:
                throw new AssertionError("unknown result type for " + getCurrentItem() + ": " + result.getResultType());
        }
        currentItemState = ItemProcessingState.EXECUTED;
    }

    /** finishes the execution of the current request, with the response that should be returned to the user */
    public void markAsCompleted(BulkItemResponse translatedResponse) {
        assertInvariants(ItemProcessingState.EXECUTED);
        assert executionResult != null && translatedResponse.getItemId() == executionResult.getItemId();
        assert translatedResponse.getItemId() == getCurrentItem().id();

        if (translatedResponse.isFailed() == false && requestToExecute != null && requestToExecute != getCurrent())  {
            request.items()[currentIndex] = new BulkItemRequest(request.items()[currentIndex].id(), requestToExecute);
        }
        getCurrentItem().setPrimaryResponse(translatedResponse);
        currentItemState = ItemProcessingState.COMPLETED;
        advance();
    }

    /** builds the bulk shard response to return to the user */
    public BulkShardResponse buildShardResponse() {
        assert hasMoreOperationsToExecute() == false;
        return new BulkShardResponse(request.shardId(),
            Arrays.stream(request.items()).map(BulkItemRequest::getPrimaryResponse).toArray(BulkItemResponse[]::new));
    }

    private boolean assertInvariants(ItemProcessingState... expectedCurrentState) {
        assert Arrays.asList(expectedCurrentState).contains(currentItemState):
            "expected current state [" + currentItemState + "] to be one of " + Arrays.toString(expectedCurrentState);
        assert currentIndex >= 0 : currentIndex;
        assert retryCounter >= 0 : retryCounter;
        switch (currentItemState) {
            case INITIAL:
                assert requestToExecute == null : requestToExecute;
                assert executionResult == null : executionResult;
                break;
            case TRANSLATED:
                assert requestToExecute != null;
                assert executionResult == null : executionResult;
                break;
            case WAIT_FOR_MAPPING_UPDATE:
                assert requestToExecute == null;
                assert executionResult == null : executionResult;
                break;
            case IMMEDIATE_RETRY:
                assert requestToExecute != null;
                assert executionResult == null : executionResult;
                break;
            case EXECUTED:
                // requestToExecute can be null if the update ended up as NOOP
                assert executionResult != null;
                break;
            case COMPLETED:
                assert requestToExecute != null;
                assert executionResult != null;
                assert getCurrentItem().getPrimaryResponse() != null;
                break;
        }
        return true;
    }
}
