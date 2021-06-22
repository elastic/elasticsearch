/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.concurrent.Executor;

/**
 * This class provide contextual state and access to resources across multiple search phases.
 */
interface SearchPhaseContext extends Executor {
    // TODO maybe we can make this concrete later - for now we just implement this in the base class for all initial phases

    /**
     * Returns the total number of shards to the current search across all indices
     */
    int getNumShards();

    /**
     * Returns a logger for this context to prevent each individual phase to create their own logger.
     */
    Logger getLogger();

    /**
     * Returns the currently executing search task
     */
    SearchTask getTask();

    /**
     * Returns the currently executing search request
     */
    SearchRequest getRequest();

    /**
     * Checks if the given context id is part of the point in time of this search (if exists).
     * We should not release search contexts that belong to the point in time during or after searches.
     */
    boolean isPartOfPointInTime(ShardSearchContextId contextId);

    /**
     * Builds and sends the final search response back to the user.
     *
     * @param internalSearchResponse the internal search response
     * @param queryResults           the results of the query phase
     */
    void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults);

    /**
     * Notifies the top-level listener of the provided exception
     */
    void onFailure(Exception e);

    /**
     * This method will communicate a fatal phase failure back to the user. In contrast to a shard failure
     * will this method immediately fail the search request and return the failure to the issuer of the request
     * @param phase the phase that failed
     * @param msg an optional message
     * @param cause the cause of the phase failure
     */
    void onPhaseFailure(SearchPhase phase, String msg, Throwable cause);

    /**
     * This method will record a shard failure for the given shard index. In contrast to a phase failure
     * ({@link #onPhaseFailure(SearchPhase, String, Throwable)}) this method will immediately return to the user but will record
     * a shard failure for the given shard index. This should be called if a shard failure happens after we successfully retrieved
     * a result from that shard in a previous phase.
     */
    void onShardFailure(int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e);

    /**
     * Returns a connection to the node if connected otherwise and {@link org.elasticsearch.transport.ConnectTransportException} will be
     * thrown.
     */
    Transport.Connection getConnection(String clusterAlias, String nodeId);

    /**
     * Returns the {@link SearchTransportService} to send shard request to other nodes
     */
    SearchTransportService getSearchTransport();

    /**
     * Releases a search context with the given context ID on the node the given connection is connected to.
     * @see org.elasticsearch.search.query.QuerySearchResult#getContextId()
     * @see org.elasticsearch.search.fetch.FetchSearchResult#getContextId()
     *
     */
    default void sendReleaseSearchContext(ShardSearchContextId contextId,
                                          Transport.Connection connection,
                                          OriginalIndices originalIndices) {
        assert isPartOfPointInTime(contextId) == false : "Must not release point in time context [" + contextId + "]";
        if (connection != null) {
            getSearchTransport().sendFreeContext(connection, contextId, originalIndices);
        }
    }

    /**
     * Builds an request for the initial search phase.
     *
     * @param shardIt the target {@link SearchShardIterator}
     * @param shardIndex the index of the shard that is used in the coordinator node to
     *                   tiebreak results with identical sort values
     */
    ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex);

    /**
     * Processes the phase transition from on phase to another. This method handles all errors that happen during the initial run execution
     * of the next phase. If there are no successful operations in the context when this method is executed the search is aborted and
     * a response is returned to the user indicating that all shards have failed.
     */
    void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase);

    /**
     * Registers a {@link Releasable} that will be closed when the search request finishes or fails.
     */
    void addReleasable(Releasable releasable);
}
