/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.Transport;

import java.util.function.BiFunction;

public class DefaultSearchPhaseContext implements SearchPhaseContext {

    private final SearchRequest request;
    private final SearchTransportService searchTransportService;
    private final BiFunction<String, String, Transport.Connection> nodeIdToConnection;

    DefaultSearchPhaseContext(SearchRequest request,
                              SearchTransportService searchTransportService,
                              BiFunction<String, String, Transport.Connection> nodeIdToConnection) {
        this.request = request;
        this.searchTransportService = searchTransportService;
        this.nodeIdToConnection = nodeIdToConnection;
    }

    @Override
    public final int getNumShards() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Logger getLogger() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final SearchTask getTask() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final SearchRequest getRequest() {
        return request;
    }

    @Override
    public boolean isPartOfPointInTime(ShardSearchContextId contextId) {
        final PointInTimeBuilder pointInTimeBuilder = request.pointInTimeBuilder();
        if (pointInTimeBuilder != null) {
            return request.pointInTimeBuilder().getSearchContextId(searchTransportService.getNamedWriteableRegistry()).contains(contextId);
        } else {
            return false;
        }
    }

    @Override
    public final Transport.Connection getConnection(String clusterAlias, String nodeId) {
        Transport.Connection conn = nodeIdToConnection.apply(clusterAlias, nodeId);
        Version minVersion = request.minCompatibleShardNode();
        if (minVersion != null && conn != null && conn.getVersion().before(minVersion)) {
            throw new VersionMismatchException("One of the shards is incompatible with the required minimum version [{}]", minVersion);
        }
        return conn;
    }

    @Override
    public final SearchTransportService getSearchTransport() {
        return searchTransportService;
    }

    @Override
    public final void execute(Runnable command) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addReleasable(Releasable releasable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void onFailure(Exception e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void onShardFailure(final int shardIndex, SearchShardTarget shardTarget, Exception e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase) {
        throw new UnsupportedOperationException();
    }

}
