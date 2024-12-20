/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.Transport;

import java.util.List;
import java.util.function.Supplier;

public interface AsyncSearchContext {

    static ShardSearchFailure[] buildShardFailures(SetOnce<AtomicArray<ShardSearchFailure>> shardFailuresRef) {
        AtomicArray<ShardSearchFailure> shardFailures = shardFailuresRef.get();
        if (shardFailures == null) {
            return ShardSearchFailure.EMPTY_ARRAY;
        }
        List<ShardSearchFailure> entries = shardFailures.asList();
        ShardSearchFailure[] failures = new ShardSearchFailure[entries.size()];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = entries.get(i);
        }
        return failures;
    }

    SearchRequest getRequest();

    void sendSearchResponse(SearchResponseSections internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults);

    SearchTransportService getSearchTransport();

    SearchTask getTask();

    void onPhaseFailure(String phase, String msg, Throwable cause);

    void addReleasable(Releasable releasable);

    void execute(Runnable command);

    void onShardFailure(int shardIndex, SearchShardTarget shard, Exception e);

    Transport.Connection getConnection(String clusterAlias, String nodeId);

    OriginalIndices getOriginalIndices(int shardIndex);

    void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection);

    void executeNextPhase(String currentPhase, Supplier<SearchPhase> nextPhaseSupplier);
}
