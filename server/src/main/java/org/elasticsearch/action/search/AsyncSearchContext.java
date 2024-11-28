/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;

public interface AsyncSearchContext {

    SearchRequest getRequest();

    void sendSearchResponse(SearchResponseSections internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults);

    SearchTransportService getSearchTransport();

    SearchTask getTask();

    void onPhaseFailure(SearchPhase phase, String msg, Throwable cause);

    void addReleasable(Releasable releasable);

    void execute(Runnable command);

    void onShardFailure(int shardIndex, SearchShardTarget shard, Exception e);
}
