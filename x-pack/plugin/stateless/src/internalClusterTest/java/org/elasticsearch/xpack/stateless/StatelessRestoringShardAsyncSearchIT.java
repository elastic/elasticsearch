/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.async.AsyncResultsIndexPlugin;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;
import org.elasticsearch.xpack.search.AsyncSearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Async search, on a stateless cluster, while the index it searches is being restored from a snapshot. A restore that asks for its
 * shards to be reported makes the search fail with a 403; one that does not leaves it failing with a 503.
 */
public class StatelessRestoringShardAsyncSearchIT extends AbstractStatelessRestoringShardIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(AsyncSearch.class);
        plugins.add(AsyncResultsIndexPlugin.class);
        return plugins;
    }

    // _async_search

    /**
     * Submits an async search of the restoring index that waits for it to complete, which it does at once because no shard can answer,
     * and returns the failure the completed search carries.
     */
    private static Exception asyncSearchFailure() {
        final var request = new SubmitAsyncSearchRequest(RESTORED_INDEX);
        request.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30));
        final AsyncSearchResponse response = client().execute(SubmitAsyncSearchAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
        try {
            assertFalse("the search should have completed", response.isRunning());
            assertNotNull("the search should have failed", response.getFailure());
            return response.getFailure();
        } finally {
            response.decRef();
        }
    }

    public void testAsyncSearchFailsWithShardRestoring() throws Exception {
        whileRestoring(true, restoreUuid -> assertShardRestoringException(asyncSearchFailure(), restoreUuid));
    }

    public void testAsyncSearchFailsWithServiceUnavailableWhenTheRestoreDoesNotReportItsShards() throws Exception {
        whileRestoring(false, restoreUuid -> assertServiceUnavailableException(asyncSearchFailure()));
    }
}
