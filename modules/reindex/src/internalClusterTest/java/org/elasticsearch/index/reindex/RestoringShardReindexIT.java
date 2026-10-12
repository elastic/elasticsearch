/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests documenting the current behaviour of query-based bulk operations
 * against a shard being restored from a snapshot.
 *
 * <p>{@code _update_by_query} and {@code _delete_by_query} use the search execution path and park
 * in {@code SearchReadyGate} when the target shard is INITIALIZING.
 *
 * <p>{@code _reindex} (with PIT-based pagination) fails fast: the {@code open_reader_context}
 * phase hits {@code NoShardAvailableActionException} (HTTP 503) before reaching any search logic.
 *
 * <p>Setup: a single-shard, no-replica index is deleted before restore so blob reads are
 * required, which is what {@code blockAllDataNodes} actually blocks.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class RestoringShardReindexIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, ReindexPlugin.class);
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(INDEX);
        ensureGreen(INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(INDEX));
    }

    // -------------------------------------------------------------------------
    // Reindex — fails fast via PIT open_reader_context
    // -------------------------------------------------------------------------

    /**
     * {@code POST /_reindex} with a restoring source index fails fast with HTTP 503. The search
     * scatter-gather layer surfaces {@code NoShardAvailableActionException} (thrown when the shard
     * iterator is exhausted for an INITIALIZING shard) as a {@code SearchPhaseExecutionException}
     * rather than parking in {@code SearchReadyGate}.
     */
    public void testReindexFromRestoringSourceFailsFastWithServiceUnavailable() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        try {
            ReindexRequest request = new ReindexRequest().setSourceIndices(INDEX).setDestIndex(INDEX + "-dest");
            ExecutionException e = expectThrows(
                ExecutionException.class,
                () -> client().execute(ReindexAction.INSTANCE, request).get(10, TimeUnit.SECONDS)
            );
            assertThat(e.getCause().getClass(), equalTo(SearchPhaseExecutionException.class));
            assertThat(((SearchPhaseExecutionException) e.getCause()).status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(e.getCause().getCause().getClass(), equalTo(NoShardAvailableActionException.class));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
        }
    }

    /**
     * {@code POST /{index}/_update_by_query} parks in {@code SearchReadyGate} waiting for the
     * shard to become search-ready.
     */
    public void testUpdateByQueryWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        UpdateByQueryRequest request = new UpdateByQueryRequest(INDEX);
        var future = client().execute(UpdateByQueryAction.INSTANCE, request);
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }

    /**
     * {@code POST /{index}/_delete_by_query} parks in {@code SearchReadyGate} waiting for the
     * shard to become search-ready.
     */
    public void testDeleteByQueryWhileRestoringParks() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        DeleteByQueryRequest request = new DeleteByQueryRequest(INDEX).setQuery(QueryBuilders.matchAllQuery());
        var future = client().execute(DeleteByQueryAction.INSTANCE, request);
        try {
            expectThrows(TimeoutException.class, () -> future.get(200, TimeUnit.MILLISECONDS));
        } finally {
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            try {
                future.get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
    }
}
