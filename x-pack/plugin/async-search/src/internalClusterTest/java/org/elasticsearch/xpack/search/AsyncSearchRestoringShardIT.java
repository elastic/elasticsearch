/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.async.AsyncResultsIndexPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncResultsTestUtils;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests documenting the current behaviour of {@code _async_search} against
 * a shard being restored from a snapshot.
 *
 * <p>{@code POST /{index}/_async_search} uses the same underlying search execution path as
 * {@code POST /{index}/_search}. When the target shard is INITIALIZING, the search request parks
 * in {@code SearchReadyGate} and the async task shows {@code is_running: true} rather than failing.
 */
@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = true)
@ESIntegTestCase.SuiteScopeTestCase
public class AsyncSearchRestoringShardIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX = "test-restore-index";
    private static final String REPO = "test-repo";
    private static final String SNAPSHOT = "test-snapshot";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockRepository.Plugin.class,
            LocalStateCompositeXPackPlugin.class,
            AsyncSearch.class,
            AsyncResultsIndexPlugin.class,
            ReindexPlugin.class
        );
    }

    @Override
    protected void setupSuiteScopeCluster() throws Exception {
        createIndexWithContent(INDEX);
        ensureGreen(INDEX);
        createRepository(REPO, "mock");
        createFullSnapshot(REPO, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(INDEX));
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        AsyncResultsTestUtils.awaitAsyncTasksAndDeleteResultsIndex();
        super.beforeIndexDeletion();
    }

    /**
     * {@code GET /_async_search/{id}} while the underlying search is still parked in
     * {@code SearchReadyGate} returns {@code is_running: true}. This documents that polling
     * behaves consistently with the initial submit: both reflect the parked state rather than
     * surfacing an error.
     */
    public void testPollingAsyncSearchWhileRestoringShowsRunning() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        SubmitAsyncSearchRequest submitRequest = new SubmitAsyncSearchRequest(INDEX);
        submitRequest.setWaitForCompletionTimeout(TimeValue.timeValueMillis(200));
        submitRequest.setKeepAlive(TimeValue.timeValueSeconds(30));

        AsyncSearchResponse submitResp = null;
        String asyncId = null;
        try {
            submitResp = client().execute(SubmitAsyncSearchAction.INSTANCE, submitRequest).actionGet();
            asyncId = submitResp.getId();
            assertThat(submitResp.isRunning(), equalTo(true));

            // Poll while the shard is still INITIALIZING — should still report is_running: true
            GetAsyncResultRequest pollRequest = new GetAsyncResultRequest(asyncId);
            pollRequest.setWaitForCompletionTimeout(TimeValue.timeValueMillis(200));
            AsyncSearchResponse pollResp = client().execute(GetAsyncSearchAction.INSTANCE, pollRequest).actionGet();
            try {
                assertThat(pollResp.isRunning(), equalTo(true));
            } finally {
                pollResp.decRef();
            }
        } finally {
            try {
                if (submitResp != null) {
                    submitResp.decRef();
                }
            } finally {
                try {
                    unblockAndDeleteRestoringIndex(REPO, INDEX);
                } finally {
                    if (asyncId != null) {
                        try {
                            client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(asyncId)).actionGet();
                        } catch (Exception ignored) {}
                    }
                }
            }
        }
    }

    /**
     * {@code POST /{index}/_async_search} against a fully-restoring index parks in
     * {@code SearchReadyGate} — the async task reports {@code is_running: true} rather
     * than failing immediately.
     */
    public void testAsyncSearchWhileRestoringShowsRunning() throws Exception {
        blockAndStartRestore(REPO, SNAPSHOT, INDEX);
        // Use a short waitForCompletionTimeout so the submit call returns immediately
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(INDEX);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(200));
        request.setKeepAlive(TimeValue.timeValueSeconds(30));

        AsyncSearchResponse resp = null;
        String asyncId = null;
        try {
            resp = client().execute(SubmitAsyncSearchAction.INSTANCE, request).actionGet();
            asyncId = resp.getId();
            // Search is parked in SearchReadyGate — task is still running
            assertThat(resp.isRunning(), equalTo(true));
        } finally {
            if (resp != null) {
                resp.decRef();
            }
            unblockAndDeleteRestoringIndex(REPO, INDEX);
            if (asyncId != null) {
                try {
                    client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(asyncId)).actionGet();
                } catch (Exception ignored) {}
            }
        }
    }
}
