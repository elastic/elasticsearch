/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.elasticsearch.reindex.BulkByPaginatedSearchParallelizationHelper.executeSlicedAction;
import static org.elasticsearch.reindex.BulkByPaginatedSearchParallelizationHelper.sliceIntoSubRequests;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchSourceBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class BulkByPaginatedSearchParallelizationHelperTests extends ESTestCase {

    private ThreadPool threadPool;
    private TaskManager taskManager;

    @Before
    public void setUpTaskManager() {
        threadPool = new TestThreadPool(getTestName());
        taskManager = new TaskManager(Settings.EMPTY, threadPool, emptySet());
    }

    @After
    public void tearDownTaskManager() {
        terminate(threadPool);
    }

    public void testSliceIntoSubRequests() throws IOException {
        SearchRequest searchRequest = randomSearchRequest(
            () -> randomSearchSourceBuilder(() -> null, () -> null, () -> null, Collections::emptyList, () -> null, () -> null)
        );
        if (searchRequest.source() != null) {
            // Clear the slice builder if there is one set. We can't call sliceIntoSubRequests if it is.
            searchRequest.source().slice(null);
        }
        int times = between(2, 100);
        String field = randomBoolean() ? IdFieldMapper.NAME : randomAlphaOfLength(5);
        int currentSliceId = 0;
        for (SearchRequest slice : sliceIntoSubRequests(searchRequest, field, times)) {
            assertEquals(field, slice.source().slice().getField());
            assertEquals(currentSliceId, slice.source().slice().getId());
            assertEquals(times, slice.source().slice().getMax());

            // If you clear the slice then the slice should be the same request as the parent request
            slice.source().slice(null);
            if (searchRequest.source() == null) {
                // Except that adding the slice might have added an empty builder
                searchRequest.source(new SearchSourceBuilder());
            }
            assertEquals(searchRequest, slice);
            currentSliceId++;
        }
    }

    /**
     * When the task is a worker, executeSlicedAction invokes the worker action with the given remote version.
     */
    public void testExecuteSlicedActionWithWorkerAndNonNullVersion() {
        ReindexRequest request = new ReindexRequest();
        BulkByScrollTask task = (BulkByScrollTask) taskManager.register("reindex", ReindexAction.NAME, request);
        task.setWorker(request.getRequestsPerSecond(), null);

        Version version = Version.CURRENT;
        AtomicReference<Version> capturedVersion = new AtomicReference<>();
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        Client client = null;
        DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();

        executeSlicedAction(task, request, ReindexAction.INSTANCE, listener, client, node, version, capturedVersion::set);

        assertThat(capturedVersion.get(), sameInstance(version));
    }

    /**
     * When the task is a worker and remote version is null (local reindex), the worker action receives null.
     */
    public void testExecuteSlicedActionWithWorkerAndNullVersion() {
        ReindexRequest request = new ReindexRequest();
        BulkByScrollTask task = (BulkByScrollTask) taskManager.register("reindex", ReindexAction.NAME, request);
        task.setWorker(request.getRequestsPerSecond(), null);

        AtomicReference<Version> capturedVersion = new AtomicReference<>(Version.CURRENT);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        Client client = null;
        DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();

        executeSlicedAction(task, request, ReindexAction.INSTANCE, listener, client, node, null, capturedVersion::set);

        assertThat(capturedVersion.get(), nullValue());
    }

    /**
     * When the task is neither a leader nor a worker (not initialized), executeSlicedAction throws.
     */
    public void testExecuteSlicedActionThrowsWhenTaskNotInitialized() {
        ReindexRequest request = new ReindexRequest();
        BulkByScrollTask task = (BulkByScrollTask) taskManager.register("reindex", ReindexAction.NAME, request);
        // Do not call setWorker or setWorkerCount

        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        Client client = null;
        DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();

        AssertionError e = expectThrows(
            AssertionError.class,
            () -> executeSlicedAction(task, request, ReindexAction.INSTANCE, listener, client, node, null, v -> {})
        );
        assertThat(e.getMessage(), containsString("initialized"));
    }
}
