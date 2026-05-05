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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.elasticsearch.reindex.BulkByPaginatedSearchParallelizationHelper.executeSlicedAction;
import static org.elasticsearch.reindex.BulkByPaginatedSearchParallelizationHelper.sliceIntoSubRequests;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.elasticsearch.search.RandomSearchRequestGenerator.randomSearchSourceBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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

    public void testSliceIntoSubRequests() {
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
     * Sliced sub-requests must keep the parent search's {@link PointInTimeBuilder} (id and keep-alive)
     * so all slices search against the same PIT while applying distinct {@link SliceBuilder} settings.
     */
    public void testSliceIntoSubRequestsPreservesPointInTimeBuilderOnEachSlice() {
        BytesReference pitId = new BytesArray(randomAlphaOfLengthBetween(8, 24));
        TimeValue keepAlive = TimeValue.timeValueMinutes(randomIntBetween(1, 30));
        int times = randomIntBetween(2, 8);
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(keepAlive)));
        SearchRequest[] slices = sliceIntoSubRequests(request, IdFieldMapper.NAME, times);
        assertThat(slices.length, equalTo(times));
        for (int i = 0; i < times; i++) {
            PointInTimeBuilder pit = slices[i].source().pointInTimeBuilder();
            assertNotNull(pit);
            assertThat(pit.getEncodedId(), equalTo(pitId));
            assertThat(pit.getKeepAlive(), equalTo(keepAlive));
            SliceBuilder slice = slices[i].source().slice();
            assertThat(slice.getField(), equalTo(IdFieldMapper.NAME));
            assertThat(slice.getId(), equalTo(i));
            assertThat(slice.getMax(), equalTo(times));
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

    /**
     * When resuming a sliced task with completed slices, each incomplete child gets
     * capturedRPS / incompleteSlices rather than capturedRPS / totalSlices.
     */
    public void testResumedSlicedTaskDistributesRpsAmongIncompleteSlices() {
        final float capturedRps = 60f;
        final int totalSlices = 4;
        final int completedSliceCount = 2;
        final int incompleteSliceCount = totalSlices - completedSliceCount;

        // Build resume info: slices 0,1 completed; slices 2,3 incomplete
        Map<Integer, ResumeInfo.SliceStatus> slices = new LinkedHashMap<>();
        for (int i = 0; i < completedSliceCount; i++) {
            BulkByScrollTask.Status status = new BulkByScrollTask.Status(
                i,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                TimeValue.ZERO,
                0f,
                null,
                TimeValue.ZERO
            );
            BulkByScrollResponse sliceResponse = new BulkByScrollResponse(TimeValue.ZERO, status, List.of(), List.of(), false);
            slices.put(i, new ResumeInfo.SliceStatus(i, null, new ResumeInfo.WorkerResult(sliceResponse, null)));
        }
        for (int i = completedSliceCount; i < totalSlices; i++) {
            ResumeInfo.ScrollWorkerResumeInfo workerInfo = new ResumeInfo.ScrollWorkerResumeInfo(
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                new BulkByScrollTask.Status(i, 0, 0, 0, 0, 0, 0, 0, 0, 0, TimeValue.ZERO, 0f, null, TimeValue.ZERO),
                null
            );
            slices.put(i, new ResumeInfo.SliceStatus(i, workerInfo, null));
        }

        ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(
            new org.elasticsearch.tasks.TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomNonNegativeLong()
        );
        ResumeInfo resumeInfo = new ResumeInfo(origin, null, slices);

        ReindexRequest request = new ReindexRequest();
        request.setRequestsPerSecond(capturedRps);
        request.setSlices(totalSlices);
        request.setResumeInfo(resumeInfo);

        BulkByScrollTask task = (BulkByScrollTask) taskManager.register("reindex", ReindexAction.NAME, request);
        task.setWorkerCount(totalSlices, capturedRps);

        List<ReindexRequest> capturedChildRequests = new ArrayList<>();
        Client client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request childRequest,
                ActionListener<Response> listener
            ) {
                capturedChildRequests.add((ReindexRequest) childRequest);
            }
        };

        DiscoveryNode node = DiscoveryNodeUtils.builder("node").roles(emptySet()).build();
        executeSlicedAction(task, request, ReindexAction.INSTANCE, ActionListener.noop(), client, node, null, v -> {});

        // Only incomplete slices should generate client.execute calls
        assertThat(capturedChildRequests.size(), equalTo(incompleteSliceCount));
        float expectedChildRps = capturedRps / incompleteSliceCount;
        for (ReindexRequest childRequest : capturedChildRequests) {
            assertThat(childRequest.getRequestsPerSecond(), equalTo(expectedChildRps));
        }
    }
}
