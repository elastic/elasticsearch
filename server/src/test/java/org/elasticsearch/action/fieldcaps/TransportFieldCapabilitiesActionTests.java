/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportFieldCapabilitiesActionTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @After
    public void closeThreadPool() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCCSCompatibilityCheck() {
        Settings settings = Settings.builder()
            .put("node.name", TransportFieldCapabilitiesActionTests.class.getSimpleName())
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
        ActionFilters actionFilters = ActionFilters.EMPTY;
        TransportVersion transportVersion = TransportVersionUtils.getNextVersion(TransportVersion.minimumCCSVersion(), true);
        try {
            TransportService transportService = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                transportVersion,
                threadPool
            );

            FieldCapabilitiesRequest fieldCapsRequest = new FieldCapabilitiesRequest();
            fieldCapsRequest.indexFilter(new DummyQueryBuilder() {
                @Override
                protected void doWriteTo(StreamOutput out) throws IOException {
                    if (out.getTransportVersion().supports(transportVersion) == false) {
                        throw new IllegalArgumentException("This query isn't serializable before transport version " + transportVersion);
                    }
                }
            });

            ClusterService clusterService = new ClusterService(
                settings,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            // indicesService is not exercised on this path: the CCS compatibility check fails before any index is touched.
            TransportFieldCapabilitiesAction action = new TransportFieldCapabilitiesAction(
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                null,
                null,
                null,
                CrossProjectModeDecider.NOOP
            );

            IllegalArgumentException ex = safeAwaitFailure(
                IllegalArgumentException.class,
                FieldCapabilitiesResponse.class,
                l -> action.doExecute(null, fieldCapsRequest, l)
            );

            assertThat(
                ex.getMessage(),
                containsString("[class org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest] is not compatible with version")
            );
            assertThat(ex.getMessage(), containsString("and the 'search.check_ccs_compatibility' setting is enabled."));
            assertEquals("This query isn't serializable before transport version " + transportVersion, ex.getCause().getMessage());
        } finally {
            assertTrue(ESTestCase.terminate(threadPool));
        }
    }

    @SuppressWarnings("unchecked")
    public void testNodeHandlerAbortedWhenTaskCancelled() throws Exception {
        ActionFilters actionFilters = ActionFilters.EMPTY;
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            null
        );
        TransportService transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        new TransportFieldCapabilitiesAction(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            null,
            null,
            null,
            CrossProjectModeDecider.NOOP
        );

        final TransportRequestHandler<FieldCapabilitiesNodeRequest> nodeHandler = (TransportRequestHandler<
            FieldCapabilitiesNodeRequest>) transportService.getRequestHandler(TransportFieldCapabilitiesAction.ACTION_NODE_NAME)
                .getHandler();

        final CancellableTask cancelledTask = new CancellableTask(
            0,
            "transport",
            TransportFieldCapabilitiesAction.ACTION_NODE_NAME,
            "",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        TaskCancelHelper.cancel(cancelledTask, "test");

        final FieldCapabilitiesNodeRequest request = new FieldCapabilitiesNodeRequest(
            List.of(new ShardId("test_index", "_na_", 0)),
            new String[] { "*" },
            new String[0],
            new String[0],
            OriginalIndices.NONE,
            null,
            0L,
            Map.of(),
            true
        );

        final AtomicReference<Exception> channelError = new AtomicReference<>();
        final TransportChannel channel = new TransportChannel() {
            @Override
            public String getProfileName() {
                return "test";
            }

            @Override
            public void sendResponse(TransportResponse response) {
                fail("expected TaskCancelledException but got a successful response");
            }

            @Override
            public void sendResponse(Exception exception) {
                channelError.set(exception);
            }
        };

        nodeHandler.messageReceived(request, channel, cancelledTask);
        assertThat("pre-cancelled task must not fetch any shard data", channelError.get(), instanceOf(TaskCancelledException.class));
    }

    /**
     * Saturates the {@code search_coordination} pool (one thread, queue of one) and sends a request to the local node whose response
     * handler forks onto the per-request single-threaded executor. The response handler is force-executed, so it is queued beyond the
     * limit and completes on a {@code search_coordination} thread once the pool frees up.
     */
    public void testResponseHandlerCompletesWhenSearchCoordinationQueueIsFull() throws Exception {
        final ThreadPool saturatedThreadPool = createSaturatableThreadPool();
        final CountDownLatch blocker = new CountDownLatch(1);
        try (
            MockTransportService transportService = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                saturatedThreadPool
            )
        ) {
            transportService.start();
            transportService.acceptIncomingRequests();
            final boolean respondWithFailure = randomBoolean();
            final String action = "internal:test/field_caps/saturated_search_coordination";
            transportService.registerRequestHandler(
                action,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                EmptyRequest::new,
                (request, channel, task) -> {
                    if (respondWithFailure) {
                        channel.sendResponse(new ElasticsearchException("simulated"));
                    } else {
                        channel.sendResponse(ActionResponse.Empty.INSTANCE);
                    }
                }
            );

            final Executor searchCoordinationExecutor = saturatedThreadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            blockAndFillQueue(searchCoordinationExecutor, blocker);

            final CountDownLatch completed = new CountDownLatch(1);
            final AtomicReference<String> completingThread = new AtomicReference<>();
            final AtomicReference<ActionResponse.Empty> response = new AtomicReference<>();
            final AtomicReference<Exception> failure = new AtomicReference<>();
            transportService.sendRequest(
                transportService.getLocalNode(),
                action,
                new EmptyRequest(),
                new ActionListenerResponseHandler<>(ActionListener.<ActionResponse.Empty>wrap(r -> {
                    response.set(r);
                    completingThread.set(Thread.currentThread().getName());
                    completed.countDown();
                }, e -> {
                    failure.set(e);
                    completingThread.set(Thread.currentThread().getName());
                    completed.countDown();
                }),
                    in -> ActionResponse.Empty.INSTANCE,
                    TransportFieldCapabilitiesAction.buildSingleThreadedExecutor(searchCoordinationExecutor)
                )
            );

            assertThat("handler must wait for the search_coordination pool", completed.getCount(), equalTo(1L));
            blocker.countDown();
            safeAwait(completed);
            assertThat(completingThread.get(), containsString(ThreadPool.Names.SEARCH_COORDINATION));
            if (respondWithFailure) {
                assertNull(response.get());
                assertThat(ExceptionsHelper.unwrapCause(failure.get()).getMessage(), equalTo("simulated"));
            } else {
                assertNull(failure.get());
                assertSame(ActionResponse.Empty.INSTANCE, response.get());
            }
        } finally {
            blocker.countDown();
            ThreadPool.terminate(saturatedThreadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * Tasks submitted to the per-request single-threaded executor that are not force-executed respect the queue limit of the
     * {@code search_coordination} pool and are notified via {@link AbstractRunnable#onRejection} when it is full.
     */
    public void testNonForcedTaskIsRejectedWhenSearchCoordinationQueueIsFull() {
        final ThreadPool saturatedThreadPool = createSaturatableThreadPool();
        final CountDownLatch blocker = new CountDownLatch(1);
        try {
            final Executor searchCoordinationExecutor = saturatedThreadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            blockAndFillQueue(searchCoordinationExecutor, blocker);

            final AtomicReference<Exception> rejection = new AtomicReference<>();
            final AtomicReference<Exception> failure = new AtomicReference<>();
            final AtomicBoolean ran = new AtomicBoolean();
            TransportFieldCapabilitiesAction.buildSingleThreadedExecutor(searchCoordinationExecutor).execute(new AbstractRunnable() {
                @Override
                public void onRejection(Exception e) {
                    rejection.set(e);
                }

                @Override
                public void onFailure(Exception e) {
                    failure.set(e);
                }

                @Override
                protected void doRun() {
                    ran.set(true);
                }
            });

            assertThat(rejection.get(), instanceOf(EsRejectedExecutionException.class));
            assertNull(failure.get());
            assertFalse(ran.get());
        } finally {
            blocker.countDown();
            ThreadPool.terminate(saturatedThreadPool, 10, TimeUnit.SECONDS);
        }
    }

    private ThreadPool createSaturatableThreadPool() {
        return new TestThreadPool(
            getTestName(),
            Settings.builder()
                .put("thread_pool." + ThreadPool.Names.SEARCH_COORDINATION + ".size", 1)
                .put("thread_pool." + ThreadPool.Names.SEARCH_COORDINATION + ".queue_size", 1)
                .build()
        );
    }

    /**
     * Occupies the single thread of the given executor until {@code blocker} is released and fills its queue of one.
     */
    private static void blockAndFillQueue(Executor executor, CountDownLatch blocker) {
        final CountDownLatch blockerStarted = new CountDownLatch(1);
        executor.execute(() -> {
            blockerStarted.countDown();
            safeAwait(blocker);
        });
        safeAwait(blockerStarted);
        executor.execute(() -> {});
    }
}
