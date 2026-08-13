/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
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
        ActionFilters actionFilters = new ActionFilters(Set.of());
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
        ActionFilters actionFilters = new ActionFilters(Set.of());
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
}
