/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.single.shard;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.RetryableSplitAwareRequest;
import org.elasticsearch.action.SplitAwareRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.StaleRequestException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.PlainShardsIterator;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Collections.emptySet;
import static org.elasticsearch.action.support.single.shard.TransportSingleShardAction.ROUTE_REFRESH_TIMEOUT;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSingleShardActionTests extends ESTestCase {
    private static ThreadPool threadPool;
    private static ClusterService clusterService;
    private static ClusterApplierService clusterApplierService;
    private static TransportService transportService;
    private static ProjectResolver projectResolver;

    @BeforeClass
    public static void beforeClass() throws Exception {
        ProjectId projectId = randomProjectIdOrDefault();
        final var settings = Settings.builder().put(ROUTE_REFRESH_TIMEOUT.getKey(), "100ms").build();

        threadPool = new TestThreadPool(TransportSingleShardActionTests.class.getSimpleName());
        clusterService = mock(ClusterService.class);
        transportService = mock(TransportService.class);

        projectResolver = TestProjectResolvers.singleProject(projectId);
        final ProjectMetadata project = ProjectMetadata.builder(projectId).build();
        final ClusterState clusterState = ClusterState.builder(new ClusterName(TransportSingleShardActionTests.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("node")).build())
            .metadata(new Metadata.Builder().put(project))
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(settings);
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterApplierService = new ClusterApplierService("node", settings, clusterSettings, threadPool) {
            private final PrioritizedEsThreadPoolExecutor directExecutor = new PrioritizedEsThreadPoolExecutor(
                "master-service",
                1,
                1,
                1,
                TimeUnit.SECONDS,
                r -> {
                    throw new AssertionError("should not create new threads");
                },
                null,
                null
            ) {

                @Override
                public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                    execute(command);
                }

                @Override
                public void execute(Runnable command) {
                    command.run();
                }
            };

            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return directExecutor;
            }
        };

        clusterApplierService.setInitialState(clusterState);
        clusterApplierService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        clusterApplierService.start();
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterApplierService.close();
        clusterApplierService = null;
        clusterService = null;
        transportService = null;
        projectResolver = null;
    }

    // test that when the target shard fails an action with StaleRequestException the coordinator fails it after a timeout
    // rather than waiting indefinitely for a routing update
    public void testStaleRequestExceptionTimesOut() {
        doAnswer(invocation -> {
            final ActionListenerResponseHandler<TestResponse> listener = invocation.getArgument(3);
            listener.handleException(
                new RemoteTransportException(
                    "stale",
                    new StaleRequestException(new ShardId("index", INDEX_UUID_NA_VALUE, 0), SplitShardCountSummary.fromInt(2))
                )
            );
            return null;
        }).when(transportService).sendRequest(any(), any(), any(), ArgumentMatchers.<ActionListenerResponseHandler<TestResponse>>any());

        final var action = new TestTransportSingleShardAction<TestRequest>(threadPool, clusterService, transportService, projectResolver);
        final var result = new PlainActionFuture<TestResponse>();

        action.execute(null, new TestRequest().index("index"), result);
        final var exception = assertThrows(ExecutionException.class, () -> result.get(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
        assertThat(exception.getCause(), isA(ElasticsearchTimeoutException.class));
    }

    // test that when the summary is updated, a request with a StaleRequestException retries and succeeds
    public void testStaleRequestExceptionRetriesAndSucceeds() throws Exception {
        // throw stale, then succeed
        doAnswer(invocation -> {
            final ActionListenerResponseHandler<TestResponse> listener = invocation.getArgument(3);
            listener.handleException(
                new RemoteTransportException(
                    "stale",
                    new StaleRequestException(new ShardId("index", INDEX_UUID_NA_VALUE, 0), SplitShardCountSummary.fromInt(1))
                )
            );
            return null;
        }).doAnswer(invocation -> {
            final ActionListenerResponseHandler<TestResponse> listener = invocation.getArgument(3);
            listener.handleResponse(new TestResponse(null));
            return null;
        }).when(transportService).sendRequest(any(), any(), any(), ArgumentMatchers.<ActionListenerResponseHandler<TestResponse>>any());

        final var action = new TestTransportSingleShardAction<TestRequest>(threadPool, clusterService, transportService, projectResolver);
        final var result = new PlainActionFuture<TestResponse>();

        // the supplier causes the request to get a new summary each time TransportSingleShardAction reads it, so that it
        // appears to update between first constructing the request and testing it after the initial failure.
        // It didn't seem worth mocking up enough cluster state machinery to update it naturally.
        final var summary = new AtomicInteger(0);
        action.execute(null, new TestRequest(() -> SplitShardCountSummary.fromInt(summary.incrementAndGet())).index("index"), result);
        final var response = result.get(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertThat(response, isA(TestResponse.class));
    }

    public void testStaleRequestExceptionIsBubbledUpForNonRetryableRequest() throws Exception {
        // throw stale, then succeed
        doAnswer(invocation -> {
            final ActionListenerResponseHandler<TestResponse> listener = invocation.getArgument(3);
            listener.handleException(
                new RemoteTransportException(
                    "stale",
                    new StaleRequestException(new ShardId("index", INDEX_UUID_NA_VALUE, 0), SplitShardCountSummary.fromInt(1))
                )
            );
            return null;
        }).doAnswer(invocation -> {
            final ActionListenerResponseHandler<TestResponse> listener = invocation.getArgument(3);
            listener.handleResponse(new TestResponse(null));
            return null;
        }).when(transportService).sendRequest(any(), any(), any(), ArgumentMatchers.<ActionListenerResponseHandler<TestResponse>>any());

        final var action = new TestTransportSingleShardAction<TestNonRetryableRequest>(
            threadPool,
            clusterService,
            transportService,
            projectResolver
        );
        final var result = new PlainActionFuture<TestResponse>();

        action.execute(null, new TestNonRetryableRequest().index("index"), result);
        // No retries since the request opted out of them.
        assertThrows(StaleRequestException.class, () -> result.actionGet(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
    }

    static class TestRequest extends SingleShardRequest<TestRequest> implements RetryableSplitAwareRequest {
        private final Supplier<SplitShardCountSummary> splitShardCountSummarySupplier;

        TestRequest() {
            this(() -> SplitShardCountSummary.UNSET);
        }

        TestRequest(Supplier<SplitShardCountSummary> splitShardCountSummarySupplier) {
            this.splitShardCountSummarySupplier = splitShardCountSummarySupplier;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public SplitShardCountSummary getSplitShardCountSummary() {
            return splitShardCountSummarySupplier.get();
        }

        @Override
        public void setSplitShardCountSummary(ProjectMetadata projectMetadata, String index) {}
    }

    static class TestNonRetryableRequest extends SingleShardRequest<TestNonRetryableRequest> implements SplitAwareRequest {
        TestNonRetryableRequest() {}

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    static class TestResponse extends ActionResponse {
        TestResponse(StreamInput ignored) {}

        @Override
        public void writeTo(StreamOutput out) {}
    }

    static class TestTransportSingleShardAction<TRequest extends SingleShardRequest<TRequest>> extends TransportSingleShardAction<
        TRequest,
        TestResponse> {
        static String NAME = "test:single_shard_action";

        protected TestTransportSingleShardAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ProjectResolver projectResolver
        ) {
            super(
                NAME,
                threadPool,
                clusterService,
                transportService,
                new ActionFilters(emptySet()),
                projectResolver,
                null,
                null,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
        }

        @Override
        protected TestResponse shardOperation(TRequest request, ShardId shardId) throws IOException {
            return null;
        }

        @Override
        protected Writeable.Reader<TestResponse> getResponseReader() {
            return TestResponse::new;
        }

        @Override
        protected boolean resolveIndex(TRequest request) {
            return false;
        }

        @Override
        protected ShardsIterator shards(ProjectState state, TransportSingleShardAction<TRequest, TestResponse>.InternalRequest request) {
            final var shardRouting = shardRoutingBuilder("index", 0, "node", true, ShardRoutingState.STARTED).build();
            return new PlainShardsIterator(List.of(shardRouting));
        }
    }
}
