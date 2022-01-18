/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TransportReplicationActionRetryOnClosedNodeIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class, MockTransportService.TestPlugin.class);
    }

    public static class Request extends ReplicationRequest<Request> {
        public Request(ShardId shardId) {
            super(shardId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String toString() {
            return "test-request";
        }
    }

    public static class Response extends ReplicationResponse {
        public Response() {}

        public Response(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class TestAction extends TransportReplicationAction<Request, Request, Response> {
        private static final String ACTION_NAME = "internal:test-replication-action";
        private static final ActionType<Response> TYPE = new ActionType<>(ACTION_NAME, Response::new);

        @Inject
        public TestAction(
            Settings settings,
            TransportService transportService,
            ClusterService clusterService,
            IndicesService indicesService,
            ThreadPool threadPool,
            ShardStateAction shardStateAction,
            ActionFilters actionFilters
        ) {
            super(
                settings,
                ACTION_NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                Request::new,
                Request::new,
                ThreadPool.Names.GENERIC
            );
        }

        @Override
        protected Response newResponseInstance(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void shardOperationOnPrimary(
            Request shardRequest,
            IndexShard primary,
            ActionListener<PrimaryResult<Request, Response>> listener
        ) {
            listener.onResponse(new PrimaryResult<>(shardRequest, new Response()));
        }

        @Override
        protected void shardOperationOnReplica(Request shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
            listener.onResponse(new ReplicaResult());
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin, NetworkPlugin {
        private CountDownLatch actionRunningLatch = new CountDownLatch(1);
        private CountDownLatch actionWaitLatch = new CountDownLatch(1);
        private volatile String testActionName;

        public TestPlugin() {}

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TestAction.TYPE, TestAction.class));
        }

        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return List.of(new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new AsyncSender() {
                        @Override
                        public <T extends TransportResponse> void sendRequest(
                            Transport.Connection connection,
                            String action,
                            TransportRequest request,
                            TransportRequestOptions options,
                            TransportResponseHandler<T> handler
                        ) {
                            // only activated on primary
                            if (action.equals(testActionName)) {
                                actionRunningLatch.countDown();
                                try {
                                    actionWaitLatch.await(10, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    throw new AssertionError(e);
                                }
                            }
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    };
                }
            });
        }
    }

    public void testRetryOnStoppedTransportService() throws Exception {
        internalCluster().startMasterOnlyNodes(2);
        String primary = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );

        String replica = internalCluster().startDataOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureGreen("test");

        TestPlugin primaryTestPlugin = getTestPlugin(primary);
        // this test only provoked an issue for the primary action, but for completeness, we pick the action randomly
        primaryTestPlugin.testActionName = TestAction.ACTION_NAME + (randomBoolean() ? "[p]" : "[r]");
        logger.info("--> Test action {}, primary {}, replica {}", primaryTestPlugin.testActionName, primary, replica);

        AtomicReference<Object> response = new AtomicReference<>();
        CountDownLatch doneLatch = new CountDownLatch(1);
        client(coordinator).execute(
            TestAction.TYPE,
            new Request(new ShardId(resolveIndex("test"), 0)),
            ActionListener.runAfter(
                ActionListener.wrap(r -> assertTrue(response.compareAndSet(null, r)), e -> assertTrue(response.compareAndSet(null, e))),
                doneLatch::countDown
            )
        );

        assertTrue(primaryTestPlugin.actionRunningLatch.await(10, TimeUnit.SECONDS));

        MockTransportService primaryTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primary
        );
        // we pause node after TransportService has moved to stopped, but before closing connections, since if connections are closed
        // we would not hit the transport service closed case.
        primaryTransportService.addOnStopListener(() -> {
            primaryTestPlugin.actionWaitLatch.countDown();
            try {
                assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));

        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        if (response.get() instanceof Exception) {
            throw new AssertionError(response.get());
        }
    }

    private TestPlugin getTestPlugin(String node) {
        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, node);
        List<TestPlugin> testPlugins = pluginsService.filterPlugins(TestPlugin.class);
        assertThat(testPlugins, Matchers.hasSize(1));
        return testPlugins.get(0);
    }
}
