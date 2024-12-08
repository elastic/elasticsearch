/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rest.action;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.datatiers.NodesDataTiersUsageTransportAction;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DataTiersUsageRestCancellationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(getTestTransportPlugin(), DataTiersUsageOnlyXPackPlugin.class, MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(ordinal, otherSettings))
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testCancellation() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();

        final CountDownLatch tasksBlockedLatch = new CountDownLatch(1);
        final SubscribableListener<Void> nodeStatsRequestsReleaseListener = new SubscribableListener<>();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).addRequestHandlingBehavior(
                NodesDataTiersUsageTransportAction.TYPE.name() + "[n]",
                (handler, request, channel, task) -> {
                    tasksBlockedLatch.countDown();
                    nodeStatsRequestsReleaseListener.addListener(
                        ActionTestUtils.assertNoFailureListener(ignored -> handler.messageReceived(request, channel, task))
                    );
                }
            );
        }

        final Request request = new Request(HttpGet.METHOD_NAME, "/_xpack/usage");
        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

        assertFalse(future.isDone());
        safeAwait(tasksBlockedLatch); // must wait for the node-level tasks to start to avoid cancelling being handled earlier
        cancellable.cancel();

        assertAllCancellableTasksAreCancelled(NodesDataTiersUsageTransportAction.TYPE.name());
        assertAllCancellableTasksAreCancelled(XPackUsageAction.NAME);

        nodeStatsRequestsReleaseListener.onResponse(null);
        expectThrows(CancellationException.class, future::actionGet);

        assertAllTasksHaveFinished(NodesDataTiersUsageTransportAction.TYPE.name());
        assertAllTasksHaveFinished(XPackUsageAction.NAME);
    }

    public static class DataTiersUsageOnlyXPackPlugin extends LocalStateCompositeXPackPlugin {
        public DataTiersUsageOnlyXPackPlugin(Settings settings, Path configPath) {
            super(settings, configPath);
        }

        @Override
        protected Class<? extends TransportAction<XPackUsageRequest, XPackUsageResponse>> getUsageAction() {
            return DataTiersOnlyTransportXPackUsageAction.class;
        }
    }

    public static class DataTiersOnlyTransportXPackUsageAction extends TransportXPackUsageAction {
        @Inject
        public DataTiersOnlyTransportXPackUsageAction(
            ThreadPool threadPool,
            TransportService transportService,
            ClusterService clusterService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            NodeClient client
        ) {
            super(threadPool, transportService, clusterService, actionFilters, indexNameExpressionResolver, client);
        }

        @Override
        protected List<ActionType<XPackUsageFeatureResponse>> usageActions() {
            return List.of(XPackUsageFeatureAction.DATA_TIERS);
        }
    }
}
