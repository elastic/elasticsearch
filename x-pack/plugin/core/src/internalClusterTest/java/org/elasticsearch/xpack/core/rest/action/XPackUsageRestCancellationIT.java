/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rest.action;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.action.TransportXPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.xpack.core.action.XPackUsageFeatureAction.xpackUsageFeatureAction;
import static org.hamcrest.core.IsEqual.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class XPackUsageRestCancellationIT extends ESIntegTestCase {
    private static final CountDownLatch blockActionLatch = new CountDownLatch(1);
    private static final CountDownLatch blockingXPackUsageActionExecuting = new CountDownLatch(1);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(getTestTransportPlugin(), BlockingUsageActionXPackPlugin.class);
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
        ensureStableCluster(1);
        final String actionName = XPackUsageAction.NAME;

        final Request request = new Request(HttpGet.METHOD_NAME, "/_xpack/usage");
        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

        assertThat(future.isDone(), equalTo(false));

        blockingXPackUsageActionExecuting.await();
        cancellable.cancel();
        assertAllCancellableTasksAreCancelled(actionName);

        blockActionLatch.countDown();
        expectThrows(CancellationException.class, future::actionGet);

        assertAllTasksHaveFinished(actionName);
    }

    public static class BlockingUsageActionXPackPlugin extends LocalStateCompositeXPackPlugin {
        public static final ActionType<XPackUsageFeatureResponse> BLOCKING_XPACK_USAGE = xpackUsageFeatureAction("blocking_xpack_usage");
        public static final ActionType<XPackUsageFeatureResponse> NON_BLOCKING_XPACK_USAGE = xpackUsageFeatureAction("regular_xpack_usage");

        public BlockingUsageActionXPackPlugin(Settings settings, Path configPath) {
            super(settings, configPath);
        }

        @Override
        protected Class<? extends TransportAction<XPackUsageRequest, XPackUsageResponse>> getUsageAction() {
            return ClusterBlockAwareTransportXPackUsageAction.class;
        }

        @Override
        public List<ActionHandler> getActions() {
            final ArrayList<ActionHandler> actions = new ArrayList<>(super.getActions());
            actions.add(new ActionHandler(BLOCKING_XPACK_USAGE, BlockingXPackUsageAction.class));
            actions.add(new ActionHandler(NON_BLOCKING_XPACK_USAGE, NonBlockingXPackUsageAction.class));
            return actions;
        }
    }

    public static class ClusterBlockAwareTransportXPackUsageAction extends TransportXPackUsageAction {
        @Inject
        public ClusterBlockAwareTransportXPackUsageAction(
            ThreadPool threadPool,
            TransportService transportService,
            ClusterService clusterService,
            ActionFilters actionFilters,
            NodeClient client
        ) {
            super(threadPool, transportService, clusterService, actionFilters, client);
        }

        @Override
        protected List<ActionType<XPackUsageFeatureResponse>> usageActions() {
            return List.of(BlockingUsageActionXPackPlugin.BLOCKING_XPACK_USAGE, BlockingUsageActionXPackPlugin.NON_BLOCKING_XPACK_USAGE);
        }
    }

    public static class BlockingXPackUsageAction extends XPackUsageFeatureTransportAction {
        @Inject
        public BlockingXPackUsageAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters
        ) {
            super(BlockingUsageActionXPackPlugin.BLOCKING_XPACK_USAGE.name(), transportService, clusterService, threadPool, actionFilters);
        }

        @Override
        protected void localClusterStateOperation(
            Task task,
            XPackUsageRequest request,
            ClusterState state,
            ActionListener<XPackUsageFeatureResponse> listener
        ) throws Exception {
            blockingXPackUsageActionExecuting.countDown();
            blockActionLatch.await();
            listener.onResponse(new XPackUsageFeatureResponse(new XPackFeatureUsage("test", false, false) {
                @Override
                public TransportVersion getMinimalSupportedVersion() {
                    return TransportVersion.current();
                }
            }));
        }
    }

    public static class NonBlockingXPackUsageAction extends XPackUsageFeatureTransportAction {
        @Inject
        public NonBlockingXPackUsageAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters
        ) {
            super(
                BlockingUsageActionXPackPlugin.NON_BLOCKING_XPACK_USAGE.name(),
                transportService,
                clusterService,
                threadPool,
                actionFilters
            );
        }

        @Override
        protected void localClusterStateOperation(
            Task task,
            XPackUsageRequest request,
            ClusterState state,
            ActionListener<XPackUsageFeatureResponse> listener
        ) {
            assert false : "Unexpected execution";
        }
    }
}
