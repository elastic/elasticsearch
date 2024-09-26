/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.network;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.EmptyRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ThreadWatchdogIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ThreadWatchdog.NETWORK_THREAD_WATCHDOG_INTERVAL.getKey(), "100ms")
            .put(ThreadWatchdog.NETWORK_THREAD_WATCHDOG_QUIET_TIME.getKey(), "0")
            .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            SlowRequestProcessingPlugin.class,
            MockTransportService.TestPlugin.class
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public static class SlowRequestProcessingPlugin extends Plugin implements ActionPlugin {

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(Route.builder(RestRequest.Method.POST, "_slow").build());
                }

                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                    blockAndWaitForWatchdogLogs();
                    new RestToXContentListener<>(channel).onResponse((b, p) -> b.startObject().endObject());
                }
            });
        }
    }

    private static void blockAndWaitForWatchdogLogs() {
        MockLog.awaitLogger(
            () -> {},
            ThreadWatchdog.class,
            new MockLog.SeenEventExpectation(
                "warning",
                ThreadWatchdog.class.getCanonicalName(),
                Level.WARN,
                "*the following threads are active but did not make progress in the preceding [100ms]:*"
                    + Thread.currentThread().getName()
                    + "*"
            ),
            new MockLog.SeenEventExpectation(
                "thread dump",
                ThreadWatchdog.class.getCanonicalName(),
                Level.WARN,
                "*hot threads dump due to active threads not making progress*"
            )
        );
    }

    public void testThreadWatchdogHttpLogging() throws IOException {
        ESRestTestCase.assertOK(getRestClient().performRequest(new Request("POST", "_slow")));
    }

    public void testThreadWatchdogTransportLogging() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final var transportServiceIterator = internalCluster().getInstances(TransportService.class).iterator();
        final var sourceTransportService = transportServiceIterator.next();
        final var targetTransportService = transportServiceIterator.next();

        targetTransportService.registerRequestHandler(
            "internal:slow",
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            EmptyRequest::new,
            (request, channel, task) -> {
                blockAndWaitForWatchdogLogs();
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );

        safeAwait(
            SubscribableListener.newForked(
                l -> sourceTransportService.sendRequest(
                    targetTransportService.getLocalNode(),
                    "internal:slow",
                    new EmptyRequest(),
                    new ActionListenerResponseHandler<TransportResponse>(
                        l,
                        in -> TransportResponse.Empty.INSTANCE,
                        EsExecutors.DIRECT_EXECUTOR_SERVICE
                    )
                )
            )
        );
    }

}
