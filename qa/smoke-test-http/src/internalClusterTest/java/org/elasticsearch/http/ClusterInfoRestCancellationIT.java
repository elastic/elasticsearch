/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.AcceptChannelHandler;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CyclicBarrier;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;

public class ClusterInfoRestCancellationIT extends HttpSmokeTestCase {

    public void testClusterInfoRequestCancellation() throws Exception {
        // we create a barrier with one extra party, so we can lock in each node within this method.
        final var cyclicBarrier = new CyclicBarrier(internalCluster().size() + 1);
        var future = new PlainActionFuture<Response>();
        internalCluster().getInstances(HttpServerTransport.class)
            .forEach(transport -> ((FakeHttpTransport) transport).cyclicBarrier = cyclicBarrier);

        logger.info("--> Sending request");
        var cancellable = getRestClient().performRequestAsync(
            new Request(HttpGet.METHOD_NAME, "/_info/_all"),
            wrapAsRestResponseListener(future)
        );

        assertFalse(future.isDone());
        awaitTaskWithPrefix(TransportNodesStatsAction.TYPE.name());

        logger.info("--> Checking that all the HttpTransport are waiting...");
        safeAwait(cyclicBarrier);

        logger.info("--> Cancelling request");
        cancellable.cancel();

        assertTrue(future.isDone());
        expectThrows(CancellationException.class, future::actionGet);
        assertAllCancellableTasksAreCancelled(TransportNodesStatsAction.TYPE.name());

        logger.info("--> Releasing all the node requests :)");
        safeAwait(cyclicBarrier);

        assertAllTasksHaveFinished(TransportNodesStatsAction.TYPE.name());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), FakeNetworkPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.HTTP_TYPE_KEY, FakeHttpTransport.NAME)
            .build();
    }

    public static class FakeNetworkPlugin extends Plugin implements NetworkPlugin {

        public FakeNetworkPlugin() {}

        @Override
        public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
            Settings settings,
            ThreadPool threadPool,
            BigArrays bigArrays,
            PageCacheRecycler pageCacheRecycler,
            CircuitBreakerService circuitBreakerService,
            NamedXContentRegistry xContentRegistry,
            NetworkService networkService,
            HttpServerTransport.Dispatcher dispatcher,
            BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext,
            ClusterSettings clusterSettings,
            Tracer tracer
        ) {
            return Map.of(
                FakeHttpTransport.NAME,
                () -> new FakeHttpTransport(
                    settings,
                    networkService,
                    threadPool,
                    xContentRegistry,
                    dispatcher,
                    clusterSettings,
                    new SharedGroupFactory(settings),
                    tracer,
                    TLSConfig.noTLS(),
                    null,
                    null
                )
            );
        }
    }

    public static class FakeHttpTransport extends Netty4HttpServerTransport {

        public static final String NAME = "fake-transport";
        private CyclicBarrier cyclicBarrier;

        public FakeHttpTransport(
            Settings settings,
            NetworkService networkService,
            ThreadPool threadPool,
            NamedXContentRegistry xContentRegistry,
            Dispatcher dispatcher,
            ClusterSettings clusterSettings,
            SharedGroupFactory sharedGroupFactory,
            Tracer tracer,
            TLSConfig tlsConfig,
            AcceptChannelHandler.AcceptPredicate acceptChannelPredicate,
            HttpValidator httpValidator
        ) {
            super(
                settings,
                networkService,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                sharedGroupFactory,
                tracer,
                tlsConfig,
                acceptChannelPredicate,
                httpValidator
            );
        }

        @Override
        public HttpStats stats() {
            safeAwait(cyclicBarrier);
            safeAwait(cyclicBarrier);
            return super.stats();
        }
    }
}
