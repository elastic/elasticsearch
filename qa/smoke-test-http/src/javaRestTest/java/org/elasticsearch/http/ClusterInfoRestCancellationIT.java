/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.netty4.AcceptChannelHandler;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.transport.netty4.TLSConfig;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;
import static org.hamcrest.core.IsEqual.equalTo;

public class ClusterInfoRestCancellationIT extends HttpSmokeTestCase {
    public void testClusterInfoRequestCancellation() throws Exception {
        var releaseables = new ArrayList<Releasable>();
        try {
            var transports = StreamSupport.stream(internalCluster().getInstances(HttpServerTransport.class).spliterator(), false)
                .map(FakeHttpTransport.class::cast)
                .toList();
            transports.forEach(t -> {
                try {
                    t.statsBlocking.acquire();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            });

            transports.forEach(t -> releaseables.add(t.statsBlocking::release));

            logger.info("--> Sending request");
            var request = new Request(HttpGet.METHOD_NAME, "/_info/_all");
            var future = new PlainActionFuture<Response>();
            var cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

            assertThat(future.isDone(), equalTo(false));
            awaitTaskWithPrefix(NodesStatsAction.NAME);

            logger.info("--> Waiting for at least one task to hit a block");
            assertBusy(() -> assertTrue(transports.stream().map(t -> t.statsBlocking).anyMatch(Semaphore::hasQueuedThreads)));

            logger.info("--> Cancelling request");
            cancellable.cancel();
            expectThrows(CancellationException.class, future::actionGet);

            assertAllCancellableTasksAreCancelled(NodesStatsAction.NAME);

        } finally {
            Releasables.close(releaseables);
        }

        assertAllTasksHaveFinished(NodesStatsAction.NAME);
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
                    randomFrom((httpPreRequest, channel, listener) -> listener.onResponse(null), null)
                )
            );
        }
    }

    public static class FakeHttpTransport extends Netty4HttpServerTransport {

        public static final String NAME = "fake-transport";
        final Semaphore statsBlocking = new Semaphore(1);

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
            try {
                statsBlocking.acquire();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            statsBlocking.release();
            return super.stats();
        }
    }
}
