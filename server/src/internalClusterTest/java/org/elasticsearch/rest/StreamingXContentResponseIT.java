/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpResponse;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class StreamingXContentResponseIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), RandomXContentResponsePlugin.class);
    }

    public static class RandomXContentResponsePlugin extends Plugin implements ActionPlugin {

        public static final String ROUTE = "/_random_xcontent_response";

        public static final String INFINITE_ROUTE = "/_random_infinite_xcontent_response";

        public final AtomicReference<Response> responseRef = new AtomicReference<>();

        public record Response(Map<String, String> fragments, CountDownLatch completedLatch) {}

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
            return List.of(
                // handler that returns a normal (finite) response
                new RestHandler() {
                    @Override
                    public List<Route> routes() {
                        return List.of(new Route(RestRequest.Method.GET, ROUTE));
                    }

                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws IOException {
                        final var response = new Response(new HashMap<>(), new CountDownLatch(1));
                        final var entryCount = between(0, 10000);
                        for (int i = 0; i < entryCount; i++) {
                            response.fragments().put(randomIdentifier(), randomIdentifier());
                        }
                        assertTrue(responseRef.compareAndSet(null, response));
                        handleStreamingXContentRestRequest(
                            channel,
                            client.threadPool(),
                            response.completedLatch(),
                            response.fragments().entrySet().iterator()
                        );
                    }
                },

                // handler that just keeps on yielding chunks until aborted
                new RestHandler() {
                    @Override
                    public List<Route> routes() {
                        return List.of(new Route(RestRequest.Method.GET, INFINITE_ROUTE));
                    }

                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws IOException {
                        final var response = new Response(new HashMap<>(), new CountDownLatch(1));
                        assertTrue(responseRef.compareAndSet(null, new Response(null, response.completedLatch())));
                        handleStreamingXContentRestRequest(channel, client.threadPool(), response.completedLatch(), new Iterator<>() {

                            private long id;

                            // carry on yielding content even after the channel closes
                            private final Semaphore trailingContentPermits = new Semaphore(between(0, 20));

                            @Override
                            public boolean hasNext() {
                                return request.getHttpChannel().isOpen() || trailingContentPermits.tryAcquire();
                            }

                            @Override
                            public Map.Entry<String, String> next() {
                                return new Map.Entry<>() {
                                    private final String key = Long.toString(id++);
                                    private final String content = randomIdentifier();

                                    @Override
                                    public String getKey() {
                                        return key;
                                    }

                                    @Override
                                    public String getValue() {
                                        return content;
                                    }

                                    @Override
                                    public String setValue(String value) {
                                        return fail(null, "must not setValue");
                                    }
                                };
                            }
                        });
                    }
                }
            );
        }

        private static void handleStreamingXContentRestRequest(
            RestChannel channel,
            ThreadPool threadPool,
            CountDownLatch completionLatch,
            Iterator<Map.Entry<String, String>> fragmentIterator
        ) throws IOException {
            try (var refs = new RefCountingRunnable(completionLatch::countDown)) {
                final var streamingXContentResponse = new StreamingXContentResponse(channel, channel.request(), refs.acquire());
                streamingXContentResponse.writeFragment(p -> ChunkedToXContentHelper.startObject(), refs.acquire());
                final var finalRef = refs.acquire();
                ThrottledIterator.run(
                    fragmentIterator,
                    (ref, fragment) -> randomFrom(EsExecutors.DIRECT_EXECUTOR_SERVICE, threadPool.generic()).execute(
                        ActionRunnable.run(ActionListener.releaseAfter(refs.acquireListener(), ref), () -> {
                            Thread.yield();
                            streamingXContentResponse.writeFragment(
                                p -> ChunkedToXContentHelper.field(fragment.getKey(), fragment.getValue()),
                                refs.acquire()
                            );
                        })
                    ),
                    between(1, 10),
                    () -> {},
                    () -> {
                        try (streamingXContentResponse; finalRef) {
                            streamingXContentResponse.writeFragment(p -> ChunkedToXContentHelper.endObject(), refs.acquire());
                        }
                    }
                );
            }
        }
    }

    public void testRandomStreamingXContentResponse() throws IOException {
        final var request = new Request("GET", RandomXContentResponsePlugin.ROUTE);
        final var response = getRestClient().performRequest(request);
        final var actualEntries = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
        assertEquals(getExpectedEntries(), actualEntries);
    }

    public void testAbort() throws IOException {
        final var request = new Request("GET", RandomXContentResponsePlugin.INFINITE_ROUTE);
        final var responseStarted = new CountDownLatch(1);
        final var bodyConsumed = new CountDownLatch(1);
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setHttpAsyncResponseConsumerFactory(() -> new HttpAsyncResponseConsumer<>() {

            final ByteBuffer readBuffer = ByteBuffer.allocate(ByteSizeUnit.KB.toIntBytes(4));
            int bytesToConsume = ByteSizeUnit.MB.toIntBytes(1);

            @Override
            public void responseReceived(HttpResponse response) {
                responseStarted.countDown();
            }

            @Override
            public void consumeContent(ContentDecoder decoder, IOControl ioControl) throws IOException {
                readBuffer.clear();
                final var bytesRead = decoder.read(readBuffer);
                if (bytesRead > 0) {
                    bytesToConsume -= bytesRead;
                }

                if (bytesToConsume <= 0) {
                    bodyConsumed.countDown();
                    ioControl.shutdown();
                }
            }

            @Override
            public void responseCompleted(HttpContext context) {}

            @Override
            public void failed(Exception ex) {}

            @Override
            public Exception getException() {
                return null;
            }

            @Override
            public HttpResponse getResult() {
                return null;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public void close() {}

            @Override
            public boolean cancel() {
                return false;
            }
        }));

        try {
            try (var restClient = createRestClient(internalCluster().getRandomNodeName())) {
                // one-node REST client to avoid retries
                expectThrows(ConnectionClosedException.class, () -> restClient.performRequest(request));
            }
            safeAwait(responseStarted);
            safeAwait(bodyConsumed);
        } finally {
            assertNull(getExpectedEntries()); // mainly just checking that all refs are released
        }
    }

    private static Map<String, String> getExpectedEntries() {
        final List<Map<String, String>> nodeResponses = StreamSupport
            // concatenate all the chunks in all the entries
            .stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(p -> p.filterPlugins(RandomXContentResponsePlugin.class))
            .flatMap(p -> {
                final var response = p.responseRef.getAndSet(null);
                if (response == null) {
                    return Stream.of();
                } else {
                    safeAwait(response.completedLatch()); // ensures that all refs have been released
                    return Stream.of(response.fragments());
                }
            })
            .toList();
        assertThat(nodeResponses, hasSize(1));
        return nodeResponses.get(0);
    }
}
