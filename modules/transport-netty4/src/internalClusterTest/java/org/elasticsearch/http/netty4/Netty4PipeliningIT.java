/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.util.ReferenceCounted;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.bytes.ZeroBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING_MAX_EVENTS;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class Netty4PipeliningIT extends ESNetty4IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(CountDown3Plugin.class, ChunkAndFailPlugin.class), super.nodePlugins());
    }

    private static final int MAX_PIPELINE_EVENTS = 10;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SETTING_PIPELINING_MAX_EVENTS.getKey(), MAX_PIPELINE_EVENTS)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testThatNettyHttpServerSupportsPipelining() throws Exception {
        runPipeliningTest(
            CountDown3Plugin.ROUTE,
            "/_nodes",
            "/_nodes/stats",
            CountDown3Plugin.ROUTE,
            "/_cluster/health",
            "/_cluster/state",
            CountDown3Plugin.ROUTE,
            "/_cat/shards"
        );
    }

    public void testChunkingFailures() throws Exception {
        runPipeliningTest(0, ChunkAndFailPlugin.randomRequestUri());
        runPipeliningTest(0, ChunkAndFailPlugin.randomRequestUri(), "/_cluster/state");
        runPipeliningTest(
            -1, // typically get the first 2 responses, but we can hit the failing chunk and close the channel soon enough to lose them too
            CountDown3Plugin.ROUTE,
            CountDown3Plugin.ROUTE,
            ChunkAndFailPlugin.randomRequestUri(),
            "/_cluster/health",
            CountDown3Plugin.ROUTE
        );
    }

    public void testPipelineOverflow() throws Exception {
        final var routes = new String[1 // the first request which never returns a response so doesn't consume a spot in the queue
            + MAX_PIPELINE_EVENTS // the responses which fill up the queue
            + 1 // to cause the overflow
            + between(0, 5) // for good measure, to e.g. make sure we don't leak these responses
        ];
        Arrays.fill(routes, "/_cluster/health");
        routes[0] = CountDown3Plugin.ROUTE; // never returns
        runPipeliningTest(0, routes);
    }

    private void runPipeliningTest(String... routes) throws InterruptedException {
        runPipeliningTest(routes.length, routes);
    }

    private void runPipeliningTest(int expectedResponseCount, String... routes) throws InterruptedException {
        try (var client = new Netty4HttpClient()) {
            final var responses = client.get(
                randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses()).address(),
                routes
            );
            try {
                logger.info("response codes: {}", responses.stream().mapToInt(r -> r.status().code()).toArray());
                if (expectedResponseCount >= 0) {
                    assertThat(responses, hasSize(expectedResponseCount));
                }
                assertThat(responses.size(), lessThanOrEqualTo(routes.length));
                assertTrue(responses.stream().allMatch(r -> r.status().code() == 200));
                assertOpaqueIdsInOrder(Netty4HttpClient.returnOpaqueIds(responses));
            } finally {
                responses.forEach(ReferenceCounted::release);
            }
        }
    }

    private void assertOpaqueIdsInOrder(Collection<String> opaqueIds) {
        // check if opaque ids are monotonically increasing
        int i = 0;
        String msg = Strings.format("Expected list of opaque ids to be monotonically increasing, got [%s]", opaqueIds);
        for (String opaqueId : opaqueIds) {
            assertThat(msg, opaqueId, is(String.valueOf(i++)));
        }
    }

    private static final ToXContentObject EMPTY_RESPONSE = (builder, params) -> builder.startObject().endObject();

    /**
     * Adds an HTTP route that waits for 3 concurrent executions before returning any of them
     */
    public static class CountDown3Plugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/countdown_3";

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
            return List.of(new BaseRestHandler() {
                private final SubscribableListener<ToXContentObject> subscribableListener = new SubscribableListener<>();
                private final CountDownActionListener countDownActionListener = new CountDownActionListener(
                    3,
                    subscribableListener.map(v -> EMPTY_RESPONSE)
                );

                private void addListener(ActionListener<ToXContentObject> listener) {
                    subscribableListener.addListener(listener);
                    countDownActionListener.onResponse(null);
                }

                @Override
                public String getName() {
                    return ROUTE;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(GET, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    return channel -> addListener(new RestToXContentListener<>(channel));
                }
            });
        }
    }

    /**
     * Adds an HTTP route that waits for 3 concurrent executions before returning any of them
     */
    public static class ChunkAndFailPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/chunk_and_fail";
        static final String FAIL_AFTER_BYTES_PARAM = "fail_after_bytes";

        static String randomRequestUri() {
            return ROUTE + '?' + FAIL_AFTER_BYTES_PARAM + '=' + between(0, ByteSizeUnit.MB.toIntBytes(2));
        }

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
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return ROUTE;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(GET, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    final var failAfterBytes = request.paramAsInt(FAIL_AFTER_BYTES_PARAM, -1);
                    if (failAfterBytes < 0) {
                        throw new IllegalArgumentException("[" + FAIL_AFTER_BYTES_PARAM + "] must be present and non-negative");
                    }
                    return channel -> randomExecutor(client.threadPool()).execute(
                        () -> channel.sendResponse(RestResponse.chunked(RestStatus.OK, new ChunkedRestResponseBody() {
                            int bytesRemaining = failAfterBytes;

                            @Override
                            public boolean isDone() {
                                return false;
                            }

                            @Override
                            public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
                                assert bytesRemaining >= 0 : "already failed";
                                if (bytesRemaining == 0) {
                                    bytesRemaining = -1;
                                    throw new IOException("simulated failure");
                                } else {
                                    final var bytesToSend = between(1, bytesRemaining);
                                    bytesRemaining -= bytesToSend;
                                    return ReleasableBytesReference.wrap(new ZeroBytesReference(bytesToSend));
                                }
                            }

                            @Override
                            public String getResponseContentTypeString() {
                                return RestResponse.TEXT_CONTENT_TYPE;
                            }
                        }, null))
                    );
                }
            });
        }
    }
}
