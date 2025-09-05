/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestResponse.TEXT_CONTENT_TYPE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class Netty4ChunkedEncodingIT extends ESNetty4IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(YieldsChunksPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    private static final String EXPECTED_NONEMPTY_BODY = """
        chunk-0
        chunk-1
        chunk-2
        """;

    public void testNonemptyResponse() throws IOException {
        getAndCheckBodyContents(YieldsChunksPlugin.CHUNKS_ROUTE, EXPECTED_NONEMPTY_BODY);
    }

    public void testEmptyResponse() throws IOException {
        getAndCheckBodyContents(YieldsChunksPlugin.EMPTY_ROUTE, "");
    }

    private static void getAndCheckBodyContents(String route, String expectedBody) throws IOException {
        try (var ignored = withResourceTracker()) {
            final var response = getRestClient().performRequest(new Request("GET", route));
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertThat(response.getEntity().getContentType().toString(), containsString(TEXT_CONTENT_TYPE));
            if (Strings.hasLength(expectedBody)) {
                assertTrue(response.getEntity().isChunked());
            } // else we might have no chunks to send which doesn't need chunked-encoding
            final String body;
            try (var reader = new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)) {
                body = Streams.copyToString(reader);
            }
            assertEquals(expectedBody, body);
        }
    }

    public void testClientCancellation() {
        try (var ignored = withResourceTracker()) {
            final var cancellable = getRestClient().performRequestAsync(
                new Request("GET", YieldsChunksPlugin.INFINITE_ROUTE),
                new ResponseListener() {
                    @Override
                    public void onSuccess(Response response) {
                        fail("should not complete");
                    }

                    @Override
                    public void onFailure(Exception exception) {
                        assertThat(exception, instanceOf(CancellationException.class));
                    }
                }
            );
            if (randomBoolean()) {
                safeSleep(scaledRandomIntBetween(10, 500));
            }
            cancellable.cancel();
        }
    }

    private static Releasable withResourceTracker() {
        assertNull(refs);
        final var latch = new CountDownLatch(1);
        refs = AbstractRefCounted.of(latch::countDown);
        return () -> {
            refs.decRef();
            try {
                safeAwait(latch);
            } finally {
                refs = null;
            }
        };
    }

    private static volatile RefCounted refs = null;

    public static class YieldsChunksPlugin extends Plugin implements ActionPlugin {
        static final String CHUNKS_ROUTE = "/_test/yields_chunks";
        static final String EMPTY_ROUTE = "/_test/yields_only_empty_chunks";
        static final String INFINITE_ROUTE = "/_test/yields_infinite_chunks";

        private static Iterator<BytesReference> emptyChunks() {
            return Iterators.forRange(0, between(0, 2), i -> BytesArray.EMPTY);
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
            return List.of(
                // 3 nonempty chunks, with some random empty chunks in between
                new BaseRestHandler() {
                    @Override
                    public String getName() {
                        return CHUNKS_ROUTE;
                    }

                    @Override
                    public List<Route> routes() {
                        return List.of(new Route(GET, CHUNKS_ROUTE));
                    }

                    @Override
                    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                        return channel -> sendChunksResponse(
                            channel,
                            Iterators.concat(
                                emptyChunks(),
                                Iterators.flatMap(
                                    Iterators.forRange(0, 3, i -> "chunk-" + i + '\n'),
                                    chunk -> Iterators.concat(Iterators.single(new BytesArray(chunk)), emptyChunks())
                                )
                            )
                        );
                    }
                },

                // only a few random empty chunks
                new BaseRestHandler() {
                    @Override
                    public String getName() {
                        return EMPTY_ROUTE;
                    }

                    @Override
                    public List<Route> routes() {
                        return List.of(new Route(GET, EMPTY_ROUTE));
                    }

                    @Override
                    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                        return channel -> sendChunksResponse(channel, emptyChunks());
                    }
                },

                // keeps on emitting chunks until cancelled
                new BaseRestHandler() {
                    @Override
                    public String getName() {
                        return INFINITE_ROUTE;
                    }

                    @Override
                    public List<Route> routes() {
                        return List.of(new Route(GET, INFINITE_ROUTE));
                    }

                    @Override
                    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                        return channel -> sendChunksResponse(channel, new Iterator<>() {
                            private static final BytesReference CHUNK = new BytesArray("CHUNK\n");

                            @Override
                            public boolean hasNext() {
                                return true;
                            }

                            @Override
                            public BytesReference next() {
                                return CHUNK;
                            }
                        });
                    }
                }
            );
        }

        private static void sendChunksResponse(RestChannel channel, Iterator<BytesReference> chunkIterator) {
            final var localRefs = refs; // single volatile read
            if (localRefs != null && localRefs.tryIncRef()) {
                channel.sendResponse(RestResponse.chunked(RestStatus.OK, new ChunkedRestResponseBodyPart() {
                    @Override
                    public boolean isPartComplete() {
                        return chunkIterator.hasNext() == false;
                    }

                    @Override
                    public boolean isLastPart() {
                        return true;
                    }

                    @Override
                    public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
                        assert false : "no continuations";
                    }

                    @Override
                    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) {
                        localRefs.mustIncRef();
                        return new ReleasableBytesReference(chunkIterator.next(), localRefs::decRef);
                    }

                    @Override
                    public String getResponseContentTypeString() {
                        return TEXT_CONTENT_TYPE;
                    }
                }, localRefs::decRef));
            } else {
                try {
                    channel.sendResponse(new RestResponse(channel, new TaskCancelledException("task cancelled")));
                } catch (IOException e) {
                    fail(e);
                }
            }
        }
    }
}
