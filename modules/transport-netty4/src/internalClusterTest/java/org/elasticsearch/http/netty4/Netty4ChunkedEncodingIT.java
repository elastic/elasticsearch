/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestResponse.TEXT_CONTENT_TYPE;
import static org.hamcrest.Matchers.containsString;

public class Netty4ChunkedEncodingIT extends ESNetty4IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(YieldsChunksPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    private static final String expectedBody = """
        chunk-0
        chunk-1
        chunk-2
        """;

    public void testLotsOfBasic() throws IOException {
        for (int i = 0 ; i < 10000; i++) {
            testBasic();
        }
    }

    public void testBasic() throws IOException {
        final var response = getRestClient().performRequest(new Request("GET", YieldsChunksPlugin.ROUTE));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(response.getEntity().getContentType().toString(), containsString(TEXT_CONTENT_TYPE));
        assertTrue(response.getEntity().isChunked());
        final String body;
        try (var reader = new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8)) {
            body = Streams.copyToString(reader);
        }
        assertEquals(expectedBody, body);
    }

    public static class YieldsChunksPlugin extends Plugin implements ActionPlugin {
        static final String ROUTE = "/_test/yields_chunks";

        public static class Request extends ActionRequest {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }

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
            Supplier<DiscoveryNodes> nodesInCluster
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
                    return channel -> channel.sendResponse(RestResponse.chunked(RestStatus.OK, new ChunkedRestResponseBody() {

                        final Iterator<BytesReference> chunkIterator = Iterators.concat(
                            emptyChunks(),
                            Iterators.flatMap(
                                Iterators.forRange(0, 3, i -> "chunk-" + i + '\n'),
                                chunk -> Iterators.concat(Iterators.single(new BytesArray(chunk)), emptyChunks())
                            )
                        );

                        @Override
                        public boolean isDone() {
                            return chunkIterator.hasNext() == false;
                        }

                        @Override
                        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) {
                            final var page = recycler.obtain(); // just to ensure nothing is leaked
                            return new ReleasableBytesReference(chunkIterator.next(), page);
                        }

                        @Override
                        public String getResponseContentTypeString() {
                            return TEXT_CONTENT_TYPE;
                        }
                    }, null));
                }
            });
        }
    }
}
