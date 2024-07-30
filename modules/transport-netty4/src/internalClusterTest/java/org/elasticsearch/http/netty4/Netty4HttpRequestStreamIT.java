/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpContent;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Netty4HttpRequestStreamIT extends ESNetty4IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(RequestContentStreamPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testBasicStream() throws IOException {
        var totalBytes = 1024 * 1024;
        var request = new Request("POST", RequestContentStreamPlugin.ROUTE);
        request.setEntity(new ByteArrayEntity(randomByteArrayOfLength(totalBytes), ContentType.APPLICATION_JSON));

        var respose = getRestClient().performRequest(request);
        assertEquals(200, respose.getStatusLine().getStatusCode());
        var gotTotalBytes = new BytesArray(respose.getEntity().getContent().readAllBytes()).utf8ToString();
        assertEquals("" + totalBytes, gotTotalBytes);
    }

    public static class RequestContentStreamPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/request-stream/basic";
        private static final Logger LOGGER = LogManager.getLogger("StreamRestHandler");

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
                    return List.of(new Route(RestRequest.Method.POST, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    return channel -> {

                        // Example of handling streamed content

                        var contentStream = request.contentStream();
                        var totalReceivedBytes = new AtomicInteger();

                        Consumer<HttpContent.Chunk> contentConsumer = (chunk) -> {
                            LOGGER.info("got next chunk with size {}", chunk.bytes().length());
                            totalReceivedBytes.addAndGet(chunk.bytes().length());
                            if (chunk.isLast() == false) {
                                contentStream.request(1024 * 10); // ask for 10kb, will round up to 16kb
                            } else {
                                channel.sendResponse(new RestResponse(RestStatus.OK, Integer.toString(totalReceivedBytes.get())));
                            }
                        };

                        contentStream.setHandler(contentConsumer); // must setup handler before first request
                        contentStream.request(1024); // initiate first chunk, will round up to 8kb
                    };
                }
            });
        }
    }

}
