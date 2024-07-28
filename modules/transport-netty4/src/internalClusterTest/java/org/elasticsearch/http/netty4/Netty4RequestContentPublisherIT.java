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
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Netty4RequestContentPublisherIT extends ESNetty4IntegTestCase {

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
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                    return new RestChannelConsumer() {
                        @Override
                        public void accept(RestChannel channel) {

                            // netty channel will hold all chunks until we subscribe
                            request.contentPublisher().subscribe(new Flow.Subscriber<>() {
                                Flow.Subscription subscription;
                                int totalReceivedBytes;

                                @Override
                                public void onSubscribe(Flow.Subscription subscription) {
                                    this.subscription = subscription;
                                    // after subscription, we can request N next messages, for example 5 chunks
                                    subscription.request(5);
                                }

                                @Override
                                public void onNext(HttpContent item) {
                                    // chunk handler
                                    var contentSize = item.content().length();
                                    LOGGER.info("got next item of a size {}", contentSize);
                                    totalReceivedBytes += contentSize;
                                    item.release();
                                    // we need explicitly ask for next N chunks
                                    subscription.request(1);
                                }

                                @Override
                                public void onError(Throwable throwable) {
                                    // not implemented yet
                                    assert false : throwable.getMessage();
                                }

                                @Override
                                public void onComplete() {
                                    // completion event after LastHttpContent
                                    LOGGER.info("complete");
                                    channel.sendResponse(new RestResponse(RestStatus.OK, Integer.toString(totalReceivedBytes)));
                                }
                            });

                        }
                    };
                }
            });
        }
    }

}
