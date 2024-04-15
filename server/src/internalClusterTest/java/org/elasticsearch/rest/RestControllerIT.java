/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class RestControllerIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    public void testHeadersEmittedWithChunkedResponses() throws IOException {
        final var client = getRestClient();
        final var response = client.performRequest(new Request("GET", ChunkedResponseWithHeadersPlugin.ROUTE));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(ChunkedResponseWithHeadersPlugin.HEADER_VALUE, response.getHeader(ChunkedResponseWithHeadersPlugin.HEADER_NAME));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ChunkedResponseWithHeadersPlugin.class);
    }

    public static class ChunkedResponseWithHeadersPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/chunked_response_with_headers";
        static final String HEADER_NAME = "test-header";
        static final String HEADER_VALUE = "test-header-value";

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
                    return ChunkedResponseWithHeadersPlugin.class.getCanonicalName();
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    return channel -> {
                        final var response = RestResponse.chunked(
                            RestStatus.OK,
                            ChunkedRestResponseBody.fromXContent(
                                params -> Iterators.single((b, p) -> b.startObject().endObject()),
                                request,
                                channel
                            ),
                            null
                        );
                        response.addHeader(HEADER_NAME, HEADER_VALUE);
                        channel.sendResponse(response);
                    };
                }
            });
        }
    }
}
