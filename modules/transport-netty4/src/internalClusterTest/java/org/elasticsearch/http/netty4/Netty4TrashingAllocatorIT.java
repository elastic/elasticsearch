/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.http.HttpServerTransport;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Netty4TrashingAllocatorIT extends ESNetty4IntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(Handler.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testTrashContent() throws InterruptedException {
        try (var client = new Netty4HttpClient()) {
            var addr = randomFrom(internalCluster().getInstance(HttpServerTransport.class).boundAddress().boundAddresses()).address();
            var content = randomAlphaOfLength(between(1024, 2048));
            var responses = client.post(addr, List.of(new Tuple<>(Handler.ROUTE, content)));
            assertEquals(HttpResponseStatus.OK, responses.stream().findFirst().get().status());
        }
    }

    public static class Handler extends Plugin implements ActionPlugin {
        static final String ROUTE = "/_test/trashing-alloc";

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
                    var content = request.content();
                    var iter = content.iterator();
                    return (chan) -> {
                        request.getHttpRequest().release();
                        assertFalse(content.hasReferences());
                        BytesRef br;
                        while ((br = iter.next()) != null) {
                            for (int i = br.offset; i < br.offset + br.length; i++) {
                                if (br.bytes[i] != 0) {
                                    fail(
                                        new AssertionError(
                                            "buffer is not trashed, off="
                                                + br.offset
                                                + " len="
                                                + br.length
                                                + " pos="
                                                + i
                                                + " ind="
                                                + (i - br.offset)
                                        )
                                    );
                                }
                            }
                        }
                        chan.sendResponse(new RestResponse(RestStatus.OK, ""));
                    };
                }
            });
        }
    }
}
