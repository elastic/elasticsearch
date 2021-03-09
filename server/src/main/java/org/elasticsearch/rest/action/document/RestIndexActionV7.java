/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.RestApiVersion;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestIndexActionV7 {
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in document "
        + "index requests is deprecated, use the typeless endpoints instead (/{index}/_doc/{id}, /{index}/_doc, "
        + "or /{index}/_create/{id}).";

    public static class CompatibleRestIndexAction extends RestIndexAction {
        @Override
        public String getName() {
            return super.getName() + "_v7";
        }

        @Override
        public List<Route> routes() {
            return List.of(Route.builder(POST, "/{index}/{type}/{id}")
                    .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                    .build(),
                Route.builder(PUT, "/{index}/{type}/{id}")
                    .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                    .build());
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            request.param("type");
            return super.prepareRequest(request, client);
        }
    }

    public static class CompatibleCreateHandler extends RestIndexAction.CreateHandler {

        @Override
        public String getName() {
            return "document_create_action_v7";
        }

        @Override
        public List<Route> routes() {
            return List.of(Route.builder(POST, "/{index}/{type}/{id}/_create")
                    .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                    .build(),
                Route.builder(PUT, "/{index}/{type}/{id}/_create")
                    .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                    .build());
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            request.param("type");
            return super.prepareRequest(request, client);
        }
    }

    public static final class CompatibleAutoIdHandler extends RestIndexAction.AutoIdHandler {

        public CompatibleAutoIdHandler(Supplier<DiscoveryNodes> nodesInCluster) {
            super(nodesInCluster);
        }

        @Override
        public String getName() {
            return "document_create_action_auto_id_v7";
        }

        @Override
        public List<Route> routes() {
            return singletonList(Route.builder(POST, "/{index}/{type}")
                .deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7)
                .build());
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            request.param("type");
            return super.prepareRequest(request, client);
        }
    }
}
