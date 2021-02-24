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
import org.elasticsearch.common.compatibility.RestApiCompatibleVersion;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
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
    static final String COMPATIBLE_API_MESSAGE = "[Compatible API usage] Index API with types has been removed, use typeless endpoints.";

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestIndexActionV7.class);

    private static void logDeprecationMessage() {
        deprecationLogger.deprecate(DeprecationCategory.MAPPINGS, "index_with_types", TYPES_DEPRECATION_MESSAGE);
        deprecationLogger.compatibleApiWarning("index_with_types", COMPATIBLE_API_MESSAGE);
    }

    public static class CompatibleRestIndexAction extends RestIndexAction {
        @Override
        public String getName() {
            return super.getName() + "_v7";
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/{index}/{type}/{id}"), new Route(PUT, "/{index}/{type}/{id}"));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            logDeprecationMessage();
            request.param("type");
            return super.prepareRequest(request, client);
        }

        @Override
        public RestApiCompatibleVersion compatibleWithVersion() {
            return RestApiCompatibleVersion.V_7;
        }
    }

    public static class CompatibleCreateHandler extends RestIndexAction.CreateHandler {

        @Override
        public String getName() {
            return "document_create_action_v7";
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/{index}/{type}/{id}/_create"), new Route(PUT, "/{index}/{type}/{id}/_create"));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            logDeprecationMessage();
            request.param("type");
            return super.prepareRequest(request, client);
        }

        @Override
        public RestApiCompatibleVersion compatibleWithVersion() {
            return RestApiCompatibleVersion.V_7;
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
            return singletonList(new Route(POST, "/{index}/{type}"));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            logDeprecationMessage();
            request.param("type");
            return super.prepareRequest(request, client);
        }

        @Override
        public RestApiCompatibleVersion compatibleWithVersion() {
            return RestApiCompatibleVersion.V_7;
        }
    }
}
