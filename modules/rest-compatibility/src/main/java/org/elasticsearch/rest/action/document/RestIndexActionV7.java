/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.document;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestIndexActionV7.class);

    private static void logDeprecationMessage() {
        deprecationLogger.deprecate("index_with_types", TYPES_DEPRECATION_MESSAGE);
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
        public Version compatibleWithVersion() {
            return Version.V_7_0_0;
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
        public Version compatibleWithVersion() {
            return Version.V_7_0_0;
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
        public Version compatibleWithVersion() {
            return Version.V_7_0_0;
        }
    }
}
