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

package org.elasticsearch.rest.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSearchActionV7 extends RestSearchAction {
    public static final String INCLUDE_TYPE_NAME_PARAMETER = "include_type_name";
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal]" + " Specifying types in search requests is deprecated.";

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestSearchActionV7.class);

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_search"),
            new Route(POST, "/_search"),
            new Route(GET, "/{index}/_search"),
            new Route(POST, "/{index}/_search"),
            new Route(GET, "/{index}/{type}/_search"),
            new Route(POST, "/{index}/{type}/_search")
        );
    }

    @Override
    public String getName() {
        return super.getName() + "_v7";
    }

    @Override
    public Version compatibleWithVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
        request.param(INCLUDE_TYPE_NAME_PARAMETER);

        if (request.hasParam("type")) {
            deprecationLogger.deprecate("search_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        return super.prepareRequest(request, client);
    }
}
