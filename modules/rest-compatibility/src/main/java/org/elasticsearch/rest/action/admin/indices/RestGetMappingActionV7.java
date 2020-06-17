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

package org.elasticsearch.rest.action.admin.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.action.admin.indices.RestCreateIndexActionV7.DEFAULT_INCLUDE_TYPE_NAME_POLICY;
import static org.elasticsearch.rest.action.admin.indices.RestCreateIndexActionV7.INCLUDE_TYPE_NAME_PARAMETER;

public class RestGetMappingActionV7 extends RestGetMappingAction {
    private static final Logger logger = LogManager.getLogger(RestGetMappingAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get" +
        " mapping requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/{index}/{type}/_mapping"),
            new Route(GET, "/{index}/_mappings/{type}"),
            new Route(GET, "/{index}/_mapping/{type}"),
            new Route(HEAD, "/{index}/_mapping/{type}"),
            new Route(GET, "/_mapping/{type}"));
    }

    @Override
    public String getName() {
        return "get_mapping_action_v7";
    }

    @Override
    public Version compatibleWithVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
        boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        if (request.method().equals(HEAD)) {
            deprecationLogger.deprecate("get_mapping_with_types","Type exists requests are deprecated, as types have been deprecated.");
        } else if (includeTypeName == false && types.length > 0) {
            throw new IllegalArgumentException("Types cannot be provided in get mapping requests, unless" +
                " include_type_name is set to true.");
        }
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
            deprecationLogger.deprecate("get_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }
        if (types.length > 0) {
            //todo compatible log about using types in path
        }
        return super.prepareRequest(request, client);
    }
}
