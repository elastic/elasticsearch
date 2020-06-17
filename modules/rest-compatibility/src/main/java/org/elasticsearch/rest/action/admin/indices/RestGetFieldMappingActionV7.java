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
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetFieldMappingActionV7 extends RestGetFieldMappingAction {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(RestGetFieldMappingAction.class));
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get " +
        "field mapping requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_mapping/field/{fields}"),
            new Route(GET, "/{index}/_mapping/field/{fields}"),

            new Route(GET, "/_mapping/{type}/field/{fields}"),
            new Route(GET, "/{index}/{type}/_mapping/field/{fields}"),
            new Route(GET, "/{index}/_mapping/{type}/field/{fields}"));
    }

    @Override
    public String getName() {
        return "get_field_mapping_action_v7";
    }

    @Override
    public Version compatibleWithVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");

        boolean includeTypeName = request.paramAsBoolean(RestCreateIndexActionV7.INCLUDE_TYPE_NAME_PARAMETER,
            RestCreateIndexActionV7.DEFAULT_INCLUDE_TYPE_NAME_POLICY);
        if (includeTypeName == false && types.length > 0) {
            throw new IllegalArgumentException("Types cannot be specified unless include_type_name" +
                " is set to true.");
        }
        if (request.hasParam(RestCreateIndexActionV7.INCLUDE_TYPE_NAME_PARAMETER)) {
            request.param(RestCreateIndexActionV7.INCLUDE_TYPE_NAME_PARAMETER);
            deprecationLogger.deprecate("get_field_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
            //todo compatible log about using INCLUDE_TYPE_NAME_PARAMETER
        }
        if (types.length > 0) {
            //todo compatible log about using types in path
        }
        if (request.hasParam("local")) {
            request.param("local");
            deprecationLogger.deprecate("get_field_mapping_local",
                "Use [local] in get field mapping requests is deprecated. "
                    + "The parameter will be removed in the next major version");
        }
        return super.prepareRequest(request, client);
    }
}
