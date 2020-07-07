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

import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.action.admin.indices.RestCreateIndexActionV7.DEFAULT_INCLUDE_TYPE_NAME_POLICY;
import static org.elasticsearch.rest.action.admin.indices.RestCreateIndexActionV7.INCLUDE_TYPE_NAME_PARAMETER;

public class RestPutMappingActionV7 extends RestPutMappingAction {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutMappingActionV7.class);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in put "
        + "mapping requests is deprecated. The parameter will be removed in the next major version.";

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(PUT, "/{index}/{type}/_mapping"),
            new Route(PUT, "/{index}/_mapping/{type}"),
            new Route(PUT, "/_mapping/{type}"),

            new Route(POST, "/{index}/{type}/_mapping"),
            new Route(POST, "/{index}/_mapping/{type}"),
            new Route(POST, "/_mapping/{type}"),

            // register the same paths, but with plural form _mappings
            new Route(PUT, "/{index}/{type}/_mappings"),
            new Route(PUT, "/{index}/_mappings/{type}"),
            new Route(PUT, "/_mappings/{type}"),

            new Route(POST, "/{index}/{type}/_mappings"),
            new Route(POST, "/{index}/_mappings/{type}"),
            new Route(POST, "/_mappings/{type}")
        );
    }

    @Override
    public String getName() {
        return "put_mapping_action_v7";
    }

    @Override
    public Version compatibleWithVersion() {
        return Version.V_7_0_0;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.deprecate("put_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        String type = request.param("type");
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();
        if (includeTypeName == false
            && (type != null || MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, sourceAsMap))) {
            throw new IllegalArgumentException(
                "Types cannot be provided in put mapping requests, unless " + "the include_type_name parameter is set to true."
            );
        }
        return super.sendPutMappingRequest(request, client, sourceAsMap);
    }
}
