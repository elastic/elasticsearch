/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest.action.admin.indices;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestGetMappingAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestGetMappingAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Using include_type_name in get" +
        " mapping requests is deprecated. The parameter will be removed in the next major version.";

    public RestGetMappingAction(final RestController controller) {
        controller.registerHandler(GET, "/_mapping", this);
        controller.registerHandler(GET, "/_mappings", this);
        controller.registerHandler(GET, "/{index}/{type}/_mapping", this);
        controller.registerHandler(GET, "/{index}/_mappings", this);
        controller.registerHandler(GET, "/{index}/_mapping", this);
        controller.registerHandler(GET, "/{index}/_mappings/{type}", this);
        controller.registerHandler(GET, "/{index}/_mapping/{type}", this);
        controller.registerHandler(HEAD, "/{index}/_mapping/{type}", this);
        controller.registerHandler(GET, "/_mapping/{type}", this);
    }

    @Override
    public String getName() {
        return "get_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] types = request.paramAsStringArrayOrEmptyIfAll("type");
        boolean includeTypeName = request.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER, DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        if (request.method().equals(HEAD)) {
            deprecationLogger.deprecated("Type exists requests are deprecated, as types have been deprecated.");
        } else if (includeTypeName == false && types.length > 0) {
            throw new IllegalArgumentException("Types cannot be provided in get mapping requests, unless" +
                " include_type_name is set to true.");
        }
        if (request.hasParam(INCLUDE_TYPE_NAME_PARAMETER)) {
            deprecationLogger.deprecatedAndMaybeLog("get_mapping_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices).types(types);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        getMappingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getMappingsRequest.masterNodeTimeout()));
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        return channel -> client.admin().indices().getMappings(getMappingsRequest, new RestBuilderListener<GetMappingsResponse>(channel) {
            @Override
            public RestResponse buildResponse(final GetMappingsResponse response, final XContentBuilder builder) throws Exception {
                final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappingsByIndex = response.getMappings();
                if (mappingsByIndex.isEmpty() && types.length != 0) {
                    builder.close();
                    return new BytesRestResponse(channel, new TypeMissingException("_all", String.join(",", types)));
                }

                final Set<String> typeNames = new HashSet<>();
                for (final ObjectCursor<ImmutableOpenMap<String, MappingMetaData>> cursor : mappingsByIndex.values()) {
                    for (final ObjectCursor<String> inner : cursor.value.keys()) {
                        typeNames.add(inner.value);
                    }
                }

                final SortedSet<String> difference = Sets.sortedDifference(Arrays.stream(types).collect(Collectors.toSet()), typeNames);

                // now remove requested aliases that contain wildcards that are simple matches
                final List<String> matches = new ArrayList<>();
                outer:
                for (final String pattern : difference) {
                    if (pattern.contains("*")) {
                        for (final String typeName : typeNames) {
                            if (Regex.simpleMatch(pattern, typeName)) {
                                matches.add(pattern);
                                continue outer;
                            }
                        }
                    }
                }
                difference.removeAll(matches);

                final RestStatus status;
                builder.startObject();
                {
                    if (difference.isEmpty()) {
                        status = RestStatus.OK;
                    } else {
                        status = RestStatus.NOT_FOUND;
                        final String message = String.format(Locale.ROOT, "type" + (difference.size() == 1 ? "" : "s") +
                            " [%s] missing", Strings.collectionToCommaDelimitedString(difference));
                        builder.field("error", message);
                        builder.field("status", status.getStatus());
                    }
                    response.toXContent(builder, request);
                }
                builder.endObject();

                return new BytesRestResponse(status, builder);
            }
        });
    }
}
