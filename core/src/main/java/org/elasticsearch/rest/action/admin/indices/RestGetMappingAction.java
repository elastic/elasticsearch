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
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
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
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetMappingAction extends BaseRestHandler {

    public RestGetMappingAction(final Settings settings, final RestController controller) {
        super(settings);
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
        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices).types(types);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        return channel -> client.admin().indices().getMappings(getMappingsRequest, new RestBuilderListener<GetMappingsResponse>(channel) {
            @Override
            public RestResponse buildResponse(final GetMappingsResponse response, final XContentBuilder builder) throws Exception {
                final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappingsByIndex = response.getMappings();
                if (mappingsByIndex.isEmpty() && (indices.length != 0 || types.length != 0)) {
                    if (indices.length != 0 && types.length == 0) {
                        builder.close();
                        return new BytesRestResponse(channel, new IndexNotFoundException(String.join(",", indices)));
                    } else {
                        builder.close();
                        return new BytesRestResponse(channel, new TypeMissingException("_all", String.join(",", types)));
                    }
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
                        final String message;
                        if (difference.size() == 1) {
                            message = String.format(Locale.ROOT, "type [%s] missing", toNamesString(difference.iterator().next()));
                        } else {
                            message = String.format(Locale.ROOT, "types [%s] missing", toNamesString(difference.toArray(new String[0])));
                        }
                        builder.field("error", message);
                        builder.field("status", status.getStatus());
                    }

                    for (final ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappingsByIndex) {
                        builder.startObject(indexEntry.key);
                        {
                            builder.startObject("mappings");
                            {
                                for (final ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
                                    builder.field(typeEntry.key, typeEntry.value.sourceAsMap());
                                }
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                }
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }
        });
    }

    private static String toNamesString(final String... names) {
        return Arrays.stream(names).collect(Collectors.joining(","));
    }

}
