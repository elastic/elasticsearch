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
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
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

/**
 * The REST handler for get alias and head alias APIs.
 */
public class RestGetAliasesAction extends BaseRestHandler {

    public RestGetAliasesAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_alias/{name}", this);
        controller.registerHandler(HEAD, "/_alias/{name}", this);
        controller.registerHandler(GET, "/{index}/_alias", this);
        controller.registerHandler(HEAD, "/{index}/_alias", this);
        controller.registerHandler(GET, "/{index}/_alias/{name}", this);
        controller.registerHandler(HEAD, "/{index}/_alias/{name}", this);
    }

    @Override
    public String getName() {
        return "get_aliases_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final boolean namesProvided = request.hasParam("name");
        final String[] aliases = request.paramAsStringArrayOrEmptyIfAll("name");
        final GetAliasesRequest getAliasesRequest = new GetAliasesRequest(aliases);
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        getAliasesRequest.indices(indices);
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));

        return channel -> client.admin().indices().getAliases(getAliasesRequest, new RestBuilderListener<GetAliasesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAliasesResponse response, XContentBuilder builder) throws Exception {
                final ImmutableOpenMap<String, List<AliasMetaData>> aliasMap = response.getAliases();

                final Set<String> aliasNames = new HashSet<>();
                final Set<String> indicesToDisplay = new HashSet<>();
                for (final ObjectObjectCursor<String, List<AliasMetaData>> cursor : aliasMap) {
                    for (final AliasMetaData aliasMetaData : cursor.value) {
                        aliasNames.add(aliasMetaData.alias());
                        if (namesProvided) {
                            indicesToDisplay.add(cursor.key);
                        }
                    }
                }

                // first remove requested aliases that are exact matches
                final SortedSet<String> difference = Sets.sortedDifference(Arrays.stream(aliases).collect(Collectors.toSet()), aliasNames);

                // now remove requested aliases that contain wildcards that are simple matches
                final List<String> matches = new ArrayList<>();
                outer:
                for (final String pattern : difference) {
                    if (pattern.contains("*")) {
                        for (final String aliasName : aliasNames) {
                            if (Regex.simpleMatch(pattern, aliasName)) {
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
                            message = String.format(Locale.ROOT, "alias [%s] missing", toNamesString(difference.iterator().next()));
                        } else {
                            message = String.format(Locale.ROOT, "aliases [%s] missing", toNamesString(difference.toArray(new String[0])));
                        }
                        builder.field("error", message);
                        builder.field("status", status.getStatus());
                    }

                    for (final ObjectObjectCursor<String, List<AliasMetaData>> entry : response.getAliases()) {
                        if (namesProvided == false || (namesProvided && indicesToDisplay.contains(entry.key))) {
                            builder.startObject(entry.key);
                            {
                                builder.startObject("aliases");
                                {
                                    for (final AliasMetaData alias : entry.value) {
                                        AliasMetaData.Builder.toXContent(alias, builder, ToXContent.EMPTY_PARAMS);
                                    }
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                    }
                }
                builder.endObject();
                return new BytesRestResponse(status, builder);
            }

        });
    }

    private static String toNamesString(final String... names) {
        if (names == null || names.length == 0) {
            return "";
        } else if (names.length == 1) {
            return names[0];
        } else {
            return Arrays.stream(names).collect(Collectors.joining(","));
        }
    }

}
