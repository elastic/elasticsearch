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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * The REST handler for get alias and head alias APIs.
 */
public class RestGetAliasesAction extends BaseRestHandler {

    public RestGetAliasesAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_alias/{name}", this);
        controller.registerHandler(HEAD, "/_alias/{name}", this);
        controller.registerHandler(GET, "/{index}/_alias/{name}", this);
        controller.registerHandler(HEAD, "/{index}/_alias/{name}", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] aliases = request.paramAsStringArrayOrEmptyIfAll("name");
        final GetAliasesRequest getAliasesRequest = new GetAliasesRequest(aliases);
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        getAliasesRequest.indices(indices);
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));

        return channel -> client.admin().indices().getAliases(getAliasesRequest, new RestBuilderListener<GetAliasesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAliasesResponse response, XContentBuilder builder) throws Exception {
                if (response.getAliases().isEmpty()) {
                    // empty body if indices were specified but no matching aliases exist
                    if (indices.length > 0) {
                        return new BytesRestResponse(OK, builder.startObject().endObject());
                    } else {
                        final String message = String.format(Locale.ROOT, "alias [%s] missing", toNamesString(getAliasesRequest.aliases()));
                        builder.startObject();
                        {
                            builder.field("error", message);
                            builder.field("status", RestStatus.NOT_FOUND.getStatus());
                        }
                        builder.endObject();
                        return new BytesRestResponse(RestStatus.NOT_FOUND, builder);
                    }
                } else {
                    builder.startObject();
                    {
                        for (final ObjectObjectCursor<String, List<AliasMetaData>> entry : response.getAliases()) {
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
                    builder.endObject();
                    return new BytesRestResponse(OK, builder);
                }
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
