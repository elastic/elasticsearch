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

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 * The REST handler for retrieving all aliases
 */
public class RestGetAllAliasesAction extends BaseRestHandler {

    public RestGetAllAliasesAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(GET, "/_alias", this);
        controller.registerHandler(GET, "/_aliases", this);
    }

    @Override
    public String getName() {
        return "get_all_aliases_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(Strings.EMPTY_ARRAY);
        getIndexRequest.features(Feature.ALIASES);
        getIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, getIndexRequest.indicesOptions()));
        getIndexRequest.local(request.paramAsBoolean("local", getIndexRequest.local()));
        getIndexRequest.humanReadable(request.paramAsBoolean("human", false));
        return channel -> client.admin().indices().getIndex(getIndexRequest, new RestBuilderListener<GetIndexResponse>(channel) {

            @Override
            public RestResponse buildResponse(final GetIndexResponse response, final XContentBuilder builder) throws Exception {
                builder.startObject();
                {
                    for (final String index : response.indices()) {
                        builder.startObject(index);
                        {
                            writeAliases(response.aliases().get(index), builder, request);
                        }
                        builder.endObject();
                    }
                }
                builder.endObject();

                return new BytesRestResponse(OK, builder);
            }

            private void writeAliases(final List<AliasMetaData> aliases, final XContentBuilder builder,
                                      final Params params) throws IOException {
                builder.startObject("aliases");
                {
                    if (aliases != null) {
                        for (final AliasMetaData alias : aliases) {
                            AliasMetaData.Builder.toXContent(alias, builder, params);
                        }
                    }
                }
                builder.endObject();
            }
        });
    }

}
