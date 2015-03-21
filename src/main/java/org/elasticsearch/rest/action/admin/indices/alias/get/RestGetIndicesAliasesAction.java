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
package org.elasticsearch.rest.action.admin.indices.alias.get;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.common.Strings.isAllOrWildcard;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

/**
 */
@Deprecated
public class RestGetIndicesAliasesAction extends BaseRestHandler {

    @Inject
    public RestGetIndicesAliasesAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/{index}/_aliases/{name}", this);
        controller.registerHandler(GET, "/_aliases/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] aliases = Strings.splitStringByCommaToArray(request.param("name"));

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest()
                .routingTable(false)
                .nodes(false)
                .indices(indices);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.listenerThreaded(false);

        client.admin().cluster().state(clusterStateRequest, new RestBuilderListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse response, XContentBuilder builder) throws Exception {
                MetaData metaData = response.getState().metaData();
                builder.startObject();

                final boolean isAllAliasesRequested = isAllOrWildcard(aliases);
                for (IndexMetaData indexMetaData : metaData) {
                    builder.startObject(indexMetaData.index(), XContentBuilder.FieldCaseConversion.NONE);
                    builder.startObject("aliases");

                    for (ObjectCursor<AliasMetaData> cursor : indexMetaData.aliases().values()) {
                        if (isAllAliasesRequested || Regex.simpleMatch(aliases, cursor.value.alias())) {
                            AliasMetaData.Builder.toXContent(cursor.value, builder, ToXContent.EMPTY_PARAMS);
                        }
                    }

                    builder.endObject();
                    builder.endObject();
                }

                builder.endObject();
                return new BytesRestResponse(OK, builder);
            }
        });
    }

}