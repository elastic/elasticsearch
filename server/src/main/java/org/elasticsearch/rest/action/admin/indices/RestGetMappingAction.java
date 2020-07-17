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

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetMappingAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_mapping"),
            new Route(GET, "/_mappings"),
            new Route(GET, "/{index}/_mapping"),
            new Route(GET, "/{index}/_mappings"));
    }

    @Override
    public String getName() {
        return "get_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        getMappingsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getMappingsRequest.masterNodeTimeout()));
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        return channel -> client.admin().indices().getMappings(getMappingsRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetMappingsResponse response, final XContentBuilder builder) throws Exception {
                builder.startObject();
                response.toXContent(builder, request);
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
