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

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutMappingAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/{index}/_mapping/"),
            new Route(PUT, "/{index}/_mapping/"),
            new Route(POST, "/{index}/_mappings/"),
            new Route(PUT, "/{index}/_mappings/"));
    }

    @Override
    public String getName() {
        return "put_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PutMappingRequest putMappingRequest = putMappingRequest(Strings.splitStringByCommaToArray(request.param("index")));

        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false,
            request.getXContentType()).v2();
        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, sourceAsMap)) {
            throw new IllegalArgumentException("Types cannot be provided in put mapping requests");
        }

        putMappingRequest.source(sourceAsMap);
        putMappingRequest.timeout(request.paramAsTime("timeout", putMappingRequest.timeout()));
        putMappingRequest.masterNodeTimeout(request.paramAsTime("master_timeout", putMappingRequest.masterNodeTimeout()));
        putMappingRequest.indicesOptions(IndicesOptions.fromRequest(request, putMappingRequest.indicesOptions()));
        putMappingRequest.writeIndexOnly(request.paramAsBoolean("write_index_only", false));
        return channel -> client.admin().indices().putMapping(putMappingRequest, new RestToXContentListener<>(channel));
    }
}
