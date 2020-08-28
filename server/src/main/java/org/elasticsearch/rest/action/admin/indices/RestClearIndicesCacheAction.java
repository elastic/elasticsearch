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

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestClearIndicesCacheAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_cache/clear"),
            new Route(POST, "/{index}/_cache/clear"));
    }

    @Override
    public String getName() {
        return "clear_indices_cache_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(
                Strings.splitStringByCommaToArray(request.param("index")));
        clearIndicesCacheRequest.indicesOptions(IndicesOptions.fromRequest(request, clearIndicesCacheRequest.indicesOptions()));
        fromRequest(request, clearIndicesCacheRequest);
        return channel -> client.admin().indices().clearCache(clearIndicesCacheRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    public static ClearIndicesCacheRequest fromRequest(final RestRequest request, ClearIndicesCacheRequest clearIndicesCacheRequest) {
        clearIndicesCacheRequest.queryCache(request.paramAsBoolean("query", clearIndicesCacheRequest.queryCache()));
        clearIndicesCacheRequest.requestCache(request.paramAsBoolean("request", clearIndicesCacheRequest.requestCache()));
        clearIndicesCacheRequest.fieldDataCache(request.paramAsBoolean("fielddata", clearIndicesCacheRequest.fieldDataCache()));
        clearIndicesCacheRequest.fields(request.paramAsStringArray("fields", clearIndicesCacheRequest.fields()));
        return clearIndicesCacheRequest;
    }
}
