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

package org.elasticsearch.graphql.api;

import static org.elasticsearch.graphql.api.GqlApiUtils.futureToListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.client.node.NodeClient;
import static org.elasticsearch.rest.RestRequest.Method.GET;

import org.elasticsearch.rest.action.cat.RestIndicesAction;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class GqlElasticsearchApi implements GqlApi {
    NodeClient client;

    public GqlElasticsearchApi(NodeClient client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<Map<String, Object>> getHello() throws Exception {
        return GqlApiUtils.executeAction(client, MainAction.INSTANCE, new MainRequest());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<Object>> getIndices() throws Exception {
        return GqlApiUtils.executeRestHandler(client, RestIndicesAction.INSTANCE, GET, "/_cat/indices?format=json");
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<String, Object>> getIndex(String indexName) throws Exception {
        String[] indices = { indexName };

        final GetIndexRequest getIndexRequest = new GetIndexRequest()
            .indices(indices);
//            .indicesOptions(options);
//        getIndexRequest.local(request.paramAsBoolean("local", getIndexRequest.local()));
//        getIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getIndexRequest.masterNodeTimeout()));
//        getIndexRequest.humanReadable(request.paramAsBoolean("human", false));
//        getIndexRequest.includeDefaults(request.paramAsBoolean("include_defaults", false));
        CompletableFuture<GetIndexResponse> future = new CompletableFuture();
        client.admin().indices().getIndex(getIndexRequest, futureToListener(future));

        return future
            .thenApply(GqlApiUtils::toMapSafe)
            .thenApply(map -> {
                System.out.println("map " + map);
                try {
                    map = (Map<String, Object>) map.get(indexName);
                    map = (Map<String, Object>) map.get("settings");
                    map = (Map<String, Object>) map.get("index");
                    return map;
                } catch (Exception e) {
                    return null;
                }
            });
    }
}
