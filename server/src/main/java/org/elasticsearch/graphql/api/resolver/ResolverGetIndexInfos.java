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

package org.elasticsearch.graphql.api.resolver;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.action.cat.RestIndicesAction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.graphql.api.GqlApiUtils.*;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class ResolverGetIndexInfos {

    @SuppressWarnings("unchecked")
    public static CompletableFuture<List<Object>> exec(NodeClient client) throws Exception {
        CompletableFuture<List<Object>> future = executeRestHandler(client, RestIndicesAction.INSTANCE, GET, "/_cat/indices?format=json");

        return future
            .thenApply(list -> {
                for (Object obj : list) {
                    Map<String, Object> map = (Map<String, Object>) obj;
                    map.put("name", map.get("index"));
                    map.remove("index");
                }
                return list;
            });
    }
}
