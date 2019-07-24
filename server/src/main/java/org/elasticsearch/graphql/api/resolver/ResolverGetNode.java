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

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.graphql.api.GqlApiUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.elasticsearch.graphql.api.GqlApiUtils.*;

public class ResolverGetNode {

    @SuppressWarnings("unchecked")
    private static Map<String, Object> transformDocumentData(Map<String, Object> obj) {
        obj.put("transportAddress", obj.get("transport_address"));
        obj.remove("transport_address");

        obj.put("buildFlavor", obj.get("build_flavor"));
        obj.remove("build_flavor");

        obj.put("buildType", obj.get("build_type"));
        obj.remove("build_type");

        obj.put("buildHash", obj.get("build_hash"));
        obj.remove("build_hash");

        obj.put("totalIndexingBuffer", obj.get("total_indexing_buffer"));
        obj.remove("total_indexing_buffer");

        obj.put("threadPool", obj.get("thread_pool"));
        obj.remove("thread_pool");

        return obj;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractNode(Map<String, Object> obj) {
        System.out.println("2 " + obj);
        obj = (Map<String, Object>) obj.get("nodes");
        System.out.println("3 " + obj);

        String key = GqlApiUtils.getSomeMapKey(obj);
        System.out.println("4 " + key);
        return key == null
            ? null
            : (Map<String, Object>) obj.get(key);
    }

    @SuppressWarnings("unchecked")
    public static CompletableFuture<Map<String, Object>> exec(NodeClient client, String nodeIdOrName) throws Exception {
        String[] nodeIds = { nodeIdOrName };
        final NodesInfoRequest request = new NodesInfoRequest(nodeIds);
        request.all();

        CompletableFuture<NodesInfoResponse> future = new CompletableFuture<NodesInfoResponse>();
        client.admin().cluster().nodesInfo(request, futureToListener(future));

        return future
            .thenApply(GqlApiUtils::toMapSafe)
            .thenApply(ResolverGetNode::extractNode)
            .thenApply(ResolverGetNode::transformDocumentData);
    }
}
