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

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.node.NodeClient;

import java.util.concurrent.CompletableFuture;

import static org.elasticsearch.graphql.api.GqlApiUtils.*;

public class ResolverGetNode2 {

    @SuppressWarnings("unchecked")
    public static CompletableFuture<NodeInfo> exec(NodeClient client, String nodeIdOrName) throws Exception {
        String[] nodeIds = { nodeIdOrName };
        final NodesInfoRequest request = new NodesInfoRequest(nodeIds);
        request.all();

        CompletableFuture<NodesInfoResponse> future = new CompletableFuture<NodesInfoResponse>();
        client.admin().cluster().nodesInfo(request, futureToListener(future));

        return future
            .thenApply(response -> response.getNodes().get(0));
    }
}
