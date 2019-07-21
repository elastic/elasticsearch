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
package org.elasticsearch.graphql.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.graphql.gql.GqlServer;
import org.elasticsearch.rest.*;

import java.util.HashMap;
import java.util.Map;

public class GraphqlRestHandler implements RestHandler {
    GqlServer gqlServer;

    public GraphqlRestHandler(GqlServer gqlServer) {
        this.gqlServer = gqlServer;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        Map<String, Object> body = request.contentParser().map();

        if (!(body.get("query") instanceof String)) throw new Exception("GraphQL request must have a query.");
        String query = (String) body.get("query");

//        String operationName = payload.get("operationName") instanceof String
//            ? (String) payload.get("operationName") : "";
//        LinkedTreeMap variables = payload.get("variables") instanceof LinkedTreeMap
//            ? (LinkedTreeMap) payload.get("variables") : null;

//        Map<String, Object> spec = graphqlServer.executeToSpecification(query, operationName, variables, ctx);

        Map<String, Object> res = gqlServer.executeToSpecification(query, "", new HashMap<>(), client);
        System.out.println("GraphQL result:");
        System.out.println(res);

        RestResponse response = new RestResponse() {
            @Override
            public String contentType() {
                return "application/json";
            }

            @Override
            public BytesReference content() {
                try {
                    return BytesReference.bytes(channel.newBuilder().map(res, false));
                } catch (Exception error) {
                    return null;
                }
            }

            @Override
            public RestStatus status() {
                return RestStatus.OK;
            }
        };
        response.addHeader("X-GraphQL", "yup!");
        channel.sendResponse(response);
    }
}
