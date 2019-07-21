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
package org.elasticsearch.graphql;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.graphql.api.GqlApi;
import org.elasticsearch.graphql.api.GqlElasticsearchApi;
import org.elasticsearch.graphql.gql.GqlServer;
import org.elasticsearch.graphql.rest.GraphqlRestHandler;
import org.elasticsearch.rest.RestRequest;

final public class GraphqlService {
    GqlApi api;
    ActionModule actionModule;
    GqlServer gqlServer;
    GraphqlRestHandler graphqlRestHandler;

    public GraphqlService(ActionModule actionModule) {
        api = new GqlElasticsearchApi();
        gqlServer = new GqlServer(api);
        this.actionModule = actionModule;
        graphqlRestHandler = new GraphqlRestHandler(gqlServer);

        init();
    }

    void init () {
        actionModule.getRestController().registerHandler(RestRequest.Method.POST, "/graphql", graphqlRestHandler);
    }
}
