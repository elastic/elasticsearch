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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.graph.GraphExploreRequest;
import org.elasticsearch.client.graph.GraphExploreResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;


public class GraphClient {
    private final RestHighLevelClient restHighLevelClient;

    GraphClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Executes an exploration request using the Graph API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/graph-explore-api.html">Graph API
     * on elastic.co</a>.
     */
    public final GraphExploreResponse explore(GraphExploreRequest graphExploreRequest,
                                                             RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(graphExploreRequest, GraphRequestConverters::explore,
                options, GraphExploreResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously executes an exploration request using the Graph API.
     *
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/graph-explore-api.html">Graph API
     * on elastic.co</a>.
     * @return cancellable that may be used to cancel the request
     */
    public final Cancellable exploreAsync(GraphExploreRequest graphExploreRequest,
                                          RequestOptions options,
                                          ActionListener<GraphExploreResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(graphExploreRequest, GraphRequestConverters::explore,
            options, GraphExploreResponse::fromXContent, listener, emptySet());
    }

}
