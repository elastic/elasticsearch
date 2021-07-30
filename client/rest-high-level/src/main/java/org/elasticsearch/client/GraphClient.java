/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
