/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.graph.action;

import org.elasticsearch.action.Action;
<<<<<<< HEAD
import org.elasticsearch.client.ElasticsearchClient;
=======
import org.elasticsearch.protocol.xpack.graph.GraphExploreResponse;
>>>>>>> 38bdf9ce323... HLRC GraphClient and associated tests (#32366)

public class GraphExploreAction extends Action<GraphExploreRequest, GraphExploreResponse, 
    GraphExploreRequestBuilder> {

    public static final GraphExploreAction INSTANCE = new GraphExploreAction();
    public static final String NAME = "indices:data/read/xpack/graph/explore";

    private GraphExploreAction() {
        super(NAME);
    }

    @Override
    public GraphExploreResponse newResponse() {
        return new GraphExploreResponse();
    }

    @Override
    public GraphExploreRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GraphExploreRequestBuilder(client, this);
    }
}
