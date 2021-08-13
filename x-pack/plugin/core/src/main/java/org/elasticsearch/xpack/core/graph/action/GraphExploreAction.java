/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.graph.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.protocol.xpack.graph.GraphExploreResponse;

public class GraphExploreAction extends ActionType<GraphExploreResponse> {

    public static final GraphExploreAction INSTANCE = new GraphExploreAction();
    public static final String NAME = "indices:data/read/xpack/graph/explore";

    private GraphExploreAction() {
        super(NAME, GraphExploreResponse::new);
    }
}
