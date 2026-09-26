/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Registers the internal kNN evaluation REST and transport actions. */
public class KnnEvalPlugin extends Plugin implements ActionPlugin {

    static final ActionType<KnnEvalResponse> KNN_EVAL_ACTION = new ActionType<>("indices:data/read/knn_eval");

    @Override
    public List<ActionHandler> getActions() {
        return List.of(new ActionHandler(KNN_EVAL_ACTION, TransportKnnEvalAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestKnnEvalAction());
    }
}
