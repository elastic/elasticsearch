/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.example.resthandler;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class ExampleRestHandlerPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<RestHandler> getRestHandlers(final RestHandlersServices restHandlersServices,
                                             final Supplier<DiscoveryNodes> nodesInCluster,
                                             final Predicate<NodeFeature> clusterSupportsFeature) {
        return singletonList(new ExampleCatAction());
    }
}
