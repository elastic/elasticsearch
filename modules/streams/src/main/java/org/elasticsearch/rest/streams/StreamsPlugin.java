/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.streams.logs.LogsStreamsActivationToggleAction;
import org.elasticsearch.rest.streams.logs.RestSetLogStreamsEnabledAction;
import org.elasticsearch.rest.streams.logs.RestStreamsStatusAction;
import org.elasticsearch.rest.streams.logs.StreamsStatusAction;
import org.elasticsearch.rest.streams.logs.TransportLogsStreamsToggleActivation;
import org.elasticsearch.rest.streams.logs.TransportStreamsStatusAction;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This plugin provides the Streams feature which builds upon data streams to
 * provide the user with a more "batteries included" experience for ingesting large
 * streams of data, such as logs.
 */
public class StreamsPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestSetLogStreamsEnabledAction(), new RestStreamsStatusAction());
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(LogsStreamsActivationToggleAction.INSTANCE, TransportLogsStreamsToggleActivation.class),
            new ActionHandler(StreamsStatusAction.INSTANCE, TransportStreamsStatusAction.class)
        );
    }
}
