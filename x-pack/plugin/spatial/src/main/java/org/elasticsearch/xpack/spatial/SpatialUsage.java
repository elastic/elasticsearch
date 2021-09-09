/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.xpack.core.common.stats.EnumCounters;
import org.elasticsearch.xpack.core.spatial.action.SpatialStatsAction;

/**
 * Tracks usage of the Spatial aggregations.
 */
public class SpatialUsage {

    private final EnumCounters<SpatialStatsAction.Item> counters = new EnumCounters<>(SpatialStatsAction.Item.class);

    public SpatialUsage() {
    }

    /**
     * Track successful parsing.
     */
    public <C, T> ContextParser<C, T> track(SpatialStatsAction.Item item, ContextParser<C, T> realParser) {
        return (parser, context) -> {
            T value = realParser.parse(parser, context);
            // Intentionally doesn't count unless the parser returns cleanly.
            counters.inc(item);
            return value;
        };
    }

    public SpatialStatsAction.NodeResponse stats(DiscoveryNode node) {
        return new SpatialStatsAction.NodeResponse(node, counters);
    }
}
