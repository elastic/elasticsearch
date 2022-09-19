/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analytics;

import org.elasticsearch.action.admin.cluster.stats.AnalyticsStatsAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.EnumCounters;
import org.elasticsearch.xcontent.ContextParser;

/**
 * Tracks usage of the Analytics aggregations.
 */
public class AnalyticsUsage {

    private final EnumCounters<AnalyticsStatsAction.Item> counters = new EnumCounters<>(AnalyticsStatsAction.Item.class);

    public AnalyticsUsage() {}

    /**
     * Track successful parsing.
     */
    public <C, T> ContextParser<C, T> track(AnalyticsStatsAction.Item item, ContextParser<C, T> realParser) {
        return (parser, context) -> {
            T value = realParser.parse(parser, context);
            // Intentionally doesn't count unless the parser returns cleanly.
            counters.inc(item);
            return value;
        };
    }

    public AnalyticsStatsAction.NodeResponse stats(DiscoveryNode node) {
        return new AnalyticsStatsAction.NodeResponse(node, counters);
    }
}
