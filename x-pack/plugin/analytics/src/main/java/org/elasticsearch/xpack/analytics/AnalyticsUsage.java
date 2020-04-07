/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.Arrays;

/**
 * Tracks usage of the Analytics aggregations.
 */
public class AnalyticsUsage {

    private final Counters counters = new Counters(
        Arrays.stream(AnalyticsStatsAction.Item.values()).map(AnalyticsStatsAction.Item::statName).toArray(String[]::new));

    public AnalyticsUsage() {
    }

    /**
     * Track successful parsing.
     */
    public <C, T> ContextParser<C, T> track(AnalyticsStatsAction.Item item, ContextParser<C, T> realParser) {
        String stat = item.statName();
        return (parser, context) -> {
            T value = realParser.parse(parser, context);
            // Intentionally doesn't count unless the parser returns cleanly.
            counters.inc(stat);
            return value;
        };
    }

    public AnalyticsStatsAction.NodeResponse stats(DiscoveryNode node) {
        return new AnalyticsStatsAction.NodeResponse(node, counters);
    }
}
