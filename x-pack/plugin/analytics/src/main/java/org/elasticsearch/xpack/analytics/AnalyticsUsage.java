/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks usage of the Analytics aggregations.
 */
public class AnalyticsUsage {
    /**
     * Items to track.
     */
    public enum Item {
        BOXPLOT,
        CUMULATIVE_CARDINALITY,
        STRING_STATS,
        TOP_METRICS,
        T_TEST;
    }

    private final Map<Item, AtomicLong> trackers = new EnumMap<>(Item.class);

    public AnalyticsUsage() {
        for (Item item: Item.values()) {
            trackers.put(item, new AtomicLong(0));
        }
    }

    /**
     * Track successful parsing.
     */
    public <C, T> ContextParser<C, T> track(Item item, ContextParser<C, T> realParser) {
        AtomicLong usage = trackers.get(item);
        return (parser, context) -> {
            T value = realParser.parse(parser, context);
            // Intentionally doesn't count unless the parser returns cleanly.
            usage.incrementAndGet();
            return value;
        };
    }

    public AnalyticsStatsAction.NodeResponse stats(DiscoveryNode node) {
        return new AnalyticsStatsAction.NodeResponse(node,
                trackers.get(Item.BOXPLOT).get(),
                trackers.get(Item.CUMULATIVE_CARDINALITY).get(),
                trackers.get(Item.STRING_STATS).get(),
                trackers.get(Item.TOP_METRICS).get(),
                trackers.get(Item.T_TEST).get());
    }
}
