/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class SearchTransportAPMMetrics {
    public static final String SEARCH_ACTION_LATENCY_BASE_METRIC = "es.search.nodes.transport_actions.latency.histogram";
    public static final String ACTION_ATTRIBUTE_NAME = "action";

    public static final String QUERY_CAN_MATCH_NODE_METRIC = "shards_can_match";
    public static final String DFS_ACTION_METRIC = "dfs_query_then_fetch/shard_dfs_phase";
    public static final String QUERY_ID_ACTION_METRIC = "dfs_query_then_fetch/shard_query_phase";
    public static final String QUERY_ACTION_METRIC = "query_then_fetch/shard_query_phase";
    public static final String FREE_CONTEXT_ACTION_METRIC = "shard_release_context";
    public static final String FETCH_ID_ACTION_METRIC = "shard_fetch_phase";
    public static final String QUERY_SCROLL_ACTION_METRIC = "scroll/shard_query_phase";
    public static final String FETCH_ID_SCROLL_ACTION_METRIC = "scroll/shard_fetch_phase";
    public static final String QUERY_FETCH_SCROLL_ACTION_METRIC = "scroll/shard_query_and_fetch_phase";
    public static final String FREE_CONTEXT_SCROLL_ACTION_METRIC = "scroll/shard_release_context";
    public static final String CLEAR_SCROLL_CONTEXTS_ACTION_METRIC = "scroll/shard_release_contexts";

    private final LongHistogram actionLatencies;

    public SearchTransportAPMMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongHistogram(
                SEARCH_ACTION_LATENCY_BASE_METRIC,
                "Transport action execution times at the node level, expressed as a histogram",
                "millis"
            )
        );
    }

    private SearchTransportAPMMetrics(LongHistogram actionLatencies) {
        this.actionLatencies = actionLatencies;
    }

    public LongHistogram getActionLatencies() {
        return actionLatencies;
    }
}
