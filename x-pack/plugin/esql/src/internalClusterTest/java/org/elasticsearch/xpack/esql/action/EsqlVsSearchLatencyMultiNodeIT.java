/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;

/**
 * {@link EsqlVsSearchLatencyIT} with the shards spread over three data nodes and the queries issued from a fourth node
 * that holds none of them, so every shard is remote to the coordinator and each query pays the full set of transport
 * hops. {@code -Dtests.latency.coordinator=data} issues the queries from a node that holds a shard instead, which is
 * what a benchmark client that round-robins over the data nodes sees. Shards get several segments by default.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 4, numClientNodes = 0, supportsDedicatedMasters = false)
public class EsqlVsSearchLatencyMultiNodeIT extends EsqlVsSearchLatencyIT {

    private static final boolean COORDINATOR_HOLDS_SHARDS = "data".equals(System.getProperty("tests.latency.coordinator", "dedicated"));
    private static final int SHARD_NODES = 3;

    @Override
    protected String coordinatorNodeName() {
        return sortedNodeNames()[0];
    }

    @Override
    protected Settings.Builder indexSettings(Settings.Builder settings) {
        String[] names = sortedNodeNames();
        // Keep one node free of shards: the coordinator itself, or another node when the coordinator should hold a shard.
        String excluded = COORDINATOR_HOLDS_SHARDS ? names[1] : names[0];
        return settings.put("index.routing.allocation.exclude._name", excluded)
            .put("index.routing.allocation.total_shards_per_node", Math.ceilDiv(NUM_SHARDS, SHARD_NODES));
    }

    @Override
    protected int defaultSegments() {
        return 8;
    }

    private String[] sortedNodeNames() {
        String[] names = internalCluster().getNodeNames();
        Arrays.sort(names);
        return names;
    }
}
