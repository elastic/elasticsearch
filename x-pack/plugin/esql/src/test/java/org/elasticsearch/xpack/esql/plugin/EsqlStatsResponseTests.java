/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class EsqlStatsResponseTests extends ESTestCase {

    public void testQueryPathsReachNodesThatKnowThem() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(EsqlStatsResponse.QUERY_PATHS);
        Counters copied = roundTrip(nodeStats(), version).getStats();
        assertThat(copied.toNestedMap(), equalTo(nodeStats().getStats().toNestedMap()));
    }

    public void testQueryPathsAreLeftOutForOlderNodes() throws IOException {
        TransportVersion version = TransportVersionUtils.getPreviousVersion(EsqlStatsResponse.QUERY_PATHS);
        Counters copied = roundTrip(nodeStats(), version).getStats();
        assertThat(copied.toNestedMap(), equalTo(Map.of("queries", Map.of("rest", Map.of("total", 2L)), "features", Map.of("where", 1L))));
    }

    private static EsqlStatsResponse.NodeStatsResponse nodeStats() {
        Counters counters = new Counters();
        counters.inc("queries.rest.total", 2);
        counters.inc("features.where", 1);
        counters.inc("query_paths.rest.other.from.total", 2);
        counters.inc("query_paths.rest.other.from.commands.where", 1);
        EsqlStatsResponse.NodeStatsResponse response = new EsqlStatsResponse.NodeStatsResponse(DiscoveryNodeUtils.create("node"));
        response.setStats(counters);
        return response;
    }

    private static EsqlStatsResponse.NodeStatsResponse roundTrip(EsqlStatsResponse.NodeStatsResponse response, TransportVersion version)
        throws IOException {
        return copyWriteable(response, new NamedWriteableRegistry(List.of()), EsqlStatsResponse.NodeStatsResponse::new, version);
    }
}
