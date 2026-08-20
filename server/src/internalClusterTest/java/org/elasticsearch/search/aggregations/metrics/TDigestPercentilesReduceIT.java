/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.closeTo;

/**
 * Coordinator-side reduction of tdigest percentiles, over a shard layout that mixes partially reduced
 * results with shard results the coordinator never received over the wire.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TDigestPercentilesReduceIT extends ESIntegTestCase {

    /**
     * A shard result that stays on the coordinating node is never serialized, so its state still holds the
     * aggregation context's {@code PreallocatedCircuitBreaker}, which is closed once the shard is done. Reduction
     * used to accumulate into whichever state had the larger compression, so such a result could become the
     * accumulator and charge that closed breaker as it grew.
     * <p>
     * The layout below fixes the order the reducer sees. Partially reduced results are consumed ahead of raw shard
     * results, and the two empty shards sit alone on a node, so that node reduces them into a partial carrying an
     * empty state, whose compression is a hard-coded 1.0 and therefore always below the request's. The raw results
     * follow in shard index order, which orders these single-shard indices by name: the coordinator's own result
     * used to be adopted as the accumulator, and merging the remaining, wire-received result into it grew it.
     */
    public void testReduceOverEmptyPartialAndCoordinatorLocalShardResult() throws Exception {
        List<String> nodes = internalCluster().startNodes(3);
        String coordinator = nodes.get(0);

        createPinnedIndex("a_local", 1, coordinator);
        createPinnedIndex("m_empty", 2, nodes.get(2));
        createPinnedIndex("z_remote", 1, nodes.get(1));

        // Each data shard holds fewer values than the HybridDigest threshold at which it switches from sorting to
        // merging (20 * the default compression of 100), so that the switch, and the allocation it needs, happens
        // while the two shard results are merged into each other during reduction.
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            docs.add(prepareIndex("a_local").setSource("value", i));
            docs.add(prepareIndex("z_remote").setSource("value", i));
        }
        indexRandom(true, docs);
        ensureGreen("a_local", "m_empty", "z_remote");

        assertResponse(
            internalCluster().client(coordinator)
                .prepareSearch("a_local", "m_empty", "z_remote")
                .setSize(0)
                .addAggregation(percentiles("percentiles").field("value").percentiles(50.0)),
            response -> {
                Percentiles result = response.getAggregations().get("percentiles");
                assertThat(result.percentile(50.0), closeTo(749.5, 30.0));
            }
        );
    }

    private void createPinnedIndex(String name, int shards, String node) {
        assertAcked(
            prepareCreate(name).setSettings(indexSettings(shards, 0).put("index.routing.allocation.require._name", node))
                .setMapping("value", "type=long")
        );
    }
}
