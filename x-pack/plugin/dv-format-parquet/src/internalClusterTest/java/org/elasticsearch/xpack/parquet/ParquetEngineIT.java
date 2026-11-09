/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * Engine spike: which engine runs a shard whose only segment is a stub that references Parquet files, and does the stub survive the
 * lifecycle of a shard (primary recovery, a replica, a replica promoted to primary, a full restart)?
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetEngineIT extends ESIntegTestCase {

    private static final int ROWS = ParquetMounts.ROWS;

    @Override
    protected boolean addMockInternalEngine() {
        // only one engine factory may apply to an index, and ours must be the one
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateParquetDocValues.class);
    }

    private void createParquetIndex(int replicas) throws Exception {
        ParquetMounts.writeFileAndRegisterRepository(this);
        assertAcked(
            prepareCreate("events").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", replicas)
                    .put(ParquetMounts.mountSettings().build())
            ).setMapping(ParquetMounts.MAPPING)
        );
    }

    private void assertServesTheStub() {
        assertResponse(
            prepareSearch("events").setSize(0)
                .setTrackTotalHits(true)
                .addAggregation(AggregationBuilders.max("max_status").field("status"))
                .addAggregation(AggregationBuilders.max("max_ts").field("ts"))
                .addAggregation(AggregationBuilders.sum("sum_ts").field("ts")),
            response -> {
                assertEquals(ROWS, response.getHits().getTotalHits().value());
                assertEquals(ROWS - 1, ((Max) response.getAggregations().get("max_status")).value(), 0.0);
                // the values come from the Parquet file: ts is 1000 times the row number
                assertEquals((ROWS - 1) * 1000.0, ((Max) response.getAggregations().get("max_ts")).value(), 0.0);
                assertEquals(1000.0 * ROWS * (ROWS - 1) / 2, ((Sum) response.getAggregations().get("sum_ts")).value(), 0.0);
            }
        );
    }

    public void testPrimaryAndReplicaServeTheStub() throws Exception {
        internalCluster().startNodes(2);
        createParquetIndex(1);
        ensureGreen("events");
        assertServesTheStub();

        // GET goes through the engine, which resolves the _id as the row number because the stub has no postings
        GetResponse get = client().prepareGet("events", "5").get();
        logger.info("--> GET of row 5: exists=[{}] source=[{}]", get.isExists(), get.getSourceAsString());
        assertTrue(get.isExists());
        assertFalse(client().prepareGet("events", Integer.toString(ROWS)).get().isExists());
        assertFalse(client().prepareGet("events", "not-a-row").get().isExists());

        // the replica copied the stub through peer recovery: stop the primary and let the replica take over
        ClusterState state = clusterService().state();
        String primaryNode = state.nodes().get(state.routingTable().index("events").shard(0).primaryShard().currentNodeId()).getName();
        internalCluster().stopNode(primaryNode);
        ensureYellow("events");
        assertServesTheStub();
    }

    public void testFullRestartReopensTheStubWithoutAttachingAgain() throws Exception {
        internalCluster().startNode();
        createParquetIndex(0);
        ensureGreen("events");
        assertServesTheStub();
        internalCluster().fullRestart();
        ensureGreen("events");
        assertServesTheStub();
    }
}
