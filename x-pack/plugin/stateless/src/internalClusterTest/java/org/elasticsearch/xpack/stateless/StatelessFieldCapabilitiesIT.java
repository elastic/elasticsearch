/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Rebalance;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.Map;

import static org.elasticsearch.cluster.InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED;
import static org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.SETTING_HOLLOW_INGESTION_TTL;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.hamcrest.Matchers.equalTo;

public class StatelessFieldCapabilitiesIT extends AbstractStatelessPluginIntegTestCase {

    private final String mapping = """
        {
            "_doc": {
                "properties": {
                    "name": {
                        "type": "text"
                    },
                    "route_length": {
                        "type": "double"
                    }
                }
            }
        }""";

    public void testFieldCapsAreExecutedOnSearchNodes() throws Exception {
        startMasterAndIndexNode();

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setMapping(mapping).setSettings(indexSettings(1, 0)));
        ensureGreen(indexName);

        expectThrows(
            NoShardAvailableActionException.class,
            () -> client().prepareFieldCaps(indexName).setFields("name", "route_length").get()
        );

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        startSearchNode();
        ensureGreen(indexName);

        assertFieldCaps(indexName);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // disable rebalancing to avoid spurious shard movements that might cause premature hollowing
            .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE)
            // disable index balance decider to avoid spurious shard movements that might cause premature hollowing
            .put(INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false)
            // disable estimated heap decider to avoid spurious shard movements that might cause premature hollowing
            .put(CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), false);
    }

    public void testFieldCapsForHollowShards() throws Exception {
        startMasterOnlyNode();
        var indexNodeSettings = Settings.builder()
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), true)
            .put(SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1))
            .build();
        var indexNodeA = startIndexNode(indexNodeSettings);
        startSearchNode();
        ensureStableCluster(3);

        var indexName = randomIndexName();
        int numberOfShards = randomIntBetween(1, 5);
        assertAcked(prepareCreate(indexName).setMapping(mapping).setSettings(indexSettings(numberOfShards, 1)));
        ensureGreen(indexName);
        insertDocs(indexName);
        flush(indexName);

        assertFieldCaps(indexName);

        String indexNodeB = startIndexNode(indexNodeSettings);
        hollowShards(indexName, numberOfShards, indexNodeA, indexNodeB);

        assertFieldCaps(indexName);
    }

    private static void insertDocs(String indexName) {
        int numDocs = randomIntBetween(64, 256);
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(indexName).source(
                    Map.of("name", randomAlphaOfLength(8), "route_length", randomDoubleBetween(0.0, 100.0, false))
                )
            );
        }
        safeGet(bulkRequest.execute());
    }

    private static void assertFieldCaps(String indexName) {
        var response = safeGet(client().prepareFieldCaps(indexName).setFields("name", "route_length").execute());
        assertThat(response.getIndices(), equalTo(new String[] { indexName }));
        assertThat(
            response.getField("name").get("text"),
            equalTo(new FieldCapabilitiesBuilder("name", "text").isAggregatable(false).build())
        );
        assertThat(
            response.getField("route_length").get("double"),
            equalTo(new FieldCapabilitiesBuilder("route_length", "double").build())
        );
    }
}
