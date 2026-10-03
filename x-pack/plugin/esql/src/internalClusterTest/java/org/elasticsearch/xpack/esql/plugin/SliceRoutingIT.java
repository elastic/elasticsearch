/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;

/**
 * Verifies that an ES|QL {@code WHERE _slice == ...} predicate on a slice-enabled index is turned into a routing value so the
 * coordinator only dispatches data-node requests to the shards that can hold the requested slice, while still returning the
 * correct rows.
 */
public class SliceRoutingIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "slice-routing-test";
    private static final int NUM_SHARDS = 3;
    private static final int NUM_SLICES = 30;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    public void testSlicePredicatePrunesShards() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexSettings.SLICE_ENABLED.getKey(), true)
                )
                .setMapping("v", "type=long")
        );

        // Spread documents across many distinct slice values so, with NUM_SLICES >> NUM_SHARDS, every shard receives data. The
        // number of docs in the chosen slice "s0" is tracked so we can assert the filtered result exactly.
        int expectedS0Docs = 0;
        int totalDocs = 0;
        BulkRequestBuilder bulk = client().prepareBulk(INDEX).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int slice = 0; slice < NUM_SLICES; slice++) {
            String sliceId = "s" + slice;
            int docs = between(1, 4);
            for (int d = 0; d < docs; d++) {
                bulk.add(new IndexRequest(INDEX).source("v", totalDocs).routing(sliceId).setRoutingFromSlice(true));
                totalDocs++;
            }
            if (slice == 0) {
                expectedS0Docs = docs;
            }
        }
        var bulkResponse = bulk.get();
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());

        Set<ShardId> allQueriedShards = ConcurrentCollections.newConcurrentSet();
        Set<ShardId> sliceQueriedShards = ConcurrentCollections.newConcurrentSet();
        try {
            // Baseline: no _slice filter, so every shard that holds data is queried.
            captureQueriedShards(allQueriedShards);
            try (EsqlQueryResponse resp = run("FROM " + INDEX + " | STATS c = COUNT(*)")) {
                assertThat(getValuesList(resp).get(0).get(0), equalTo((long) totalDocs));
            }
            cleanAllTransportRules();
            // With NUM_SLICES distinct routing values over NUM_SHARDS shards, all shards receive data with overwhelming probability.
            assertThat(allQueriedShards.size(), greaterThanOrEqualTo(2));

            // With a _slice equality, routing prunes down to the single shard that "s0" hashes to.
            captureQueriedShards(sliceQueriedShards);
            try (EsqlQueryResponse resp = run("FROM " + INDEX + " METADATA _slice | WHERE _slice == \"s0\" | STATS c = COUNT(*)")) {
                assertThat(getValuesList(resp).get(0).get(0), equalTo((long) expectedS0Docs));
            }
        } finally {
            cleanAllTransportRules();
        }

        // A single routing value always maps to exactly one shard, so only that shard should have been queried.
        assertThat(sliceQueriedShards, hasSize(1));
        assertThat(sliceQueriedShards, everyItem(in(allQueriedShards)));
    }

    private static void captureQueriedShards(Set<ShardId> queriedShards) {
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            as(transportService, MockTransportService.class).addRequestHandlingBehavior(
                ComputeService.DATA_ACTION_NAME,
                (handler, request, channel, task) -> {
                    DataNodeRequest dataNodeRequest = (DataNodeRequest) request;
                    for (DataNodeRequest.Shard shard : dataNodeRequest.shards()) {
                        queriedShards.add(shard.shardId());
                    }
                    handler.messageReceived(request, channel, task);
                }
            );
        }
    }

    private static void cleanAllTransportRules() {
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            as(transportService, MockTransportService.class).clearAllRules();
        }
    }
}
