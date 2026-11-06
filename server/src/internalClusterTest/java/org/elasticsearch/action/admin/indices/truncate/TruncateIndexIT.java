/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.truncate;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TruncateIndexIT extends ESIntegTestCase {

    private static final String INDEX = "test";

    private BroadcastResponse truncate(String... indices) {
        return client().execute(TruncateIndexAction.INSTANCE, new TruncateIndexRequest(indices)).actionGet();
    }

    private void indexDocs(int from, int to) {
        for (int i = from; i < to; i++) {
            prepareIndex(INDEX).setId("doc-" + i).setSource("field", "value-" + i).get();
        }
        refresh(INDEX);
    }

    /**
     * Asserts that every copy of every shard holds exactly the given number of documents. Counting per copy (rather than searching)
     * is what shows that a replica did not keep, or get back, documents the primary dropped.
     */
    private long docCountOnPrimaries() {
        final IndicesStatsResponse stats = indicesAdmin().prepareStats(INDEX).clear().setDocs(true).get();
        long total = 0;
        for (ShardStats shardStats : stats.getShards()) {
            if (shardStats.getShardRouting().primary()) {
                total += shardStats.getStats().getDocs().getCount();
            }
        }
        return total;
    }

    private void assertDocCountOnEveryCopy(long expected) {
        final IndicesStatsResponse stats = indicesAdmin().prepareStats(INDEX).clear().setDocs(true).get();
        assertThat("docs on primaries", docCountOnPrimaries(), equalTo(expected));
        for (ShardStats shardStats : stats.getShards()) {
            final ShardRouting routing = shardStats.getShardRouting();
            if (routing.primary() == false) {
                // each replica must match its own primary
                long primaryCount = -1;
                for (ShardStats other : stats.getShards()) {
                    if (other.getShardRouting().primary() && other.getShardRouting().shardId().equals(routing.shardId())) {
                        primaryCount = other.getStats().getDocs().getCount();
                    }
                }
                assertThat("docs on replica " + routing, shardStats.getStats().getDocs().getCount(), equalTo(primaryCount));
            }
        }
    }

    public void testTruncateDiscardsDocumentsOnAllCopies() {
        internalCluster().startNodes(2);
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(between(1, 3), 1)));
        ensureGreen(INDEX);
        indexDocs(0, 100);
        assertHitCount(prepareSearch(INDEX).setSize(0).setTrackTotalHits(true), 100);
        assertDocCountOnEveryCopy(100);

        final BroadcastResponse response = truncate(INDEX);
        assertAllSuccessful(response);

        refresh(INDEX);
        assertHitCount(prepareSearch(INDEX).setSize(0).setTrackTotalHits(true), 0);
        assertDocCountOnEveryCopy(0);

        // the index is still fully usable afterwards, and ids can be reused
        indexDocs(0, 10);
        assertHitCount(prepareSearch(INDEX).setSize(0).setTrackTotalHits(true), 10);
        assertDocCountOnEveryCopy(10);
    }

    public void testTruncateKeepsMappingsSettingsAndAliases() {
        internalCluster().startNodes(2);
        assertAcked(
            prepareCreate(INDEX).setSettings(indexSettings(1, 1).put("index.refresh_interval", "30s"))
                .setMapping("field", "type=keyword", "number", "type=long")
                .addAlias(new org.elasticsearch.action.admin.indices.alias.Alias("test-alias"))
        );
        ensureGreen(INDEX);
        indexDocs(0, 5);

        final MappingMetadata mappingBefore = clusterService().state().metadata().getProject().index(INDEX).mapping();
        assertAllSuccessful(truncate(INDEX));

        final ClusterState after = clusterService().state();
        assertThat(after.metadata().getProject().index(INDEX).mapping(), equalTo(mappingBefore));
        assertThat(after.metadata().getProject().index(INDEX).getSettings().get("index.refresh_interval"), equalTo("30s"));
        assertThat(after.metadata().getProject().index(INDEX).getAliases(), hasKey("test-alias"));
        assertDocCountOnEveryCopy(0);
    }

    public void testWildcardRequiresExplicitName() {
        internalCluster().startNodes(1);
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(1, 0)));
        indexDocs(0, 3);
        // the test cluster relaxes this safeguard by default
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        expectThrows(IllegalArgumentException.class, () -> truncate("*"));
        assertDocCountOnEveryCopy(3);
    }

    /**
     * Truncates repeatedly while writers keep indexing. Which writes survive depends on timing, but whatever does must be the same on
     * the primary and the replica; the framework's end-of-test checks (sequence numbers and doc ids across copies) also verify that.
     */
    public void testTruncateWhileIndexing() throws Exception {
        internalCluster().startNodes(2);
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(between(1, 2), 1)));
        ensureGreen(INDEX);

        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicInteger counter = new AtomicInteger();
        final Thread[] writers = new Thread[2];
        for (int t = 0; t < writers.length; t++) {
            writers[t] = new Thread(() -> {
                while (stop.get() == false) {
                    prepareIndex(INDEX).setId("doc-" + counter.incrementAndGet()).setSource("field", "value").get();
                }
            });
            writers[t].start();
        }
        try {
            for (int i = 0; i < 5; i++) {
                Thread.sleep(randomIntBetween(10, 100));
                assertAllSuccessful(truncate(INDEX));
            }
        } finally {
            stop.set(true);
            for (Thread writer : writers) {
                writer.join();
            }
        }
        refresh(INDEX);
        ensureGreen(INDEX);
        assertDocCountOnEveryCopy(docCountOnPrimaries());
    }

    /**
     * A copy that is created after a truncate gets its content from a primary whose history starts at the truncate.
     */
    public void testNewReplicaRecoversFromTruncatedPrimary() {
        internalCluster().startNodes(2);
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(1, 0)));
        ensureGreen(INDEX);
        indexDocs(0, 50);
        assertAllSuccessful(truncate(INDEX));
        indexDocs(100, 107);

        assertAcked(indicesAdmin().prepareUpdateSettings(INDEX).setSettings(Settings.builder().put("index.number_of_replicas", 1)).get());
        ensureGreen(INDEX);
        assertDocCountOnEveryCopy(7);
    }

    /**
     * A replica that is offline while the index is truncated and written to comes back holding documents the truncate discarded. It
     * has to learn about the truncate from the primary's history rather than keep them.
     */
    public void testReplicaThatMissedTruncateRecovers() throws Exception {
        internalCluster().startNodes(2);
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(1, 1)));
        ensureGreen(INDEX);
        indexDocs(0, 50);

        final ClusterState state = clusterService().state();
        final ShardRouting replica = state.routingTable().index(INDEX).shard(0).replicaShards().getFirst();
        final String replicaNode = state.nodes().get(replica.currentNodeId()).getName();

        internalCluster().restartNode(replicaNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // only the primary is available, so only one of the two copies can be truncated
                assertThat(truncate(INDEX).getSuccessfulShards(), equalTo(1));
                indexDocs(100, 105);
                return super.onNodeStopped(nodeName);
            }
        });
        ensureGreen(INDEX);
        assertDocCountOnEveryCopy(5);
    }
}
