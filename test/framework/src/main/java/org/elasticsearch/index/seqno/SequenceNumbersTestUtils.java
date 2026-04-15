/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.seqno;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;

import static org.elasticsearch.test.ESIntegTestCase.internalCluster;
import static org.elasticsearch.test.ESTestCase.safeGet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Utilities for asserting on sequence number fields at the Lucene level in integration tests.
 */
public final class SequenceNumbersTestUtils {

    private SequenceNumbersTestUtils() {}

    /**
     * Asserts that all shards of the given index either have or lack {@code _seq_no} doc values on disk.
     * Uses the default {@link ESIntegTestCase#internalCluster()}.
     *
     * @param indexName               the index to check
     * @param expectDocValuesOnDisk   {@code true} to assert doc values are present, {@code false} to assert they are empty
     * @param expectedShards          the exact number of shards expected to be verified
     */
    public static void assertShardsHaveSeqNoDocValues(String indexName, boolean expectDocValuesOnDisk, int expectedShards) {
        assertShardsHaveSeqNoDocValues(internalCluster(), indexName, expectDocValuesOnDisk, expectedShards);
    }

    /**
     * Asserts that all shards of the given index on the given cluster either have or lack {@code _seq_no} doc values on disk.
     *
     * @param cluster                 the cluster to check
     * @param indexName               the index to check
     * @param expectDocValuesOnDisk   {@code true} to assert doc values are present, {@code false} to assert they are empty
     * @param expectedShards          the exact number of shards expected to be verified
     */
    public static void assertShardsHaveSeqNoDocValues(
        InternalTestCluster cluster,
        String indexName,
        boolean expectDocValuesOnDisk,
        int expectedShards
    ) {
        int nbCheckedShards = 0;
        for (var indicesServices : cluster.getDataNodeInstances(IndicesService.class)) {
            for (var indexService : indicesServices) {
                if (indexService.index().getName().equals(indexName)) {
                    for (var indexShard : indexService) {
                        final var shardId = indexShard.shardId();
                        var checked = indexShard.withEngineOrNull(engine -> {
                            if (engine != null) {
                                try (var searcher = engine.acquireSearcher("assert_seq_no_dv")) {
                                    for (var leaf : searcher.getLeafContexts()) {
                                        var leafReader = leaf.reader();
                                        NumericDocValues seqNoDV = leafReader.getNumericDocValues(SeqNoFieldMapper.NAME);
                                        if (expectDocValuesOnDisk) {
                                            assertThat(shardId + " _seq_no doc values should be present", seqNoDV, notNullValue());
                                            assertThat(seqNoDV.nextDoc(), not(equalTo(DocIdSetIterator.NO_MORE_DOCS)));
                                        } else if (seqNoDV != null) {
                                            assertThat(
                                                shardId + " _seq_no doc values should be empty",
                                                seqNoDV.nextDoc(),
                                                equalTo(DocIdSetIterator.NO_MORE_DOCS)
                                            );
                                        }
                                        return true;
                                    }
                                } catch (IOException ioe) {
                                    throw new AssertionError(ioe);
                                }
                            }
                            return false;
                        });
                        if (checked) {
                            nbCheckedShards++;
                        }
                    }
                }
            }
        }
        assertThat("expected to verify " + expectedShards + " shard(s)", nbCheckedShards, equalTo(expectedShards));
    }

    /**
     * Asserts that the total number of {@code _seq_no} doc values across all shards of the given index equals the expected count.
     *
     * @param cluster         the cluster to check
     * @param indexName       the index to check
     * @param expectedCount   the expected total number of doc values per shard
     * @param expectedShards  the exact number of shards expected to be verified
     */
    public static void assertShardsSeqNoDocValuesCount(
        InternalTestCluster cluster,
        String indexName,
        long expectedCount,
        int expectedShards
    ) {
        int checked = 0;
        for (IndicesService indicesService : cluster.getDataNodeInstances(IndicesService.class)) {
            checked += doAssertShardsSeqNoDocValuesCount(indicesService, indexName, expectedCount);
        }
        assertThat("expected to verify " + expectedShards + " shard(s)", checked, equalTo(expectedShards));
    }

    /**
     * Asserts that the total number of {@code _seq_no} doc values for each shard of the given index on a specific node
     * equals the expected count.
     *
     * @param indicesService  the {@link IndicesService} instance for the node to check
     * @param indexName       the index to check
     * @param expectedCount   the expected total number of doc values per shard
     * @param expectedShards  the exact number of shards expected to be verified
     */
    public static void assertShardsSeqNoDocValuesCount(
        IndicesService indicesService,
        String indexName,
        long expectedCount,
        int expectedShards
    ) {
        int checked = doAssertShardsSeqNoDocValuesCount(indicesService, indexName, expectedCount);
        assertThat("expected to verify " + expectedShards + " shard(s)", checked, equalTo(expectedShards));
    }

    private static int doAssertShardsSeqNoDocValuesCount(IndicesService indicesService, String indexName, long expectedCount) {
        int checked = 0;
        for (var indexService : indicesService) {
            if (indexService.index().getName().equals(indexName)) {
                for (var indexShard : indexService) {
                    Long count = indexShard.withEngineOrNull(engine -> {
                        if (engine == null) {
                            return null;
                        }
                        try (var searcher = engine.acquireSearcher("assert_seq_no_count")) {
                            long total = 0;
                            for (var leaf : searcher.getLeafContexts()) {
                                NumericDocValues seqNoDV = leaf.reader().getNumericDocValues(SeqNoFieldMapper.NAME);
                                if (seqNoDV != null) {
                                    while (seqNoDV.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                        total++;
                                    }
                                }
                            }
                            return total;
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    });
                    if (count != null) {
                        assertThat("retained seq_no doc values count", count, equalTo(expectedCount));
                        checked++;
                    }
                }
            }
        }
        return checked;
    }

    /**
     * Waits until all retention leases on all shards of the given index have their retaining sequence number
     * equal to the expected value.
     *
     * @param client                    the client to use for stats requests
     * @param indexName                 the index to check
     * @param expectedRetainingSeqNo    the expected retaining sequence number for every lease
     */
    public static void assertRetentionLeasesAdvanced(Client client, String indexName, long expectedRetainingSeqNo) throws Exception {
        ESIntegTestCase.assertBusy(() -> {
            var stats = client.admin().indices().prepareStats(indexName).get();
            for (var shardStats : stats.getShards()) {
                for (RetentionLease lease : shardStats.getRetentionLeaseStats().retentionLeases().leases()) {
                    assertThat(
                        "retention lease [" + lease.id() + "] should have advanced",
                        lease.retainingSequenceNumber(),
                        equalTo(expectedRetainingSeqNo)
                    );
                }
            }
        });
    }

    /**
     * Persist the global checkpoint on all primary shards of the given index into disk.
     * This makes sure that the persisted global checkpoint on those shards will equal to the in-memory value.
     *
     * This helper method is useful if you do not use replicas in your test setup.
     *
     * Uses the default {@link ESIntegTestCase#internalCluster()}.
     */
    public static void persistGlobalCheckpointOnPrimaryShards(String index) {
        persistGlobalCheckpointOnPrimaryShards(internalCluster(), index);
    }

    /**
     * Persist the global checkpoint on all primary shards of the given index into disk.
     * This makes sure that the persisted global checkpoint on those shards will equal to the in-memory value.
     *
     * This helper method is useful if you do not use replicas in your test setup.
     *
     * @param cluster the cluster containing the index
     * @param index   the name of the index
     */
    public static void persistGlobalCheckpointOnPrimaryShards(InternalTestCluster cluster, String index) {
        final var future = new PlainActionFuture<Void>();
        try (var listeners = new RefCountingListener(future)) {
            for (String node : cluster.nodesInclude(index)) {
                for (IndexService indexService : cluster.getInstance(IndicesService.class, node)) {
                    for (IndexShard indexShard : indexService) {
                        if (indexShard.routingEntry().primary() == false) {
                            continue;
                        }
                        assertThat(
                            "Shard " + indexShard.getShardUuid() + " should be active",
                            indexShard.routingEntry().active(),
                            equalTo(true)
                        );

                        final var globalCheckpoint = indexShard.withEngine(Engine::getMaxSeqNo);
                        final var listener = listeners.acquire(
                            ignored -> assertThat(
                                "Global checkpoint not synced for shard: " + indexShard.routingEntry(),
                                indexShard.getLastSyncedGlobalCheckpoint(),
                                equalTo(globalCheckpoint)
                            )
                        );
                        indexShard.syncGlobalCheckpoint(globalCheckpoint, e -> {
                            if (e == null) {
                                listener.onResponse(null);
                            } else {
                                listener.onFailure(e);
                            }
                        });
                    }
                }
            }
        }
        safeGet(future);
    }
}
