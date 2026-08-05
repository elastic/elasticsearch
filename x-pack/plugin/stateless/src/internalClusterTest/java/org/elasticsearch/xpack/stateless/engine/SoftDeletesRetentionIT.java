/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertShardsHaveSeqNoDocValues;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.stateless.StatelessMergeIT.blockMergePool;
import static org.elasticsearch.xpack.stateless.commits.HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that in stateless mode, the soft deletes retention policy ignores retention leases
 * and advances the min retained sequence number based solely on the global checkpoint.
 * This is because {@link IndexEngine#shouldRetainForPeerRecovery()} returns {@code false}.
 */
public class SoftDeletesRetentionIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // Disable background flushes
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Disable hollowing as the test force merges the shard directly without a client to unhollow
            .put(STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), Boolean.FALSE)
            // Test blockes merges
            .put(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true);
    }

    public void testRetentionLeasesIgnoredForSoftDeletesPolicy() throws Exception {
        final boolean disableSeqNos = randomBoolean();
        startMasterOnlyNode();
        final var indexNode = startIndexNode();

        final var indexName = randomIdentifier();
        final var indexSettings = indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
            .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(100L))
            .put("index.merge.policy.segments_per_tier", 1000);

        if (disableSeqNos) {
            indexSettings.put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true);
            indexSettings.put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY);
        }
        createIndex(indexName, indexSettings.build());
        ensureGreen(indexName);

        // Block the merge thread pool to avoid concurrent merges to be executed before the global checkpoint has caught up
        final var unblockMerges = new CountDownLatch(1);
        blockMergePool(internalCluster().getInstance(ThreadPool.class, indexNode), unblockMerges);

        final int numBatches = randomIntBetween(5, 10);
        final int numDocsPerBatch = randomIntBetween(10, 100);

        final var docsIds = new HashSet<String>();
        for (int batch = 0; batch < numBatches; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < numDocsPerBatch; doc++) {
                var docId = String.format(Locale.ROOT, "%d-%d", batch, doc);
                bulk.add(prepareIndex(indexName).setId(docId).setSource("field", "value-" + doc));
            }
            var bulkResponse = bulk.get();
            assertNoFailures(bulkResponse);

            for (var result : bulkResponse.getItems()) {
                assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
                assertThat(result.getVersion(), equalTo(1L));
                docsIds.add(result.getId());
            }
        }

        // Add a retention lease at seq no 0: in standard mode this would retain ALL soft-deleted docs
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var retentionLeaseId = randomIdentifier();
        client().execute(RetentionLeaseActions.ADD, new RetentionLeaseActions.AddRequest(shardId, retentionLeaseId, 0L, "test"))
            .actionGet();

        final long numDocs = docsIds.size();

        final var indexShard = findIndexShard(indexName);
        assertBusy(() -> {
            var seqNoStats = indexShard.seqNoStats();
            assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(numDocs - 1L));

            var leases = indexShard.getRetentionLeases().leases();
            assertTrue(leases.stream().anyMatch(l -> l.id().equals(retentionLeaseId) && l.retainingSequenceNumber() == 0L));
        });

        // Update random docs to create soft-deleted versions of the originals.
        // We leave at least one doc behind, so the merge still has a target to hit.
        var randomDocs = randomSubsetOf(randomIntBetween(1, docsIds.size() - 1), docsIds);
        var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (var randomDoc : randomDocs) {
            bulk.add(prepareIndex(indexName).setId(randomDoc).setSource("field", "updated-" + randomDoc));
        }
        var bulkResponse = bulk.get();
        assertNoFailures(bulkResponse);

        for (var result : bulkResponse.getItems()) {
            assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));
            assertThat(result.getVersion(), equalTo(2L));
            docsIds.add(result.getId());
        }

        // Waits for the global checkpoint to catch up with max. sequence number
        final long expectedMinRetainedSeqNo = docsIds.size() + randomDocs.size();
        final long expectedGlobalCheckpoint = expectedMinRetainedSeqNo - 1L;
        assertBusy(() -> {
            indexShard.sync();

            assertThat(indexShard.seqNoStats().getGlobalCheckpoint(), equalTo(expectedGlobalCheckpoint));
            assertThat(indexShard.getLastKnownGlobalCheckpoint(), equalTo(expectedGlobalCheckpoint));
            assertThat(indexShard.getLastSyncedGlobalCheckpoint(), equalTo(expectedGlobalCheckpoint));
            assertThat(indexShard.getMinRetainedSeqNo(), equalTo(expectedMinRetainedSeqNo));
        });

        // Kick off a merge and releases the thread pool so that merges use the latest minRetainedSeqNo
        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setFlush(true).setMaxNumSegments(1).execute();
        unblockMerges.countDown();

        var forceMergeResponse = safeGet(forceMerge);
        assertThat(forceMergeResponse.getFailedShards(), equalTo(0));
        refresh(indexName);

        var stats = indicesAdmin().prepareStats(indexName).clear().setDocs(true).setSegments(true).get();
        assertThat("soft-deleted docs should be purged despite retention lease", stats.getPrimaries().getDocs().getDeleted(), equalTo(0L));
        assertThat(stats.getPrimaries().getDocs().getCount(), equalTo(numDocs));
        assertThat(stats.getPrimaries().getSegments().getCount(), equalTo(1L));

        if (disableSeqNos) {
            assertShardsHaveSeqNoDocValues(internalCluster(), indexName, false, 1);
        }

        // Verify the retention lease is still present (it wasn't removed, just ignored by the policy)
        boolean found = false;
        for (RetentionLease lease : findIndexShard(indexName).getRetentionLeases().leases()) {
            if (lease.id().equals(retentionLeaseId)) {
                assertThat(lease.retainingSequenceNumber(), equalTo(0L));
                found = true;
            }
        }
        assertTrue("custom retention lease should still be present", found);
    }
}
