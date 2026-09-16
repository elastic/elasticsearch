/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicatorReader;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

/**
 * What recovery does with the tail of the translog - operations that were acknowledged but never captured in an
 * uploaded commit - when the newest commits are removed from the object store by hand, as happens when a corrupt
 * commit is rolled back during an incident.
 *
 * Batch A is committed and survives. Batch B is committed and its commit is then deleted, standing in for the
 * rolled-back commits. Batch C is acknowledged but never committed, so it lives only in the translog.
 *
 * Both tests below lose batches B and C - one hundred acknowledged operations - and both leave the cluster green.
 * They differ only in whether anything is written to the log when it happens.
 */
public class TranslogTailRecoveryIT extends AbstractStatelessPluginIntegTestCase {

    private static final int BATCH = 50;

    @Override
    public int getUploadMaxCommits() {
        // one commit per uploaded blob, so each flush is separately deletable
        return 1;
    }

    /**
     * The surviving commit's translog start file was reclaimed, but the generation holding batch C is still present and
     * still names the reclaimed one. The referenced set is therefore not contiguous, and recovery says so.
     */
    public void testRollbackOntoAReclaimedStartFileSkipsThePresentTail() throws Exception {
        final Rollback rollback = indexThreeBatchesAndStopTheNode();

        deleteCommits(rollback);

        final IndexShard recovered = recoverOnAFreshIndexNode(rollback);

        assertThat(recovered.recoveryState().getTranslog().recoveredOperations(), equalTo(0));
        assertThat(recovered.seqNoStats().getMaxSeqNo(), equalTo((long) BATCH - 1));
        assertThat(recovered.docStats().getCount(), equalTo((long) BATCH));
    }

    /**
     * Nothing at or above the start file survives, so there is no hole to find - the plan is empty before the
     * contiguity check ever runs. Recovery replays nothing, reports success, and logs not one word about it.
     */
    public void testRecoveryIsSilentWhenNoTranslogSurvivesAboveTheStartFile() throws Exception {
        final Rollback rollback = indexThreeBatchesAndStopTheNode();

        deleteCommits(rollback);
        deleteAllTranslogFiles(rollback);

        final IndexShard recovered;
        try (var mockLog = MockLog.capture(TranslogReplicatorReader.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no hole is reported",
                    TranslogReplicatorReader.class.getCanonicalName(),
                    Level.INFO,
                    "*translog recovery hit hole*"
                )
            );
            recovered = recoverOnAFreshIndexNode(rollback);
            mockLog.assertAllExpectationsMatched();
        }

        assertThat(recovered.recoveryState().getTranslog().recoveredOperations(), equalTo(0));
        assertThat(recovered.seqNoStats().getMaxSeqNo(), equalTo((long) BATCH - 1));
        assertThat(recovered.docStats().getCount(), equalTo((long) BATCH));
    }

    private record Rollback(String indexName, ShardId shardId, List<String> commitsToDelete, List<String> translogNodeIds) {}

    private Rollback indexThreeBatchesAndStopTheNode() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode();

        final String indexName = "tail";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final ShardId shardId = shard(indexNode, indexName).shardId();
        final AtomicInteger ids = new AtomicInteger();

        // batch A - committed, and this commit survives the rollback
        indexDocs(indexName, BATCH, () -> "a-" + ids.getAndIncrement());
        flush(indexName);
        final List<String> afterA = commitBlobs(getObjectStoreService(indexNode), shardId);

        // batch B - committed, but its commit is deleted below
        indexDocs(indexName, BATCH, () -> "b-" + ids.getAndIncrement());
        flush(indexName);
        final List<String> afterB = commitBlobs(getObjectStoreService(indexNode), shardId);

        // batch C - acknowledged, never committed: the translog tail
        indexDocs(indexName, BATCH, () -> "c-" + ids.getAndIncrement());

        logger.info("--- seqno stats before failure: {}", shard(indexNode, indexName).seqNoStats());
        dumpLayout(getObjectStoreService(indexNode), shardId, "before failure");

        final List<String> toDelete = afterB.stream().filter(blob -> afterA.contains(blob) == false).toList();
        assertFalse("expected batch B to have produced a new commit blob", toDelete.isEmpty());
        final List<String> translogNodeIds = List.copyOf(getObjectStoreService(indexNode).getNodesWithTranslogBlobContainers());

        internalCluster().stopNode(indexNode);
        assertBusy(() -> assertThat(internalCluster().size(), equalTo(1)));

        return new Rollback(indexName, shardId, toDelete, translogNodeIds);
    }

    private void deleteCommits(Rollback rollback) throws IOException {
        final ObjectStoreService objectStore = getCurrentMasterObjectStoreService();
        for (BlobContainer term : objectStore.getProjectBlobContainer(rollback.shardId()).children(OperationPurpose.INDICES).values()) {
            term.deleteBlobsIgnoringIfNotExists(OperationPurpose.INDICES, rollback.commitsToDelete().iterator());
        }
        logger.info("--- deleted commits (the rollback): {}", rollback.commitsToDelete());
    }

    private void deleteAllTranslogFiles(Rollback rollback) throws IOException {
        final ObjectStoreService objectStore = getCurrentMasterObjectStoreService();
        for (String ephemeralId : rollback.translogNodeIds()) {
            final BlobContainer container = objectStore.getTranslogBlobContainer(ephemeralId);
            final List<String> files = List.copyOf(container.listBlobs(OperationPurpose.TRANSLOG).keySet());
            container.deleteBlobsIgnoringIfNotExists(OperationPurpose.TRANSLOG, files.iterator());
            logger.info("--- deleted translog files for node {}: {}", ephemeralId, files);
        }
    }

    private IndexShard recoverOnAFreshIndexNode(Rollback rollback) throws Exception {
        dumpLayout(getCurrentMasterObjectStoreService(), rollback.shardId(), "after rollback, before recovery");

        // a fresh index node gets a new ephemeral id, while the surviving commit still records the old one
        final String newIndexNode = startIndexNode();
        ensureGreen(rollback.indexName());

        final IndexShard recovered = shard(newIndexNode, rollback.indexName());
        logger.info("================ RESULT ================");
        logger.info("--- seqno stats after recovery: {}", recovered.seqNoStats());
        logger.info("--- translog ops replayed: {}", recovered.recoveryState().getTranslog().recoveredOperations());
        logger.info("--- docs on the recovered shard: {}", recovered.docStats().getCount());
        logger.info("========================================");
        return recovered;
    }

    private IndexShard shard(String node, String indexName) {
        return internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(indexName)).getShard(0);
    }

    private List<String> commitBlobs(ObjectStoreService objectStore, ShardId shardId) throws IOException {
        final TreeSet<String> blobs = new TreeSet<>();
        for (BlobContainer term : objectStore.getProjectBlobContainer(shardId).children(OperationPurpose.INDICES).values()) {
            blobs.addAll(term.listBlobs(OperationPurpose.INDICES).keySet());
        }
        return List.copyOf(blobs);
    }

    private void dumpLayout(ObjectStoreService objectStore, ShardId shardId, String label) throws IOException {
        logger.info("================ OBJECT STORE LAYOUT: {} ================", label);
        final Map<String, BlobContainer> terms = new TreeMap<>(
            objectStore.getProjectBlobContainer(shardId).children(OperationPurpose.INDICES)
        );
        for (Map.Entry<String, BlobContainer> term : terms.entrySet()) {
            logger.info(
                "  commits primaryTerm={} -> {}",
                term.getKey(),
                List.copyOf(new TreeSet<>(term.getValue().listBlobs(OperationPurpose.INDICES).keySet()))
            );
        }
        for (String ephemeralId : new TreeSet<>(objectStore.getNodesWithTranslogBlobContainers())) {
            logger.info(
                "  translog node={} -> {}",
                ephemeralId,
                List.copyOf(new TreeSet<>(objectStore.getTranslogBlobContainer(ephemeralId).listBlobs(OperationPurpose.TRANSLOG).keySet()))
            );
        }
        logger.info("=========================================================");
    }
}
