/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.SnapshotMatchers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LuceneChangesSnapshotTests extends EngineTestCase {

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) // always enable soft-deletes
            .build();
    }

    public void testBasics() throws Exception {
        long fromSeqNo = randomNonNegativeLong();
        long toSeqNo = randomLongBetween(fromSeqNo, Long.MAX_VALUE);
        // Empty engine
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", fromSeqNo, toSeqNo, true, randomBoolean(), randomBoolean())) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(
                error.getMessage(),
                containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
            );
        }
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", fromSeqNo, toSeqNo, false, randomBoolean(), randomBoolean())) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }
        int numOps = between(1, 100);
        int refreshedSeqNo = -1;
        for (int i = 0; i < numOps; i++) {
            String id = Integer.toString(randomIntBetween(i, i + 5));
            ParsedDocument doc = createParsedDoc(id, null, randomBoolean());
            if (randomBoolean()) {
                engine.index(indexForDoc(doc));
            } else {
                engine.delete(new Engine.Delete(doc.id(), newUid(doc.id()), primaryTerm.get()));
            }
            if (rarely()) {
                if (randomBoolean()) {
                    engine.flush();
                } else {
                    engine.refresh("test");
                }
                refreshedSeqNo = i;
            }
        }
        if (refreshedSeqNo == -1) {
            fromSeqNo = between(0, numOps);
            toSeqNo = randomLongBetween(fromSeqNo, numOps * 2);

            Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    false,
                    randomBoolean(),
                    randomBoolean(),
                    IndexVersion.CURRENT
                )
            ) {
                searcher = null;
                assertThat(snapshot, SnapshotMatchers.size(0));
            } finally {
                IOUtils.close(searcher);
            }

            searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    true,
                    randomBoolean(),
                    randomBoolean(),
                    IndexVersion.CURRENT
                )
            ) {
                searcher = null;
                IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
                assertThat(
                    error.getMessage(),
                    containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
                );
            } finally {
                IOUtils.close(searcher);
            }
        } else {
            fromSeqNo = randomLongBetween(0, refreshedSeqNo);
            toSeqNo = randomLongBetween(refreshedSeqNo + 1, numOps * 2);
            Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    false,
                    randomBoolean(),
                    randomBoolean(),
                    IndexVersion.CURRENT
                )
            ) {
                searcher = null;
                assertThat(snapshot, SnapshotMatchers.containsSeqNoRange(fromSeqNo, refreshedSeqNo));
            } finally {
                IOUtils.close(searcher);
            }
            searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    true,
                    randomBoolean(),
                    randomBoolean(),
                    IndexVersion.CURRENT
                )
            ) {
                searcher = null;
                IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
                assertThat(
                    error.getMessage(),
                    containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
                );
            } finally {
                IOUtils.close(searcher);
            }
            toSeqNo = randomLongBetween(fromSeqNo, refreshedSeqNo);
            searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    true,
                    randomBoolean(),
                    randomBoolean(),
                    IndexVersion.CURRENT
                )
            ) {
                searcher = null;
                assertThat(snapshot, SnapshotMatchers.containsSeqNoRange(fromSeqNo, toSeqNo));
            } finally {
                IOUtils.close(searcher);
            }
        }
        // Get snapshot via engine will auto refresh
        fromSeqNo = randomLongBetween(0, numOps - 1);
        toSeqNo = randomLongBetween(fromSeqNo, numOps - 1);
        try (
            Translog.Snapshot snapshot = engine.newChangesSnapshot(
                "test",
                fromSeqNo,
                toSeqNo,
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            )
        ) {
            assertThat(snapshot, SnapshotMatchers.containsSeqNoRange(fromSeqNo, toSeqNo));
        }
    }

    /**
     * A nested document is indexed into Lucene as multiple documents. While the root document has both sequence number and primary term,
     * non-root documents don't have primary term but only sequence numbers. This test verifies that {@link LuceneChangesSnapshot}
     * correctly skip non-root documents and returns at most one operation per sequence number.
     */
    public void testSkipNonRootOfNestedDocuments() throws Exception {
        Map<Long, Long> seqNoToTerm = new HashMap<>();
        List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 100), randomBoolean(), randomBoolean(), randomBoolean());
        for (Engine.Operation op : operations) {
            if (engine.getLocalCheckpointTracker().hasProcessed(op.seqNo()) == false) {
                seqNoToTerm.put(op.seqNo(), op.primaryTerm());
            }
            applyOperation(engine, op);
            if (rarely()) {
                engine.refresh("test");
            }
            if (rarely()) {
                engine.rollTranslogGeneration();
            }
            if (rarely()) {
                engine.flush();
            }
        }
        long maxSeqNo = engine.getLocalCheckpointTracker().getMaxSeqNo();
        engine.refresh("test");
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        final boolean accessStats = randomBoolean();
        try (
            Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                searcher,
                between(1, 100),
                0,
                maxSeqNo,
                false,
                randomBoolean(),
                accessStats,
                IndexVersion.CURRENT
            )
        ) {
            if (accessStats) {
                assertThat(snapshot.totalOperations(), equalTo(seqNoToTerm.size()));
            }
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                assertThat(op.toString(), op.primaryTerm(), equalTo(seqNoToTerm.get(op.seqNo())));
            }
            assertThat(snapshot.skippedOperations(), equalTo(0));
        }
    }

    public void testUpdateAndReadChangesConcurrently() throws Exception {
        Follower[] followers = new Follower[between(1, 3)];
        CountDownLatch readyLatch = new CountDownLatch(followers.length + 1);
        AtomicBoolean isDone = new AtomicBoolean();
        for (int i = 0; i < followers.length; i++) {
            followers[i] = new Follower(engine, isDone, readyLatch);
            followers[i].start();
        }
        boolean onPrimary = randomBoolean();
        List<Engine.Operation> operations = new ArrayList<>();
        int numOps = frequently() ? scaledRandomIntBetween(1, 1500) : scaledRandomIntBetween(5000, 20_000);
        for (int i = 0; i < numOps; i++) {
            String id = Integer.toString(randomIntBetween(0, randomBoolean() ? 10 : numOps * 2));
            ParsedDocument doc = createParsedDoc(id, randomAlphaOfLengthBetween(1, 5), randomBoolean());
            final Engine.Operation op;
            if (onPrimary) {
                if (randomBoolean()) {
                    op = new Engine.Index(newUid(doc), primaryTerm.get(), doc);
                } else {
                    op = new Engine.Delete(doc.id(), newUid(doc.id()), primaryTerm.get());
                }
            } else {
                if (randomBoolean()) {
                    op = replicaIndexForDoc(doc, randomNonNegativeLong(), i, randomBoolean());
                } else {
                    op = replicaDeleteForDoc(doc.id(), randomNonNegativeLong(), i, randomNonNegativeLong());
                }
            }
            operations.add(op);
        }
        readyLatch.countDown();
        readyLatch.await();
        Randomness.shuffle(operations);
        concurrentlyApplyOps(operations, engine);
        assertThat(engine.getLocalCheckpointTracker().getProcessedCheckpoint(), equalTo(operations.size() - 1L));
        isDone.set(true);
        for (Follower follower : followers) {
            follower.join();
            IOUtils.close(follower.engine, follower.engine.store);
        }
    }

    public void testAccessStoredFieldsSequentially() throws Exception {
        try (Store store = createStore(); Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            int smallBatch = between(5, 9);
            long seqNo = 0;
            for (int i = 0; i < smallBatch; i++) {
                engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(seqNo), null), 1, seqNo, true));
                seqNo++;
            }
            engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(1000), null), 1, 1000, true));
            seqNo = 11;
            int largeBatch = between(15, 100);
            for (int i = 0; i < largeBatch; i++) {
                engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(seqNo), null), 1, seqNo, true));
                seqNo++;
            }
            // disable optimization for a small batch
            Translog.Operation op;
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    0L,
                    between(1, smallBatch),
                    false,
                    randomBoolean(),
                    randomBoolean()
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
            // disable optimization for non-sequential accesses
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    between(1, 3),
                    between(20, 100),
                    false,
                    randomBoolean(),
                    randomBoolean()
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
            // enable optimization for sequential access of 10+ docs
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    11,
                    between(21, 100),
                    false,
                    true,
                    randomBoolean()
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertTrue(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertTrue(snapshot.useSequentialStoredFieldsReader());
            }
            // disable optimization if snapshot is accessed by multiple consumers
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    11,
                    between(21, 100),
                    false,
                    false,
                    randomBoolean()
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
        }
    }

    class Follower extends Thread {
        private final InternalEngine leader;
        private final InternalEngine engine;
        private final TranslogHandler translogHandler;
        private final AtomicBoolean isDone;
        private final CountDownLatch readLatch;

        Follower(InternalEngine leader, AtomicBoolean isDone, CountDownLatch readLatch) throws IOException {
            this.leader = leader;
            this.isDone = isDone;
            this.readLatch = readLatch;
            this.translogHandler = new TranslogHandler(
                xContentRegistry(),
                IndexSettingsModule.newIndexSettings(shardId.getIndexName(), leader.engineConfig.getIndexSettings().getSettings())
            );
            this.engine = createEngine(createStore(), createTempDir());
        }

        void pullOperations(InternalEngine follower) throws IOException {
            long leaderCheckpoint = leader.getLocalCheckpointTracker().getProcessedCheckpoint();
            long followerCheckpoint = follower.getLocalCheckpointTracker().getProcessedCheckpoint();
            if (followerCheckpoint < leaderCheckpoint) {
                long fromSeqNo = followerCheckpoint + 1;
                long batchSize = randomLongBetween(0, 100);
                long toSeqNo = Math.min(fromSeqNo + batchSize, leaderCheckpoint);
                try (
                    Translog.Snapshot snapshot = leader.newChangesSnapshot(
                        "test",
                        fromSeqNo,
                        toSeqNo,
                        true,
                        randomBoolean(),
                        randomBoolean()
                    )
                ) {
                    translogHandler.run(follower, snapshot);
                }
            }
        }

        @Override
        public void run() {
            try {
                readLatch.countDown();
                readLatch.await();
                while (isDone.get() == false
                    || engine.getLocalCheckpointTracker().getProcessedCheckpoint() < leader.getLocalCheckpointTracker()
                        .getProcessedCheckpoint()) {
                    pullOperations(engine);
                }
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
                // have to verify without source since we are randomly testing without _source
                List<DocIdSeqNoAndSource> docsWithoutSourceOnFollower = getDocIds(engine, true).stream()
                    .map(d -> new DocIdSeqNoAndSource(d.id(), null, d.seqNo(), d.primaryTerm(), d.version()))
                    .toList();
                List<DocIdSeqNoAndSource> docsWithoutSourceOnLeader = getDocIds(leader, true).stream()
                    .map(d -> new DocIdSeqNoAndSource(d.id(), null, d.seqNo(), d.primaryTerm(), d.version()))
                    .toList();
                assertThat(docsWithoutSourceOnFollower, equalTo(docsWithoutSourceOnLeader));
            } catch (Exception ex) {
                throw new AssertionError(ex);
            }
        }
    }

    private List<Translog.Operation> drainAll(Translog.Snapshot snapshot) throws IOException {
        List<Translog.Operation> operations = new ArrayList<>();
        Translog.Operation op;
        while ((op = snapshot.next()) != null) {
            final Translog.Operation newOp = op;
            logger.trace("Reading [{}]", op);
            assert operations.stream().allMatch(o -> o.seqNo() < newOp.seqNo()) : "Operations [" + operations + "], op [" + op + "]";
            operations.add(newOp);
        }
        return operations;
    }

    public void testOverFlow() throws Exception {
        long fromSeqNo = randomLongBetween(0, 5);
        long toSeqNo = randomLongBetween(Long.MAX_VALUE - 5, Long.MAX_VALUE);
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", fromSeqNo, toSeqNo, true, randomBoolean(), randomBoolean())) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(
                error.getMessage(),
                containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
            );
        }
    }

    public void testStats() throws Exception {
        try (Store store = createStore(); Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            int numOps = between(100, 5000);
            long startingSeqNo = randomLongBetween(0, Integer.MAX_VALUE);
            List<Engine.Operation> operations = generateHistoryOnReplica(
                numOps,
                startingSeqNo,
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            );
            applyOperations(engine, operations);

            LongSupplier fromSeqNo = () -> {
                if (randomBoolean()) {
                    return 0L;
                } else if (randomBoolean()) {
                    return startingSeqNo;
                } else {
                    return randomLongBetween(0, startingSeqNo);
                }
            };

            LongSupplier toSeqNo = () -> {
                final long maxSeqNo = engine.getSeqNoStats(-1).getMaxSeqNo();
                if (randomBoolean()) {
                    return maxSeqNo;
                } else if (randomBoolean()) {
                    return Long.MAX_VALUE;
                } else {
                    return randomLongBetween(maxSeqNo, Long.MAX_VALUE);
                }
            };
            // Can't access stats if didn't request it
            try (
                Translog.Snapshot snapshot = engine.newChangesSnapshot(
                    "test",
                    fromSeqNo.getAsLong(),
                    toSeqNo.getAsLong(),
                    false,
                    randomBoolean(),
                    false
                )
            ) {
                IllegalStateException error = expectThrows(IllegalStateException.class, snapshot::totalOperations);
                assertThat(error.getMessage(), equalTo("Access stats of a snapshot created with [access_stats] is false"));
                final List<Translog.Operation> translogOps = drainAll(snapshot);
                assertThat(translogOps, hasSize(numOps));
                error = expectThrows(IllegalStateException.class, snapshot::totalOperations);
                assertThat(error.getMessage(), equalTo("Access stats of a snapshot created with [access_stats] is false"));
            }
            // Access stats and operations
            try (
                Translog.Snapshot snapshot = engine.newChangesSnapshot(
                    "test",
                    fromSeqNo.getAsLong(),
                    toSeqNo.getAsLong(),
                    false,
                    randomBoolean(),
                    true
                )
            ) {
                assertThat(snapshot.totalOperations(), equalTo(numOps));
                final List<Translog.Operation> translogOps = drainAll(snapshot);
                assertThat(translogOps, hasSize(numOps));
                assertThat(snapshot.totalOperations(), equalTo(numOps));
            }
            // Verify count
            assertThat(engine.countChanges("test", fromSeqNo.getAsLong(), toSeqNo.getAsLong()), equalTo(numOps));
        }
    }
}
