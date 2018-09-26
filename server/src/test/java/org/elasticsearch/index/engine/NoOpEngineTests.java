/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class NoOpEngineTests extends EngineTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    public void testNoopEngine() throws IOException {
        engine.close();
        final NoOpEngine engine = new NoOpEngine(noOpConfig(INDEX_SETTINGS, store, primaryTranslogDir));
        expectThrows(UnsupportedOperationException.class, () -> engine.syncFlush(null, null));
        expectThrows(UnsupportedOperationException.class, () -> engine.ensureTranslogSynced(null));
        assertThat(engine.refreshNeeded(), equalTo(false));
        assertThat(engine.shouldPeriodicallyFlush(), equalTo(false));
        engine.close();
    }

    public void testTwoNoopEngines() throws IOException {
        engine.close();
        // Ensure that we can't open two noop engines for the same store
        final EngineConfig engineConfig = noOpConfig(INDEX_SETTINGS, store, primaryTranslogDir);
        try (NoOpEngine ignored = new NoOpEngine(engineConfig)) {
            UncheckedIOException e = expectThrows(UncheckedIOException.class, () -> new NoOpEngine(engineConfig));
            assertThat(e.getCause(), instanceOf(LockObtainFailedException.class));
        }
    }

    public void testNoopAfterRegularEngine() throws IOException {
        int docs = randomIntBetween(1, 10);
        ReplicationTracker tracker = (ReplicationTracker) engine.config().getGlobalCheckpointSupplier();
        ShardRouting routing = TestShardRouting.newShardRouting("test", shardId.id(), "node",
            null, true, ShardRoutingState.STARTED, allocationId);
        IndexShardRoutingTable table = new IndexShardRoutingTable.Builder(shardId).addShard(routing).build();
        tracker.updateFromMaster(1L, Collections.singleton(allocationId.getId()), table, Collections.emptySet());
        tracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        for (int i = 0; i < docs; i++) {
            ParsedDocument doc = testParsedDocument("" + i, null, testDocumentWithTextField(), B_1, null);
            engine.index(indexForDoc(doc));
            tracker.updateLocalCheckpoint(allocationId.getId(), i);
        }

        flushAndTrimTranslog(engine);

        long localCheckpoint = engine.getLocalCheckpoint();
        long maxSeqNo = engine.getSeqNoStats(100L).getMaxSeqNo();
        engine.close();

        final NoOpEngine noOpEngine = new NoOpEngine(noOpConfig(INDEX_SETTINGS, store, primaryTranslogDir, tracker));
        assertThat(noOpEngine.getLocalCheckpoint(), equalTo(localCheckpoint));
        assertThat(noOpEngine.getSeqNoStats(100L).getMaxSeqNo(), equalTo(maxSeqNo));
        try (Engine.IndexCommitRef ref = noOpEngine.acquireLastIndexCommit(false)) {
            try (IndexReader reader = DirectoryReader.open(ref.getIndexCommit())) {
                assertThat(reader.numDocs(), equalTo(docs));
            }
        }
        noOpEngine.close();
    }

    public void testNoopEngineWithInvalidTranslogUUID() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 100);
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false));
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(engine.getLocalCheckpoint());
                }
                flushAndTrimTranslog(engine);
            }

            final Path newTranslogDir = createTempDir();
            // A new translog will have a different UUID than the existing store/noOp engine does
            Translog newTranslog = createTranslog(newTranslogDir, () -> 1L);
            newTranslog.close();

            EngineCreationFailureException e = expectThrows(EngineCreationFailureException.class,
                () -> new NoOpEngine(noOpConfig(INDEX_SETTINGS, store, newTranslogDir)));
            assertThat(e.getCause(), instanceOf(TranslogCorruptedException.class));
        }
    }

    public void testNoopEngineWithNonZeroTranslogOperations() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            final MergePolicy mergePolicy = NoMergePolicy.INSTANCE;
            EngineConfig config = config(defaultSettings, store, createTempDir(), mergePolicy, null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 100);
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false));
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(engine.getLocalCheckpoint());
                }
                engine.syncTranslog();
                engine.flushAndClose();
                engine.close();

                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new NoOpEngine(engine.engineConfig));
                assertThat(e.getMessage(), is("Expected 0 translog operations but there were " + numDocs));
            }
        }
    }

    public void testNoOpEngineDocStats() throws Exception {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            final int numDocs = scaledRandomIntBetween(10, 3000);
            int deletions = 0;
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    engine.index(indexForDoc(createParsedDoc(Integer.toString(i), null)));
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(engine.getLocalCheckpoint());
                }

                for (int i = 0; i < numDocs; i++) {
                    if (randomBoolean()) {
                        String delId = Integer.toString(i);
                        Engine.DeleteResult result = engine.delete(new Engine.Delete("test", delId, newUid(delId), primaryTerm.get()));
                        assertTrue(result.isFound());
                        globalCheckpoint.set(engine.getLocalCheckpoint());
                        deletions += 1;
                    }
                }
                engine.waitForOpsToComplete(numDocs + deletions - 1);
                flushAndTrimTranslog(engine);
                engine.close();
            }

            final DocsStats expectedDocStats;
            try (InternalEngine engine = createEngine(config)) {
                expectedDocStats = engine.docStats();
            }

            try (NoOpEngine noOpEngine = new NoOpEngine(config)) {
                assertEquals(expectedDocStats.getCount(), noOpEngine.docStats().getCount());
                assertEquals(expectedDocStats.getDeleted(), noOpEngine.docStats().getDeleted());
                assertEquals(expectedDocStats.getTotalSizeInBytes(), noOpEngine.docStats().getTotalSizeInBytes());
                assertEquals(expectedDocStats.getAverageSizeInBytes(), noOpEngine.docStats().getAverageSizeInBytes());
            } catch (AssertionError e) {
                logger.error(config.getMergePolicy());
                throw e;
            }
        }
    }

    private void flushAndTrimTranslog(final InternalEngine engine) {
        engine.flush(true, true);
        final TranslogDeletionPolicy deletionPolicy = engine.getTranslog().getDeletionPolicy();
        deletionPolicy.setRetentionSizeInBytes(-1);
        deletionPolicy.setRetentionAgeInMillis(-1);
        deletionPolicy.setMinTranslogGenerationForRecovery(engine.getTranslog().getGeneration().translogFileGeneration);
        engine.flush(true, true);
    }
}
