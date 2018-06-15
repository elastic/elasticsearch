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
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class NoopEngineTests extends EngineTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);

    public void testNoopEngine() throws IOException {
        engine.close();
        final NoopEngine engine = new NoopEngine(noopConfig(INDEX_SETTINGS, store, primaryTranslogDir));
        expectThrows(UnsupportedOperationException.class, () -> engine.index(null));
        expectThrows(UnsupportedOperationException.class, () -> engine.delete(null));
        expectThrows(UnsupportedOperationException.class, () -> engine.noOp(null));
        expectThrows(UnsupportedOperationException.class, () -> engine.syncFlush(null, null));
        expectThrows(UnsupportedOperationException.class, () -> engine.get(null, null));
        expectThrows(UnsupportedOperationException.class, () -> engine.acquireSearcher(null, null));
        expectThrows(UnsupportedOperationException.class, () -> engine.ensureTranslogSynced(null));
        expectThrows(UnsupportedOperationException.class, engine::activateThrottling);
        expectThrows(UnsupportedOperationException.class, engine::deactivateThrottling);
        assertThat(engine.refreshNeeded(), equalTo(false));
        assertThat(engine.shouldPeriodicallyFlush(), equalTo(false));
        engine.close();
    }

    public void testTwoNoopEngines() throws IOException {
        engine.close();
        // It's so noop you can even open two engines for the same store without tripping anything,
        // this ensures we're not doing any kind of locking on the store or filesystem level in
        // the noop engine
        final NoopEngine engine1 = new NoopEngine(noopConfig(INDEX_SETTINGS, store, primaryTranslogDir));
        final NoopEngine engine2 = new NoopEngine(noopConfig(INDEX_SETTINGS, store, primaryTranslogDir));
        engine1.close();
        engine2.close();
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

        engine.flush(true, true);
        engine.getTranslog().getDeletionPolicy().setRetentionSizeInBytes(-1);
        engine.getTranslog().getDeletionPolicy().setRetentionAgeInMillis(-1);
        engine.getTranslog().getDeletionPolicy().setMinTranslogGenerationForRecovery(
            engine.getTranslog().getGeneration().translogFileGeneration);
        engine.flush(true, true);

        long localCheckpoint = engine.getLocalCheckpoint();
        long maxSeqNo = engine.getSeqNoStats(100L).getMaxSeqNo();
        engine.close();

        final NoopEngine noopEngine = new NoopEngine(noopConfig(INDEX_SETTINGS, store, primaryTranslogDir, tracker));
        assertThat(noopEngine.getLocalCheckpoint(), equalTo(localCheckpoint));
        assertThat(noopEngine.getSeqNoStats(100L).getMaxSeqNo(), equalTo(maxSeqNo));
        try (Engine.IndexCommitRef ref = noopEngine.acquireLastIndexCommit(false)) {
            try (IndexReader reader = DirectoryReader.open(ref.getIndexCommit())) {
                assertThat(reader.numDocs(), equalTo(docs));
            }
        }
        noopEngine.close();
    }

    public void testNoopEngineWithInvalidTranslogUUID() throws IOException {
        Path newTranslogDir = createTempDir();
        // A new translog will have a different UUID than the existing store/noop engine does
        Translog newTranslog = createTranslog(newTranslogDir, () -> 1L);
        newTranslog.close();
        EngineCreationFailureException e = expectThrows(EngineCreationFailureException.class,
            () -> new NoopEngine(noopConfig(INDEX_SETTINGS, store, newTranslogDir)));
        assertThat(e.getCause(), instanceOf(TranslogCorruptedException.class));
    }
}
