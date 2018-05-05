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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class LuceneChangesSnapshotTests extends EngineTestCase {

    public void testEmptyEngine() throws Exception {
        MapperService mapper = createMapperService("test");
        long fromSeqNo = randomNonNegativeLong();
        long toSeqNo = randomLongBetween(fromSeqNo, Long.MAX_VALUE);
        try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, fromSeqNo, toSeqNo, true)) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(error.getMessage(),
                containsString("not all operations between min_seqno [" + fromSeqNo + "] and max_seqno [" + toSeqNo + "] found"));
        }
        try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, fromSeqNo, toSeqNo, false)) {
            assertThat(drainAll(snapshot), empty());
        }
    }

    public void testRequiredFullRange() throws Exception {
        MapperService mapper = createMapperService("test");
        int numOps = between(0, 100);
        for (int i = 0; i < numOps; i++) {
            String id = Integer.toString(randomIntBetween(i, i + 5));
            ParsedDocument doc = createParsedDoc(id, null);
            if (randomBoolean()) {
                engine.index(indexForDoc(doc));
            } else {
                engine.delete(new Engine.Delete(doc.type(), doc.id(), newUid(doc.id()), primaryTerm.get()));
            }
            if (rarely()) {
                engine.flush();
            }
        }
        int iters = between(1, 10);
        for (int i = 0; i < iters; i++) {
            int fromSeqNo = between(0, numOps * 2);
            int toSeqNo = between(Math.max(numOps + 1, fromSeqNo), numOps * 10);
            try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, fromSeqNo, toSeqNo, true)) {
                IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
                assertThat(error.getMessage(),
                    containsString("not all operations between min_seqno [" + fromSeqNo + "] and max_seqno [" + toSeqNo + "] found"));
            }
            try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, fromSeqNo, toSeqNo, false)) {
                List<Translog.Operation> ops = drainAll(snapshot);
                int readOps = Math.min(toSeqNo, numOps) - Math.min(fromSeqNo, numOps);
                assertThat(ops, hasSize(readOps));
            }
        }
    }

    public void testDedupByPrimaryTerm() throws Exception {
        MapperService mapper = createMapperService("test");
        Map<Long, Long> latestOperations = new HashMap<>();
        int numOps = scaledRandomIntBetween(10, 2000);
        List<Long> seqNos = LongStream.range(0, numOps).boxed().collect(Collectors.toList());
        Randomness.shuffle(seqNos);
        for (int i = 0; i < numOps; i++) {
            if (randomBoolean()) {
                primaryTerm.set(randomLongBetween(primaryTerm.get(), Long.MAX_VALUE));
                engine.rollTranslogGeneration();
            }
            if (randomBoolean()) {
                primaryTerm.set(randomLongBetween(1, primaryTerm.get()));
            }
            String id = Integer.toString(randomIntBetween(i, i + 5));
            ParsedDocument doc = createParsedDoc(id, null);
            final long seqNo = seqNos.remove(0);
            if (randomBoolean()) {
                engine.index(replicaIndexForDoc(doc, randomNonNegativeLong(), seqNo, false));
            } else {
                engine.delete(replicaDeleteForDoc(doc.id(), randomNonNegativeLong(), seqNo, threadPool.relativeTimeInMillis()));
            }
            latestOperations.put(seqNo, primaryTerm.get());
            if (rarely()) {
                engine.flush();
            }
            if (rarely()){
                engine.forceMerge(randomBoolean(), between(1, 2), false, false, false);
            }
        }
        final boolean requiredFullRange = randomBoolean();
        long fromSeqNo = randomLongBetween(0, numOps);
        long toSeqNo = randomLongBetween(fromSeqNo, requiredFullRange ? numOps : numOps * 2);
        try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapper, fromSeqNo, toSeqNo, requiredFullRange)) {
            List<Translog.Operation> ops = drainAll(snapshot);
            for (Translog.Operation op : ops) {
                assertThat(op.toString(), op.primaryTerm(), equalTo(latestOperations.get(op.seqNo())));
            }
        }
    }

    public void testHistoryOnPrimary() throws Exception {
        final List<Engine.Operation> operations = generateSingleDocHistory(false,
            randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), primaryTerm.get(), 10, 1000, "1");
        assertOperationHistoryInLucene(operations, true);
    }

    public void testHistoryOnReplica() throws Exception {
        final List<Engine.Operation> operations = generateSingleDocHistory(true,
            randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL), 2, 10, 1000, "2");
        Randomness.shuffle(operations);
        assertOperationHistoryInLucene(operations, false);
    }

    private void assertOperationHistoryInLucene(List<Engine.Operation> operations, boolean requiredFullRange) throws IOException {
        Set<Long> expectedSeqNos = new HashSet<>();
        Settings settings = Settings.builder()
            .put(defaultSettings.getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), operations.size())
            .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(defaultSettings.getIndexMetaData()).settings(settings).build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexMetaData);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore();
             InternalEngine engine = createEngine(config(indexSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get))) {
            for (Engine.Operation op : operations) {
                if (op instanceof Engine.Index) {
                    Engine.IndexResult indexResult = engine.index((Engine.Index) op);
                    assertThat(indexResult.getFailure(), nullValue());
                    expectedSeqNos.add(indexResult.getSeqNo());
                } else {
                    Engine.DeleteResult deleteResult = engine.delete((Engine.Delete) op);
                    assertThat(deleteResult.getFailure(), nullValue());
                    expectedSeqNos.add(deleteResult.getSeqNo());
                }
                if (randomBoolean()) {
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getLocalCheckpointTracker().getCheckpoint()));
                }
                if (rarely()) {
                    engine.refresh("test");
                }
                if (rarely()) {
                    engine.flush();
                }
                if (rarely()) {
                    engine.forceMerge(true);
                }
            }
            long maxSeqNo = engine.getLocalCheckpointTracker().getMaxSeqNo();
            long toSeqNo = requiredFullRange ? maxSeqNo : randomLongBetween(maxSeqNo, Long.MAX_VALUE);
            MapperService mapperService = createMapperService("test");
            try (Translog.Snapshot snapshot = engine.newLuceneChangesSnapshot("test", mapperService, 0, toSeqNo, requiredFullRange)) {
                List<Translog.Operation> actualOps = drainAll(snapshot);
                List<Long> sortedSeqNo = new ArrayList<>(expectedSeqNos);
                Collections.sort(sortedSeqNo);
                assertThat(actualOps.stream().map(o -> o.seqNo()).collect(Collectors.toList()),
                    containsInAnyOrder(sortedSeqNo.toArray()));
            }

            assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine, mapperService);
        }
    }

    List<Translog.Operation> drainAll(Translog.Snapshot snapshot) throws IOException {
        List<Translog.Operation> operations = new ArrayList<>();
        Translog.Operation op;
        while ((op = snapshot.next()) != null) {
            final Translog.Operation newOp = op;
            assert operations.stream().allMatch(o -> o.seqNo() < newOp.seqNo()) : "Operations [" + operations + "], op [" + op + "]";
            operations.add(newOp);
        }
        return operations;
    }
}
