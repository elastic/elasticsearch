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

import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class KeepUntilGlobalCheckpointDeletionPolicyTests extends EngineTestCase {

    public void testUnassignedGlobalCheckpoint() throws IOException {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final AtomicLong maxSeqNoLeap = new AtomicLong(0);
        final Path indexPath = createTempDir();

        try (Store store = createStore()) {
            int initDocs = scaledRandomIntBetween(10, 1000);
            int initCommits = 1;
            try (InternalEngine engine = newEngine(Integer.MAX_VALUE, store, indexPath, globalCheckpoint::get, maxSeqNoLeap::get)) {
                for (int i = 0; i < initDocs; i++) {
                    addDoc(engine, Integer.toString(i));
                    maxSeqNoLeap.set(randomInt(10));
                    if (frequently()) {
                        initCommits++;
                        engine.flush(true, true);
                    }
                    if (rarely()) {
                        engine.rollTranslogGeneration();
                    }
                }
                engine.flush(true, true);
                try (Translog.Snapshot snapshot = engine.getTranslog().newSnapshot()) {
                    assertThat("Unassigned global checkpoint reserves all translog", snapshot.totalOperations(), equalTo(initDocs));
                }
            }
            assertThat(DirectoryReader.listCommits(store.directory()), hasSize(initCommits + 1));

            final int maxKeptCommits = between(1, initCommits);
            try (InternalEngine engine = newEngine(maxKeptCommits, store, indexPath, globalCheckpoint::get, maxSeqNoLeap::get)) {
                assertThat("Unassigned global checkpoint reserves up to [maxKeptCommits]",
                    DirectoryReader.listCommits(store.directory()), hasSize(maxKeptCommits));
                int moreDocs = scaledRandomIntBetween(1, 100);
                for (int i = 0; i < moreDocs; i++) {
                    maxSeqNoLeap.set(randomInt(10));
                    addDoc(engine, Integer.toString(initDocs + i));
                    if (frequently()) {
                        engine.flush(true, true);
                    }
                }
                assertThat("Unassigned global checkpoint reserves up to [maxKeptCommits]",
                    DirectoryReader.listCommits(store.directory()), hasSize(maxKeptCommits));
            }
        }
    }

    public void testKeepUpGlobalCheckpoint() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final AtomicLong maxSeqNoLeap = new AtomicLong(0);
        final int maxKeptCommits = between(1, 100);
        final Path indexPath = createTempDir();

        try (Store store = createStore()) {
            int initDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = newEngine(maxKeptCommits, store, indexPath, globalCheckpoint::get, maxSeqNoLeap::get)) {
                for (int i = 0; i < initDocs; i++) {
                    addDoc(engine, Integer.toString(i));
                    globalCheckpoint.set(engine.seqNoService().getLocalCheckpoint());
                    if (frequently()) {
                        engine.flush(true, true);
                        assertThat("OnCommit deletes unreferenced commits", DirectoryReader.listCommits(store.directory()), hasSize(1));
                    }
                }
                engine.flush(true, true);
            }
            assertThat(DirectoryReader.listCommits(store.directory()), hasSize(1));
            try (InternalEngine engine = newEngine(maxKeptCommits, store, indexPath, globalCheckpoint::get, maxSeqNoLeap::get)) {
                assertThat("OnInit deletes unreferenced commits", DirectoryReader.listCommits(store.directory()), hasSize(1));
                int moreDocs = scaledRandomIntBetween(1, 100);
                for (int i = 0; i < moreDocs; i++) {
                    addDoc(engine, Integer.toString(initDocs + i));
                    globalCheckpoint.set(engine.seqNoService().getLocalCheckpoint());
                    if (frequently()) {
                        engine.flush(true, true);
                        assertThat("OnCommit deletes unreferenced commits", DirectoryReader.listCommits(store.directory()), hasSize(1));
                    }
                }
            }
        }
    }

    public void testLaggingGlobalCheckpointWithLargeRetention() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final AtomicLong maxSeqNoLeap = new AtomicLong(0);
        final Path indexPath = createTempDir();

        try (Store store = createStore()) {
            int numDocs = scaledRandomIntBetween(100, 1000);
            final int maxKeptCommits = between(1000, 2000);
            try (InternalEngine engine = newEngine(maxKeptCommits, store, indexPath, globalCheckpoint::get, maxSeqNoLeap::get)) {
                for (int i = 0; i < numDocs; i++) {
                    maxSeqNoLeap.set(randomInt(10));
                    addDoc(engine, Integer.toString(i));
                    if (frequently()) {
                        globalCheckpoint.set(engine.seqNoService().getLocalCheckpoint());
                    }
                    if (frequently()) {
                        engine.flush(true, true);
                        assertThat("Reserved commits should be 1", reservedCommits(store, globalCheckpoint.get()), hasSize(1));
                    }
                    if (rarely()) {
                        engine.rollTranslogGeneration();
                    }
                }
                engine.flush(true, true);
            }
            assertThat("Reserved commits should be 1", reservedCommits(store, globalCheckpoint.get()), hasSize(1));
        }
    }

    public void testLaggingGlobalCheckpointWithSmallRetention() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final AtomicLong maxSeqNoLeap = new AtomicLong(0);
        final Path indexPath = createTempDir();
        final int maxKeptCommits = between(1, 5);

        try (Store store = createStore()) {
            int initDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = newEngine(maxKeptCommits, store, indexPath, globalCheckpoint::get, maxSeqNoLeap::get)) {
                for (int i = 0; i < initDocs; i++) {
                    maxSeqNoLeap.set(randomInt(10));
                    addDoc(engine, Integer.toString(i));
                    maxSeqNoLeap.set(randomInt(10));
                    if (frequently()) {
                        engine.flush(true, true);
                    }
                    if (rarely()) {
                        engine.rollTranslogGeneration();
                    }
                }
                engine.flush(true, true);
            }
            assertThat(DirectoryReader.listCommits(store.directory()), hasSize(lessThanOrEqualTo(maxKeptCommits)));

            try (InternalEngine engine = newEngine(maxKeptCommits, store, indexPath, globalCheckpoint::get, maxSeqNoLeap::get)) {
                assertThat("Unassigned global checkpoint reserves up to [maxKeptCommits]",
                    DirectoryReader.listCommits(store.directory()), hasSize(lessThanOrEqualTo(maxKeptCommits)));
                int moreDocs = scaledRandomIntBetween(1, 100);
                for (int i = 0; i < moreDocs; i++) {
                    maxSeqNoLeap.set(randomInt(10));
                    addDoc(engine, Integer.toString(initDocs + i));
                    if (frequently()) {
                        engine.flush(true, true);
                        assertThat("Unassigned global checkpoint reserves up to [maxKeptCommits]",
                            DirectoryReader.listCommits(store.directory()), hasSize(lessThanOrEqualTo(maxKeptCommits)));
                    }
                }
            }
        }
    }

    List<IndexCommit> reservedCommits(Store store, long currentGlobalCheckpoint) throws IOException {
        List<IndexCommit> reservedCommits = new ArrayList<>();
        List<IndexCommit> existingCommits = DirectoryReader.listCommits(store.directory());
        for (IndexCommit commit : existingCommits) {
            if (commit.getFileNames().isEmpty() == false) {
                if (Long.parseLong(commit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)) <= currentGlobalCheckpoint) {
                    reservedCommits.add(commit);
                }
            }
        }
        return reservedCommits;
    }

    void addDoc(Engine engine, String id) throws IOException {
        ParseContext.Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument(id, null, document, B_1, null);
        engine.index(indexForDoc(doc));
    }

    InternalEngine newEngine(final int maxKeptIndexCommits, final Store store, final Path indexPath,
                             final LongSupplier globalCheckpointSupplier, final LongSupplier maxSeqNoLeapSupplier) throws IOException {

        final IndexSettings indexSettings = new IndexSettings(defaultSettings.getIndexMetaData(),
            defaultSettings.getNodeSettings(), defaultSettings.getScopedSettings());

        final IndexMetaData.Builder builder = IndexMetaData.builder(indexSettings.getIndexMetaData());
        builder.settings(Settings.builder().put(indexSettings.getSettings())
            .put(IndexSettings.MAX_INDEX_COMMITS_RETENTION.getKey(), maxKeptIndexCommits));
        indexSettings.updateIndexMetaData(builder.build());

        return createEngine(indexSettings, store, indexPath, newMergePolicy(), null,
            (config, seqNoStats) -> new SequenceNumbersService(
                config.getShardId(),
                config.getAllocationId(),
                config.getIndexSettings(),
                seqNoStats.getMaxSeqNo(),
                seqNoStats.getLocalCheckpoint(),
                seqNoStats.getGlobalCheckpoint()) {
                @Override
                public long getGlobalCheckpoint() {
                    return globalCheckpointSupplier.getAsLong();
                }

                @Override
                public long getMaxSeqNo() {
                    return super.getMaxSeqNo() + maxSeqNoLeapSupplier.getAsLong();
                }
            }
        );
    }
}
