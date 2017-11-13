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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class KeepUntilGlobalCheckpointDeletionPolicyTests extends EngineTestCase {
    final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
    final AtomicInteger docId = new AtomicInteger();

    @Before
    public void resetCounters() throws Exception {
        globalCheckpoint.set(SequenceNumbers.UNASSIGNED_SEQ_NO);
        docId.set(0);
    }

    public void testUnassignedGlobalCheckpoint() throws IOException {
        Path indexPath = createTempDir();
        globalCheckpoint.set(SequenceNumbers.UNASSIGNED_SEQ_NO);
        try (Store store = createStore()) {
            int initDocs = scaledRandomIntBetween(10, 1000);
            int initCommits = 1;
            try (InternalEngine engine = newEngine(store, indexPath)) {
                for (int i = 0; i < initDocs; i++) {
                    addDoc(engine);
                    if (frequently()) {
                        initCommits++;
                        engine.flush(true, true);
                    }
                    if (rarely()) {
                        engine.rollTranslogGeneration();
                    }
                }
                engine.flush(true, true);
            }
            assertThat(DirectoryReader.listCommits(store.directory()), hasSize(initCommits + 1));
            try (InternalEngine engine = newEngine(store, indexPath)) {
                engine.refresh("test");
                assertThat("Unassigned global checkpoint reserves all commits", DirectoryReader.listCommits(store.directory()),
                    hasSize(initCommits + 1));
                try (Translog.Snapshot snapshot = engine.getTranslog().newSnapshot()) {
                    assertThat("Unassigned global checkpoint reserves all translog", snapshot.totalOperations(), equalTo(initDocs));
                }
                int moreDocs = scaledRandomIntBetween(1, 100);
                int extraCommits = 0;
                for (int i = 0; i < moreDocs; i++) {
                    addDoc(engine);
                    if (frequently()) {
                        engine.flush(true, true);
                        extraCommits++;
                    }
                }
                assertThat("Unassigned global checkpoint reserves all commits", DirectoryReader.listCommits(store.directory()),
                    hasSize(initCommits + 1 + extraCommits));
                try (Translog.Snapshot snapshot = engine.getTranslog().newSnapshot()) {
                    assertThat("Unassigned global checkpoint reserves all translog", snapshot.totalOperations(),
                        equalTo(initDocs + moreDocs));
                }
            }
        }
    }

    public void testKeepUpGlobalCheckpoint() throws Exception {
        Path indexPath = createTempDir();
        try (Store store = createStore()) {
            int initDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = newEngine(store, indexPath)) {
                for (int i = 0; i < initDocs; i++) {
                    addDoc(engine);
                    globalCheckpoint.set(engine.seqNoService().getLocalCheckpoint());
                    if (frequently()) {
                        engine.flush(true, true);
                    }
                }
                engine.flush(true, true);
            }
            assertThat(DirectoryReader.listCommits(store.directory()), hasSize(1));
            try (InternalEngine engine = newEngine(store, indexPath)) {
                assertThat("OnInit deletes unreferenced commits", DirectoryReader.listCommits(store.directory()), hasSize(1));
                int moreDocs = scaledRandomIntBetween(1, 100);
                for (int i = 0; i < moreDocs; i++) {
                    addDoc(engine);
                    globalCheckpoint.set(engine.seqNoService().getLocalCheckpoint());
                    if (frequently()) {
                        engine.flush(true, true);
                        assertThat("OnCommit deletes unreferenced commits", DirectoryReader.listCommits(store.directory()), hasSize(1));
                    }
                }
            }
        }
    }

    public void testLaggingGlobalCheckpoint() throws Exception {
        Path indexPath = createTempDir();
        try (Store store = createStore()) {
            int initDocs = scaledRandomIntBetween(100, 1000);
            try (InternalEngine engine = newEngine(store, indexPath)) {
                for (int i = 0; i < initDocs; i++) {
                    addDoc(engine);
                    if (frequently()) {
                        globalCheckpoint.set(engine.seqNoService().getLocalCheckpoint());
                    }
                    if (frequently()) {
                        engine.flush(true, true);
                    }
                    if (rarely()) {
                        engine.rollTranslogGeneration();
                    }
                    try (Translog.Snapshot snapshot = engine.getTranslog().newSnapshot()) {
                        assertThat((long) snapshot.totalOperations(), greaterThanOrEqualTo(requiredOperations(i + 1)));
                    }
                }
                engine.flush(true, true);
            }
            assertThat("Reserved commits should be 1", reservedCommits(), hasSize(1));

            try (InternalEngine engine = newEngine(store, indexPath)) {
                assertThat("Reserved commits should always be 1", reservedCommits(), hasSize(1));
                int moreDocs = scaledRandomIntBetween(1, 100);
                for (int i = 0; i < moreDocs; i++) {
                    addDoc(engine);
                    if (frequently()) {
                        globalCheckpoint.set(engine.seqNoService().getLocalCheckpoint());
                    }
                    if (frequently()) {
                        engine.flush(true, true);
                        assertThat("Reserved commits should be 1", reservedCommits(), hasSize(1));
                    }
                    if (rarely()) {
                        engine.rollTranslogGeneration();
                    }
                    try (Translog.Snapshot snapshot = engine.getTranslog().newSnapshot()) {
                        assertThat((long) snapshot.totalOperations(), greaterThanOrEqualTo(requiredOperations(initDocs + i + 1)));
                    }
                }
            }
        }
    }

    long requiredOperations(int processedOps) {
        return processedOps - Math.max(0, globalCheckpoint.get());
    }

    List<IndexCommit> reservedCommits() throws IOException {
        List<IndexCommit> reservedCommits = new ArrayList<>();
        List<IndexCommit> existingCommits = DirectoryReader.listCommits(store.directory());
        for (IndexCommit commit : existingCommits) {
            if (Long.parseLong(commit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)) <= globalCheckpoint.get()) {
                reservedCommits.add(commit);
            }
        }
        return reservedCommits;
    }

    void addDoc(Engine engine) throws IOException {
        ParseContext.Document document = testDocumentWithTextField();
        document.add(new Field(SourceFieldMapper.NAME, BytesReference.toBytes(B_1), SourceFieldMapper.Defaults.FIELD_TYPE));
        ParsedDocument doc = testParsedDocument(Integer.toString(docId.getAndIncrement()), null, document, B_1, null);
        engine.index(indexForDoc(doc));
    }

    InternalEngine newEngine(Store store, Path indexPath) throws IOException {
        return createEngine(defaultSettings, store, indexPath, newMergePolicy(), null,
            (config, seqNoStats) -> new SequenceNumbersService(
                config.getShardId(),
                config.getAllocationId(),
                config.getIndexSettings(),
                seqNoStats.getMaxSeqNo(),
                seqNoStats.getLocalCheckpoint(),
                seqNoStats.getGlobalCheckpoint()) {
                @Override
                public long getGlobalCheckpoint() {
                    return globalCheckpoint.get();
                }
            }
        );
    }
}
