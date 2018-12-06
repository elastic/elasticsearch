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

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class ReadOnlyEngineTests extends EngineTestCase {

    public void testReadOnlyEngine() throws Exception {
        IOUtils.close(engine, store);
        Engine readOnlyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            final SeqNoStats lastSeqNoStats;
            final List<DocIdSeqNoAndTerm> lastDocIds;
            try (InternalEngine engine = createEngine(config)) {
                Engine.Get get = null;
                for (int i = 0; i < numDocs; i++) {
                    if (rarely()) {
                        continue; // gap in sequence number
                    }
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false));
                    if (get == null || rarely()) {
                        get = newGet(randomBoolean(), doc);
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getLocalCheckpoint()));
                }
                engine.syncTranslog();
                engine.flush();
                readOnlyEngine = new ReadOnlyEngine(engine.engineConfig, engine.getSeqNoStats(globalCheckpoint.get()),
                    engine.getTranslogStats(), false, Function.identity());
                lastSeqNoStats = engine.getSeqNoStats(globalCheckpoint.get());
                lastDocIds = getDocIds(engine, true);
                assertThat(readOnlyEngine.getLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                    assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                for (int i = 0; i < numDocs; i++) {
                    if (randomBoolean()) {
                        String delId = Integer.toString(i);
                        engine.delete(new Engine.Delete("test", delId, newUid(delId), primaryTerm.get()));
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                }
                Engine.Searcher external = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL);
                Engine.Searcher internal = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                assertSame(external.reader(), internal.reader());
                IOUtils.close(external, internal);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                try (Engine.GetResult getResult = readOnlyEngine.get(get, readOnlyEngine::acquireSearcher)) {
                    assertTrue(getResult.exists());
                }

            }
            // Close and reopen the main engine
            InternalEngineTests.trimUnsafeCommits(config);
            try (InternalEngine recoveringEngine = new InternalEngine(config)) {
                recoveringEngine.initializeMaxSeqNoOfUpdatesOrDeletes();
                recoveringEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
            }
        } finally {
            IOUtils.close(readOnlyEngine);
        }
    }

    public void testFlushes() throws IOException {
        IOUtils.close(engine, store);
        Engine readOnlyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    if (rarely()) {
                        continue; // gap in sequence number
                    }
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(new Engine.Index(newUid(doc), doc, i, primaryTerm.get(), 1, null, Engine.Operation.Origin.REPLICA,
                        System.nanoTime(), -1, false));
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getLocalCheckpoint()));
                }
                engine.syncTranslog();
                engine.flushAndClose();
                readOnlyEngine = new ReadOnlyEngine(engine.engineConfig, null , null, true, Function.identity());
                Engine.CommitId flush = readOnlyEngine.flush(randomBoolean(), randomBoolean());
                assertEquals(flush, readOnlyEngine.flush(randomBoolean(), randomBoolean()));
            } finally {
                IOUtils.close(readOnlyEngine);
            }
        }
    }

    public void testReadOnly() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            store.createEmpty(Version.CURRENT.luceneVersion);
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null , null, true, Function.identity())) {
                Class<? extends Throwable> expectedException = LuceneTestCase.TEST_ASSERTS_ENABLED ? AssertionError.class :
                    UnsupportedOperationException.class;
                expectThrows(expectedException, () -> readOnlyEngine.index(null));
                expectThrows(expectedException, () -> readOnlyEngine.delete(null));
                expectThrows(expectedException, () -> readOnlyEngine.noOp(null));
                expectThrows(UnsupportedOperationException.class, () ->  readOnlyEngine.syncFlush(null, null));
            }
        }
    }
}
