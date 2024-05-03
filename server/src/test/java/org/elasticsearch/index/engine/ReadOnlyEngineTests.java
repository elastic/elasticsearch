/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogStats;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader.getElasticsearchDirectoryReader;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class ReadOnlyEngineTests extends EngineTestCase {

    public void testReadOnlyEngine() throws Exception {
        IOUtils.close(engine, store);
        Engine readOnlyEngine = null;
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            final SeqNoStats lastSeqNoStats;
            final List<DocIdSeqNoAndSource> lastDocIds;
            try (InternalEngine engine = createEngine(config)) {
                Engine.Get get = null;
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    if (get == null || rarely()) {
                        get = newGet(randomBoolean(), doc);
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                }
                engine.syncTranslog();
                globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), engine.getPersistedLocalCheckpoint()));
                engine.flush();
                readOnlyEngine = new ReadOnlyEngine(
                    engine.engineConfig,
                    engine.getSeqNoStats(globalCheckpoint.get()),
                    engine.getTranslogStats(),
                    false,
                    Function.identity(),
                    true,
                    randomBoolean()
                );
                lastSeqNoStats = engine.getSeqNoStats(globalCheckpoint.get());
                lastDocIds = getDocIds(engine, true);
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                for (int i = 0; i < numDocs; i++) {
                    if (randomBoolean()) {
                        String delId = Integer.toString(i);
                        engine.delete(new Engine.Delete(delId, newUid(delId), primaryTerm.get()));
                    }
                    if (rarely()) {
                        engine.flush();
                    }
                }
                try (
                    ReadOnlyEngine readOnlyEngineWithLazySoftDeletes = new ReadOnlyEngine(
                        engine.engineConfig,
                        engine.getSeqNoStats(globalCheckpoint.get()),
                        engine.getTranslogStats(),
                        false,
                        Function.identity(),
                        true,
                        true
                    )
                ) {
                    EngineTestCase.checkNoSoftDeletesLoaded(readOnlyEngineWithLazySoftDeletes);
                }
                Engine.Searcher external = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.EXTERNAL);
                Engine.Searcher internal = readOnlyEngine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
                assertSame(external.getIndexReader(), internal.getIndexReader());
                assertThat(external.getIndexReader(), instanceOf(DirectoryReader.class));
                DirectoryReader dirReader = external.getDirectoryReader();
                ElasticsearchDirectoryReader esReader = getElasticsearchDirectoryReader(dirReader);
                IndexReader.CacheHelper helper = esReader.getReaderCacheHelper();
                assertNotNull(helper);
                assertEquals(helper.getKey(), dirReader.getReaderCacheHelper().getKey());

                IOUtils.close(external, internal);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
                MapperService mapperService = createMapperService();
                try (
                    Engine.GetResult getResult = readOnlyEngine.get(
                        get,
                        mapperService.mappingLookup(),
                        mapperService.documentParser(),
                        randomSearcherWrapper()
                    )
                ) {
                    assertTrue(getResult.exists());
                }
            }
            // Close and reopen the main engine
            try (InternalEngine recoveringEngine = new InternalEngine(config)) {
                recoverFromTranslog(recoveringEngine, translogHandler, Long.MAX_VALUE);
                // the locked down engine should still point to the previous commit
                assertThat(readOnlyEngine.getPersistedLocalCheckpoint(), equalTo(lastSeqNoStats.getLocalCheckpoint()));
                assertThat(readOnlyEngine.getSeqNoStats(globalCheckpoint.get()).getMaxSeqNo(), equalTo(lastSeqNoStats.getMaxSeqNo()));
                assertThat(getDocIds(readOnlyEngine, false), equalTo(lastDocIds));
            }
        } finally {
            IOUtils.close(readOnlyEngine);
        }
    }

    public void testEnsureMaxSeqNoIsEqualToGlobalCheckpoint() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            final int numDocs = scaledRandomIntBetween(10, 100);
            try (InternalEngine engine = createEngine(config)) {
                long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    maxSeqNo = engine.getProcessedLocalCheckpoint();
                }
                engine.syncTranslog();
                globalCheckpoint.set(engine.getPersistedLocalCheckpoint() - 1);
                engine.flushAndClose();

                IllegalStateException exception = expectThrows(
                    IllegalStateException.class,
                    () -> new ReadOnlyEngine(config, null, null, true, Function.identity(), true, randomBoolean()) {
                        @Override
                        protected boolean assertMaxSeqNoEqualsToGlobalCheckpoint(final long maxSeqNo, final long globalCheckpoint) {
                            // we don't want the assertion to trip in this test
                            return true;
                        }
                    }
                );
                assertThat(
                    exception.getMessage(),
                    equalTo(
                        "Maximum sequence number ["
                            + maxSeqNo
                            + "] from last commit does not match global checkpoint ["
                            + globalCheckpoint.get()
                            + "]"
                    )
                );
            }
        }
    }

    public void testReadOnly() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            store.createEmpty();
            try (
                ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(
                    config,
                    null,
                    new TranslogStats(),
                    true,
                    Function.identity(),
                    true,
                    randomBoolean()
                )
            ) {
                Class<? extends Throwable> expectedException = LuceneTestCase.TEST_ASSERTS_ENABLED
                    ? AssertionError.class
                    : UnsupportedOperationException.class;
                expectThrows(expectedException, () -> readOnlyEngine.index(null));
                expectThrows(expectedException, () -> readOnlyEngine.delete(null));
                expectThrows(expectedException, () -> readOnlyEngine.noOp(null));
            }
        }
    }

    /**
     * Test that {@link ReadOnlyEngine#verifyEngineBeforeIndexClosing()} never fails
     * whatever the value of the global checkpoint to check is.
     */
    public void testVerifyShardBeforeIndexClosingIsNoOp() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            store.createEmpty();
            try (
                ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(
                    config,
                    null,
                    new TranslogStats(),
                    true,
                    Function.identity(),
                    true,
                    randomBoolean()
                )
            ) {
                globalCheckpoint.set(randomNonNegativeLong());
                try {
                    readOnlyEngine.verifyEngineBeforeIndexClosing();
                } catch (final IllegalStateException e) {
                    fail("Read-only engine pre-closing verifications failed");
                }
            }
        }
    }

    public void testForceMergeOnReadOnlyEngine() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 100);
            int numSegments;
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    engine.flush();
                    globalCheckpoint.set(i);
                }
                engine.syncTranslog();
                engine.flushAndClose();
                numSegments = engine.getLastCommittedSegmentInfos().size();
            }

            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, null, true, Function.identity(), true, randomBoolean())) {
                if (numSegments > 1) {
                    final int target = between(1, numSegments - 1);
                    UnsupportedOperationException exception = expectThrows(
                        UnsupportedOperationException.class,
                        () -> readOnlyEngine.forceMerge(true, target, false, UUIDs.randomBase64UUID())
                    );
                    assertThat(
                        exception.getMessage(),
                        equalTo(
                            "force merge is not supported on a read-only engine, "
                                + "target max number of segments["
                                + target
                                + "], current number of segments["
                                + numSegments
                                + "]."
                        )
                    );
                }

                readOnlyEngine.forceMerge(true, ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS, false, UUIDs.randomBase64UUID());
                readOnlyEngine.forceMerge(true, numSegments, false, UUIDs.randomBase64UUID());
                readOnlyEngine.forceMerge(true, numSegments + 1, false, UUIDs.randomBase64UUID());
                assertEquals(readOnlyEngine.getLastCommittedSegmentInfos().size(), numSegments);
            }
        }
    }

    public void testRecoverFromTranslogAppliesNoOperations() throws IOException {
        IOUtils.close(engine, store);
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            int numDocs = scaledRandomIntBetween(10, 1000);
            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    if (rarely()) {
                        engine.flush();
                    }
                    globalCheckpoint.set(i);
                }
                engine.syncTranslog();
                engine.flushAndClose();
            }
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, null, true, Function.identity(), true, randomBoolean())) {
                final TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), config.getIndexSettings());
                recoverFromTranslog(readOnlyEngine, translogHandler, randomNonNegativeLong());

                assertThat(translogHandler.appliedOperations(), equalTo(0L));
            }
        }
    }

    public void testTranslogStats() throws IOException {
        IOUtils.close(engine, store);
        try (Store store = createStore()) {
            final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
            EngineConfig config = config(defaultSettings, store, createTempDir(), newMergePolicy(), null, null, globalCheckpoint::get);
            final boolean softDeletesEnabled = config.getIndexSettings().isSoftDeleteEnabled();
            final int numDocs = frequently() ? scaledRandomIntBetween(10, 200) : 0;
            int uncommittedDocs = 0;

            try (InternalEngine engine = createEngine(config)) {
                for (int i = 0; i < numDocs; i++) {
                    ParsedDocument doc = testParsedDocument(Integer.toString(i), null, testDocument(), new BytesArray("{}"), null);
                    engine.index(
                        new Engine.Index(
                            newUid(doc),
                            doc,
                            i,
                            primaryTerm.get(),
                            1,
                            null,
                            Engine.Operation.Origin.REPLICA,
                            System.nanoTime(),
                            -1,
                            false,
                            SequenceNumbers.UNASSIGNED_SEQ_NO,
                            0
                        )
                    );
                    globalCheckpoint.set(i);
                    if (rarely()) {
                        engine.flush();
                        uncommittedDocs = 0;
                    } else {
                        uncommittedDocs += 1;
                    }
                }

                assertThat(
                    engine.getTranslogStats().estimatedNumberOfOperations(),
                    equalTo(softDeletesEnabled ? uncommittedDocs : numDocs)
                );
                assertThat(engine.getTranslogStats().getUncommittedOperations(), equalTo(uncommittedDocs));
                assertThat(engine.getTranslogStats().getTranslogSizeInBytes(), greaterThan(0L));
                assertThat(engine.getTranslogStats().getUncommittedSizeInBytes(), greaterThan(0L));
                assertThat(engine.getTranslogStats().getEarliestLastModifiedAge(), greaterThanOrEqualTo(0L));

                engine.flush(true, true);
            }

            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, null, true, Function.identity(), true, randomBoolean())) {
                assertThat(readOnlyEngine.getTranslogStats().estimatedNumberOfOperations(), equalTo(softDeletesEnabled ? 0 : numDocs));
                assertThat(readOnlyEngine.getTranslogStats().getUncommittedOperations(), equalTo(0));
                assertThat(readOnlyEngine.getTranslogStats().getTranslogSizeInBytes(), greaterThan(0L));
                assertThat(readOnlyEngine.getTranslogStats().getUncommittedSizeInBytes(), greaterThan(0L));
                assertThat(readOnlyEngine.getTranslogStats().getEarliestLastModifiedAge(), greaterThanOrEqualTo(0L));
            }
        }
    }

    public void testSearcherId() throws Exception {
        IOUtils.close(engine, store);
        AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        try (Store store = createStore()) {
            final EngineConfig config = config(
                defaultSettings,
                store,
                createTempDir(),
                NoMergePolicy.INSTANCE,
                null,
                null,
                globalCheckpoint::get
            );
            String lastSearcherId;
            try (InternalEngine engine = createEngine(config)) {
                lastSearcherId = ReadOnlyEngine.generateSearcherId(engine.getLastCommittedSegmentInfos());
                assertNotNull(lastSearcherId);
                int iterations = randomIntBetween(0, 10);
                for (int i = 0; i < iterations; i++) {
                    assertThat(ReadOnlyEngine.generateSearcherId(engine.getLastCommittedSegmentInfos()), equalTo(lastSearcherId));
                    final List<Engine.Operation> operations = generateHistoryOnReplica(
                        between(1, 100),
                        engine.getProcessedLocalCheckpoint() + 1L,
                        false,
                        randomBoolean(),
                        randomBoolean()
                    );
                    applyOperations(engine, operations);
                    engine.flush(randomBoolean(), true);
                    final String newCommitId = ReadOnlyEngine.generateSearcherId(engine.getLastCommittedSegmentInfos());
                    assertThat(newCommitId, not(equalTo(lastSearcherId)));
                    if (randomBoolean()) {
                        engine.flush(true, true);
                        assertThat(ReadOnlyEngine.generateSearcherId(engine.getLastCommittedSegmentInfos()), equalTo(newCommitId));
                    }
                    lastSearcherId = newCommitId;
                }
                globalCheckpoint.set(engine.getProcessedLocalCheckpoint());
            }
            try (ReadOnlyEngine readOnlyEngine = new ReadOnlyEngine(config, null, null, true, Function.identity(), true, randomBoolean())) {
                try (
                    Engine.SearcherSupplier searcher = readOnlyEngine.acquireSearcherSupplier(
                        Function.identity(),
                        randomFrom(Engine.SearcherScope.values())
                    )
                ) {
                    assertThat(searcher.getSearcherId(), equalTo(lastSearcherId));
                }
            }
        }
    }
}
