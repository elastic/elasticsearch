/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.DocIdSeqNoAndSource;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.TranslogHandler;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.index.engine.EngineTestCase.getDocIds;
import static org.elasticsearch.index.engine.EngineTestCase.getNumVersionLookups;
import static org.elasticsearch.index.engine.EngineTestCase.getTranslog;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class FollowingEngineTests extends ESTestCase {

    private ThreadPool threadPool;
    private Index index;
    private ShardId shardId;
    private AtomicLong primaryTerm = new AtomicLong();
    private AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
    private IndexMode indexMode;
    private FieldType idFieldType;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("following-engine-tests");
        index = new Index("index", "uuid");
        shardId = new ShardId(index, 0);
        primaryTerm.set(randomLongBetween(1, Long.MAX_VALUE));
        indexMode = randomFrom(IndexMode.values());
        switch (indexMode) {
            case STANDARD:
                idFieldType = ProvidedIdFieldMapper.Defaults.FIELD_TYPE;
                break;
            case TIME_SERIES:
                idFieldType = TsidExtractingIdFieldMapper.FIELD_TYPE;
                break;
            default:
                throw new UnsupportedOperationException("Unknown index mode [" + indexMode + "]");
        }
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testFollowingEngineRejectsNonFollowingIndex() throws IOException {
        final Settings.Builder builder = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT);
        if (randomBoolean()) {
            builder.put("index.xpack.ccr.following_index", false);
        }
        final Settings settings = builder.build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store);
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FollowingEngine(engineConfig));
            assertThat(e, hasToString(containsString("a following engine can not be constructed for a non-following index")));
        }
    }

    public void testIndexSeqNoIsMaintained() throws IOException {
        final long seqNo = randomIntBetween(0, Integer.MAX_VALUE);
        runIndexTest(seqNo, Engine.Operation.Origin.PRIMARY, (followingEngine, indexToTest) -> {
            final Engine.IndexResult result = followingEngine.index(indexToTest);
            assertThat(result.getSeqNo(), equalTo(seqNo));
        });
    }

    /*
     * A following engine (whether or not it is an engine for a primary or replica shard) needs to maintain ordering semantics as the
     * operations presented to it can arrive out of order (while a leader engine that is for a primary shard dictates the order). This test
     * ensures that these semantics are maintained.
     */
    public void testOutOfOrderDocuments() throws IOException {
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put("index.xpack.ccr.following_index", true)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store);
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final VersionType versionType = randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE);
                final List<Engine.Operation> ops = EngineTestCase.generateSingleDocHistory(true, versionType, 2, 2, 20, "id");
                ops.stream().mapToLong(op -> op.seqNo()).max().ifPresent(followingEngine::advanceMaxSeqNoOfUpdatesOrDeletes);
                EngineTestCase.assertOpsOnReplica(ops, followingEngine, true, logger);
            }
        }
    }

    public void runIndexTest(
        final long seqNo,
        final Engine.Operation.Origin origin,
        final CheckedBiConsumer<FollowingEngine, Engine.Index, IOException> consumer
    ) throws IOException {
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put("index.xpack.ccr.following_index", true)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store);
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final Engine.Index indexToTest = indexForFollowing("id", seqNo, origin);
                consumer.accept(followingEngine, indexToTest);
            }
        }
    }

    public void testDeleteSeqNoIsMaintained() throws IOException {
        final long seqNo = randomIntBetween(0, Integer.MAX_VALUE);
        runDeleteTest(seqNo, Engine.Operation.Origin.PRIMARY, (followingEngine, delete) -> {
            followingEngine.advanceMaxSeqNoOfUpdatesOrDeletes(randomLongBetween(seqNo, Long.MAX_VALUE));
            final Engine.DeleteResult result = followingEngine.delete(delete);
            assertThat(result.getSeqNo(), equalTo(seqNo));
        });
    }

    public void runDeleteTest(
        final long seqNo,
        final Engine.Operation.Origin origin,
        final CheckedBiConsumer<FollowingEngine, Engine.Delete, IOException> consumer
    ) throws IOException {
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put("index.xpack.ccr.following_index", true)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store);
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final String id = "id";
                final Engine.Delete delete = new Engine.Delete(
                    id,
                    new Term("_id", id),
                    seqNo,
                    primaryTerm.get(),
                    randomNonNegativeLong(),
                    VersionType.EXTERNAL,
                    origin,
                    System.currentTimeMillis(),
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                );

                consumer.accept(followingEngine, delete);
            }
        }
    }

    public void testDoNotFillSeqNoGaps() throws Exception {
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put("index.xpack.ccr.following_index", true)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store);
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                followingEngine.index(indexForFollowing("id", 128, Engine.Operation.Origin.PRIMARY));
                int addedNoops = followingEngine.fillSeqNoGaps(primaryTerm.get());
                assertThat(addedNoops, equalTo(0));
            }
        }
    }

    private EngineConfig engineConfig(
        final ShardId shardIdValue,
        final IndexSettings indexSettings,
        final ThreadPool threadPool,
        final Store store
    ) {
        final IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        final Path translogPath = createTempDir("translog");
        final TranslogConfig translogConfig = new TranslogConfig(
            shardIdValue,
            translogPath,
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE
        );
        return new EngineConfig(
            shardIdValue,
            threadPool,
            indexSettings,
            null,
            store,
            newMergePolicy(),
            indexWriterConfig.getAnalyzer(),
            indexWriterConfig.getSimilarity(),
            new CodecService(null, BigArrays.NON_RECYCLING_INSTANCE),
            new Engine.EventListener() {
                @Override
                public void onFailedEngine(String reason, Exception e) {

                }
            },
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            translogConfig,
            TimeValue.timeValueMinutes(5),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            new NoneCircuitBreakerService(),
            globalCheckpoint::longValue,
            () -> RetentionLeases.EMPTY,
            () -> primaryTerm.get(),
            IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER,
            null
        );
    }

    private static Store createStore(final ShardId shardId, final IndexSettings indexSettings, final Directory directory) {
        return new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
    }

    private FollowingEngine createEngine(Store store, EngineConfig config) throws IOException {
        store.createEmpty();
        final String translogUuid = Translog.createEmptyTranslog(
            config.getTranslogConfig().getTranslogPath(),
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            1L
        );
        store.associateIndexWithNewTranslog(translogUuid);
        FollowingEngine followingEngine = new FollowingEngine(config);
        TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), config.getIndexSettings());
        followingEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        return followingEngine;
    }

    private Engine.Index indexForFollowing(String id, long seqNo, Engine.Operation.Origin origin) {
        final long version = randomBoolean() ? 1 : randomNonNegativeLong();
        return indexForFollowing(id, seqNo, origin, version);
    }

    private Engine.Index indexForFollowing(String id, long seqNo, Engine.Operation.Origin origin, long version) {
        final ParsedDocument parsedDocument = EngineTestCase.createParsedDoc(id, idFieldType, null);
        return new Engine.Index(
            EngineTestCase.newUid(parsedDocument),
            parsedDocument,
            seqNo,
            primaryTerm.get(),
            version,
            VersionType.EXTERNAL,
            origin,
            System.currentTimeMillis(),
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            randomBoolean(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
    }

    private Engine.Delete deleteForFollowing(String id, long seqNo, Engine.Operation.Origin origin, long version) {
        return IndexShard.prepareDelete(
            id,
            seqNo,
            primaryTerm.get(),
            version,
            VersionType.EXTERNAL,
            origin,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );
    }

    private Engine.Index indexForPrimary(String id) {
        final ParsedDocument parsedDoc = EngineTestCase.createParsedDoc(id, idFieldType, null);
        return new Engine.Index(EngineTestCase.newUid(parsedDoc), primaryTerm.get(), parsedDoc);
    }

    private Engine.Delete deleteForPrimary(String id) {
        final ParsedDocument parsedDoc = EngineTestCase.createParsedDoc(id, idFieldType, null);
        return new Engine.Delete(parsedDoc.id(), EngineTestCase.newUid(parsedDoc), primaryTerm.get());
    }

    private Engine.Result applyOperation(Engine engine, Engine.Operation op, long primaryTermValue, Engine.Operation.Origin origin)
        throws IOException {
        final VersionType versionType = origin == Engine.Operation.Origin.PRIMARY ? VersionType.EXTERNAL : null;
        final Engine.Result result;
        if (op instanceof Engine.Index engineIndex) {
            result = engine.index(
                new Engine.Index(
                    engineIndex.uid(),
                    engineIndex.parsedDoc(),
                    engineIndex.seqNo(),
                    primaryTermValue,
                    engineIndex.version(),
                    versionType,
                    origin,
                    engineIndex.startTime(),
                    engineIndex.getAutoGeneratedIdTimestamp(),
                    engineIndex.isRetry(),
                    engineIndex.getIfSeqNo(),
                    engineIndex.getIfPrimaryTerm()
                )
            );
        } else if (op instanceof Engine.Delete delete) {
            result = engine.delete(
                new Engine.Delete(
                    delete.id(),
                    delete.uid(),
                    delete.seqNo(),
                    primaryTermValue,
                    delete.version(),
                    versionType,
                    origin,
                    delete.startTime(),
                    delete.getIfSeqNo(),
                    delete.getIfPrimaryTerm()
                )
            );
        } else {
            Engine.NoOp noOp = (Engine.NoOp) op;
            result = engine.noOp(new Engine.NoOp(noOp.seqNo(), primaryTermValue, origin, noOp.startTime(), noOp.reason()));
        }
        return result;
    }

    public void testBasicOptimization() throws Exception {
        runFollowTest((leader, follower) -> {
            long numDocs = between(1, 100);
            for (int i = 0; i < numDocs; i++) {
                leader.index(indexForPrimary(Integer.toString(i)));
            }
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(-1L));
            assertThat(getNumVersionLookups(follower), equalTo(0L));
            assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));

            // Do not apply optimization for deletes or updates
            long versionLookUps = 0;
            for (int i = 0; i < numDocs; i++) {
                if (randomBoolean()) {
                    versionLookUps++;
                    leader.index(indexForPrimary(Integer.toString(i)));
                } else if (randomBoolean()) {
                    versionLookUps++;
                    leader.delete(deleteForPrimary(Integer.toString(i)));
                }
            }
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), greaterThanOrEqualTo(versionLookUps));
            assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(leader.getMaxSeqNoOfUpdatesOrDeletes()));
            assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));
            // Apply optimization for documents that do not exist
            long moreDocs = between(1, 100);
            versionLookUps = getNumVersionLookups(follower);
            Set<String> docIds = getDocIds(follower, true).stream().map(doc -> doc.id()).collect(Collectors.toSet());
            for (int i = 0; i < moreDocs; i++) {
                String docId = randomValueOtherThanMany(docIds::contains, () -> Integer.toString(between(1, 1000)));
                docIds.add(docId);
                leader.index(indexForPrimary(docId));
            }
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(leader.getMaxSeqNoOfUpdatesOrDeletes()));
            assertThat(getNumVersionLookups(follower), equalTo(versionLookUps));
            assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));
        });
    }

    public void testOptimizeAppendOnly() throws Exception {
        int numOps = scaledRandomIntBetween(1, 1000);
        List<Engine.Operation> ops = new ArrayList<>();
        for (int i = 0; i < numOps; i++) {
            ops.add(indexForPrimary(Integer.toString(i)));
        }
        runFollowTest((leader, follower) -> {
            EngineTestCase.concurrentlyApplyOps(ops, leader);
            assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(-1L));
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), equalTo(0L));
        });
    }

    public void testOptimizeMultipleVersions() throws Exception {
        List<Engine.Operation> ops = new ArrayList<>();
        for (int numOps = scaledRandomIntBetween(1, 1000), i = 0; i < numOps; i++) {
            String id = Integer.toString(between(0, 100));
            if (randomBoolean()) {
                ops.add(indexForPrimary(id));
            } else {
                ops.add(deleteForPrimary(id));
            }
        }
        Randomness.shuffle(ops);
        runFollowTest((leader, follower) -> {
            EngineTestCase.concurrentlyApplyOps(ops, leader);
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            long numVersionLookups = getNumVersionLookups(follower);
            final List<Engine.Operation> appendOps = new ArrayList<>();
            for (int numAppends = scaledRandomIntBetween(0, 100), i = 0; i < numAppends; i++) {
                appendOps.add(indexForPrimary("append-" + i));
            }
            EngineTestCase.concurrentlyApplyOps(appendOps, leader);
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), equalTo(numVersionLookups));
        });
    }

    public void testOptimizeSingleDocSequentially() throws Exception {
        runFollowTest((leader, follower) -> {
            leader.index(indexForPrimary("id"));
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), equalTo(0L));

            leader.delete(deleteForPrimary("id"));
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), equalTo(1L));

            leader.index(indexForPrimary("id"));
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), equalTo(1L));

            leader.index(indexForPrimary("id"));
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), equalTo(2L));
        });
    }

    public void testOptimizeSingleDocConcurrently() throws Exception {
        List<Engine.Operation> ops = EngineTestCase.generateSingleDocHistory(false, randomFrom(VersionType.values()), 2, 10, 500, "id");
        Randomness.shuffle(ops);
        runFollowTest((leader, follower) -> {
            EngineTestCase.concurrentlyApplyOps(ops, leader);
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));

            leader.delete(deleteForPrimary("id"));
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            long numVersionLookups = getNumVersionLookups(follower);

            leader.index(indexForPrimary("id"));
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), equalTo(numVersionLookups));

            leader.index(indexForPrimary("id"));
            EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            assertThat(getNumVersionLookups(follower), equalTo(numVersionLookups + 1L));
        });
    }

    public void testConcurrentIndexOperationsWithDeletesCanAdvanceMaxSeqNoOfUpdates() throws Exception {
        // See #72527 for more details
        Settings followerSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put("index.xpack.ccr.following_index", true)
            .build();

        IndexMetadata followerIndexMetadata = IndexMetadata.builder(index.getName()).settings(followerSettings).build();
        IndexSettings followerIndexSettings = new IndexSettings(followerIndexMetadata, Settings.EMPTY);
        try (Store followerStore = createStore(shardId, followerIndexSettings, newDirectory())) {
            EngineConfig followerConfig = engineConfig(shardId, followerIndexSettings, threadPool, followerStore);
            followerStore.createEmpty();
            String translogUuid = Translog.createEmptyTranslog(
                followerConfig.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED,
                shardId,
                1L
            );
            followerStore.associateIndexWithNewTranslog(translogUuid);
            CountDownLatch concurrentDeleteOpLatch = new CountDownLatch(1);
            final long indexNewDocWithSameIdSeqNo = 4;
            FollowingEngine followingEngine = new FollowingEngine(followerConfig) {
                @Override
                protected void advanceMaxSeqNoOfUpdatesOnPrimary(long seqNo) {
                    if (seqNo == indexNewDocWithSameIdSeqNo) {
                        // wait until the concurrent delete finishes meaning that processedLocalCheckpoint == maxSeqNoOfUpdatesOrDeletes
                        try {
                            concurrentDeleteOpLatch.await();
                            assertThat(getProcessedLocalCheckpoint(), equalTo(getMaxSeqNoOfUpdatesOrDeletes()));
                        } catch (Exception exception) {
                            throw new RuntimeException(exception);
                        }
                    }
                    super.advanceMaxSeqNoOfUpdatesOnPrimary(seqNo);
                }
            };
            TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), followerConfig.getIndexSettings());
            followingEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
            try (followingEngine) {
                final long leaderMaxSeqNoOfUpdatesOnPrimary = 3;
                followingEngine.advanceMaxSeqNoOfUpdatesOrDeletes(leaderMaxSeqNoOfUpdatesOnPrimary);

                followingEngine.index(indexForFollowing("1", 0, Engine.Operation.Origin.PRIMARY, 1));
                followingEngine.delete(deleteForFollowing("1", 1, Engine.Operation.Origin.PRIMARY, 2));
                followingEngine.index(indexForFollowing("2", 2, Engine.Operation.Origin.PRIMARY, 1));
                assertThat(followingEngine.getProcessedLocalCheckpoint(), equalTo(2L));

                CyclicBarrier barrier = new CyclicBarrier(3);
                Thread thread1 = new Thread(() -> {
                    try {
                        barrier.await();
                        followingEngine.delete(deleteForFollowing("2", 3, Engine.Operation.Origin.PRIMARY, 2));
                        concurrentDeleteOpLatch.countDown();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                Thread thread2 = new Thread(() -> {
                    try {
                        barrier.await();
                        followingEngine.index(indexForFollowing("1", indexNewDocWithSameIdSeqNo, Engine.Operation.Origin.PRIMARY, 3));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                thread1.start();
                thread2.start();
                barrier.await();
                thread1.join();
                thread2.join();

                assertThat(followingEngine.getMaxSeqNoOfUpdatesOrDeletes(), greaterThanOrEqualTo(leaderMaxSeqNoOfUpdatesOnPrimary));
            }
        }
    }

    private void runFollowTest(CheckedBiConsumer<InternalEngine, FollowingEngine, Exception> task) throws Exception {
        final CheckedBiConsumer<InternalEngine, FollowingEngine, Exception> wrappedTask = (leader, follower) -> {
            Thread[] threads = new Thread[between(1, 8)];
            AtomicBoolean taskIsCompleted = new AtomicBoolean();
            AtomicLong lastFetchedSeqNo = new AtomicLong(follower.getProcessedLocalCheckpoint());
            CountDownLatch latch = new CountDownLatch(threads.length + 1);
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        latch.countDown();
                        latch.await();
                        fetchOperations(taskIsCompleted, lastFetchedSeqNo, leader, follower);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                });
                threads[i].start();
            }
            try {
                latch.countDown();
                latch.await();
                task.accept(leader, follower);
                EngineTestCase.waitForOpsToComplete(follower, leader.getProcessedLocalCheckpoint());
            } finally {
                taskIsCompleted.set(true);
                for (Thread thread : threads) {
                    thread.join();
                }
                assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), greaterThanOrEqualTo(leader.getMaxSeqNoOfUpdatesOrDeletes()));
                assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));
                EngineTestCase.assertConsistentHistoryBetweenTranslogAndLuceneIndex(follower);
                EngineTestCase.assertAtMostOneLuceneDocumentPerSequenceNumber(follower);
            }
        };

        Settings leaderSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .build();
        IndexMetadata leaderIndexMetadata = IndexMetadata.builder(index.getName()).settings(leaderSettings).build();
        IndexSettings leaderIndexSettings = new IndexSettings(leaderIndexMetadata, leaderSettings);
        try (Store leaderStore = createStore(shardId, leaderIndexSettings, newDirectory())) {
            leaderStore.createEmpty();
            EngineConfig leaderConfig = engineConfig(shardId, leaderIndexSettings, threadPool, leaderStore);
            leaderStore.associateIndexWithNewTranslog(
                Translog.createEmptyTranslog(
                    leaderConfig.getTranslogConfig().getTranslogPath(),
                    SequenceNumbers.NO_OPS_PERFORMED,
                    shardId,
                    1L
                )
            );
            try (InternalEngine leaderEngine = new InternalEngine(leaderConfig)) {
                leaderEngine.skipTranslogRecovery();
                Settings followerSettings = Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT)
                    .put("index.xpack.ccr.following_index", true)
                    .build();
                IndexMetadata followerIndexMetadata = IndexMetadata.builder(index.getName()).settings(followerSettings).build();
                IndexSettings followerIndexSettings = new IndexSettings(followerIndexMetadata, leaderSettings);
                try (Store followerStore = createStore(shardId, followerIndexSettings, newDirectory())) {
                    EngineConfig followerConfig = engineConfig(shardId, followerIndexSettings, threadPool, followerStore);
                    try (FollowingEngine followingEngine = createEngine(followerStore, followerConfig)) {
                        wrappedTask.accept(leaderEngine, followingEngine);
                    }
                }
            }
        }
    }

    private void fetchOperations(AtomicBoolean stopped, AtomicLong lastFetchedSeqNo, InternalEngine leader, FollowingEngine follower)
        throws IOException {
        final TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), follower.config().getIndexSettings());
        while (stopped.get() == false) {
            final long checkpoint = leader.getProcessedLocalCheckpoint();
            final long lastSeqNo = lastFetchedSeqNo.get();
            if (lastSeqNo < checkpoint) {
                final long nextSeqNo = randomLongBetween(lastSeqNo + 1, checkpoint);
                if (lastFetchedSeqNo.compareAndSet(lastSeqNo, nextSeqNo)) {
                    // extends the fetch range so we may deliver some overlapping operations more than once.
                    final long fromSeqNo = randomLongBetween(Math.max(lastSeqNo - 5, 0), lastSeqNo + 1);
                    final long toSeqNo = randomLongBetween(nextSeqNo, Math.min(nextSeqNo + 5, checkpoint));
                    try (
                        Translog.Snapshot snapshot = shuffleSnapshot(
                            leader.newChangesSnapshot("test", fromSeqNo, toSeqNo, true, randomBoolean(), randomBoolean())
                        )
                    ) {
                        follower.advanceMaxSeqNoOfUpdatesOrDeletes(leader.getMaxSeqNoOfUpdatesOrDeletes());
                        Translog.Operation op;
                        while ((op = snapshot.next()) != null) {
                            EngineTestCase.applyOperation(
                                follower,
                                translogHandler.convertToEngineOp(op, randomFrom(Engine.Operation.Origin.values()))
                            );
                        }
                        follower.syncTranslog();
                    }
                }
            }
        }
    }

    private Translog.Snapshot shuffleSnapshot(Translog.Snapshot snapshot) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>();
        Translog.Operation op;
        while ((op = snapshot.next()) != null) {
            operations.add(op);
        }
        Randomness.shuffle(operations);
        final Iterator<Translog.Operation> iterator = operations.iterator();

        return new Translog.Snapshot() {
            @Override
            public int totalOperations() {
                return snapshot.totalOperations();
            }

            @Override
            public Translog.Operation next() {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }

            @Override
            public void close() throws IOException {
                snapshot.close();
            }
        };
    }

    public void testProcessOnceOnPrimary() throws Exception {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put("index.xpack.ccr.following_index", true);
        switch (indexMode) {
            case STANDARD:
                break;
            case TIME_SERIES:
                settingsBuilder.put("index.mode", "time_series").put("index.routing_path", "foo");
                break;
            default:
                throw new UnsupportedOperationException("Unknown index mode [" + indexMode + "]");
        }
        final Settings settings = settingsBuilder.build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        final CheckedBiFunction<String, Integer, ParsedDocument, IOException> nestedDocFunc = EngineTestCase.nestedParsedDocFactory();
        int numOps = between(10, 100);
        List<Engine.Operation> operations = new ArrayList<>(numOps);
        for (int i = 0; i < numOps; i++) {
            String docId = Integer.toString(between(1, 100));
            ParsedDocument doc = randomBoolean()
                ? EngineTestCase.createParsedDoc(docId, idFieldType, null)
                : nestedDocFunc.apply(docId, randomInt(3));
            if (randomBoolean()) {
                operations.add(
                    new Engine.Index(
                        EngineTestCase.newUid(doc),
                        doc,
                        i,
                        primaryTerm.get(),
                        1L,
                        VersionType.EXTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        threadPool.relativeTimeInMillis(),
                        -1,
                        true,
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0
                    )
                );
            } else if (randomBoolean()) {
                operations.add(
                    new Engine.Delete(
                        doc.id(),
                        EngineTestCase.newUid(doc),
                        i,
                        primaryTerm.get(),
                        1L,
                        VersionType.EXTERNAL,
                        Engine.Operation.Origin.PRIMARY,
                        threadPool.relativeTimeInMillis(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0
                    )
                );
            } else {
                operations.add(
                    new Engine.NoOp(i, primaryTerm.get(), Engine.Operation.Origin.PRIMARY, threadPool.relativeTimeInMillis(), "test-" + i)
                );
            }
        }
        Randomness.shuffle(operations);
        final long oldTerm = randomLongBetween(1, Integer.MAX_VALUE);
        primaryTerm.set(oldTerm);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store);
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                followingEngine.advanceMaxSeqNoOfUpdatesOrDeletes(operations.size() - 1L);
                final Map<Long, Long> operationWithTerms = new HashMap<>();
                for (Engine.Operation op : operations) {
                    long term = randomLongBetween(1, oldTerm);
                    Engine.Result result = applyOperation(followingEngine, op, term, randomFrom(Engine.Operation.Origin.values()));
                    assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
                    operationWithTerms.put(op.seqNo(), term);
                    if (rarely()) {
                        followingEngine.refresh("test");
                    }
                }
                // Primary should reject duplicates
                globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(), followingEngine.getProcessedLocalCheckpoint()));
                final long newTerm = randomLongBetween(oldTerm + 1, Long.MAX_VALUE);
                for (Engine.Operation op : operations) {
                    Engine.Result result = applyOperation(followingEngine, op, newTerm, Engine.Operation.Origin.PRIMARY);
                    assertThat(result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
                    assertThat(result.getFailure(), instanceOf(AlreadyProcessedFollowingEngineException.class));
                    AlreadyProcessedFollowingEngineException failure = (AlreadyProcessedFollowingEngineException) result.getFailure();
                    if (op.seqNo() <= globalCheckpoint.get()) {
                        assertThat(
                            "should not look-up term for operations at most the global checkpoint",
                            failure.getExistingPrimaryTerm().isPresent(),
                            equalTo(false)
                        );
                    } else {
                        assertThat(failure.getExistingPrimaryTerm().getAsLong(), equalTo(operationWithTerms.get(op.seqNo())));
                    }
                }
                for (DocIdSeqNoAndSource docId : getDocIds(followingEngine, true)) {
                    assertThat(docId.primaryTerm(), equalTo(operationWithTerms.get(docId.seqNo())));
                }
                // Replica should accept duplicates
                primaryTerm.set(newTerm);
                followingEngine.rollTranslogGeneration();
                for (Engine.Operation op : operations) {
                    Engine.Operation.Origin nonPrimary = randomValueOtherThan(
                        Engine.Operation.Origin.PRIMARY,
                        () -> randomFrom(Engine.Operation.Origin.values())
                    );
                    Engine.Result result = applyOperation(followingEngine, op, newTerm, nonPrimary);
                    assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
                }
                for (DocIdSeqNoAndSource docId : getDocIds(followingEngine, true)) {
                    assertThat(docId.primaryTerm(), equalTo(operationWithTerms.get(docId.seqNo())));
                }
            }
        }
    }

    /**
     * Test that {@link FollowingEngine#verifyEngineBeforeIndexClosing()} never fails
     * whatever the value of the global checkpoint to check is.
     */
    public void testVerifyShardBeforeIndexClosingIsNoOp() throws IOException {
        final long seqNo = randomIntBetween(0, Integer.MAX_VALUE);
        runIndexTest(seqNo, Engine.Operation.Origin.PRIMARY, (followingEngine, indexToTest) -> {
            globalCheckpoint.set(randomNonNegativeLong());
            try {
                followingEngine.verifyEngineBeforeIndexClosing();
            } catch (final IllegalStateException e) {
                fail("Following engine pre-closing verifications failed");
            }
        });
    }

    public void testMaxSeqNoInCommitUserData() throws Exception {
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put("index.xpack.ccr.following_index", true)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store);
            try (FollowingEngine engine = createEngine(store, engineConfig)) {
                AtomicBoolean running = new AtomicBoolean(true);
                Thread rollTranslog = new Thread(() -> {
                    while (running.get() && getTranslog(engine).currentFileGeneration() < 500) {
                        engine.rollTranslogGeneration(); // make adding operations to translog slower
                    }
                });
                rollTranslog.start();

                Thread indexing = new Thread(() -> {
                    List<Engine.Operation> ops = EngineTestCase.generateSingleDocHistory(true, VersionType.EXTERNAL, 2, 50, 500, "id");
                    engine.advanceMaxSeqNoOfUpdatesOrDeletes(ops.stream().mapToLong(Engine.Operation::seqNo).max().getAsLong());
                    for (Engine.Operation op : ops) {
                        if (running.get() == false) {
                            return;
                        }
                        try {
                            EngineTestCase.applyOperation(engine, op);
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                });
                indexing.start();

                int numCommits = between(5, 20);
                for (int i = 0; i < numCommits; i++) {
                    engine.flush(false, true);
                }
                running.set(false);
                indexing.join();
                rollTranslog.join();
                EngineTestCase.assertMaxSeqNoInCommitUserData(engine);
            }
        }
    }
}
