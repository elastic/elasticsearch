/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.TranslogHandler;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.index.engine.EngineTestCase.getDocIds;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasToString;

public class FollowingEngineTests extends ESTestCase {

    private ThreadPool threadPool;
    private Index index;
    private ShardId shardId;
    private AtomicLong primaryTerm = new AtomicLong();

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("following-engine-tests");
        index = new Index("index", "uuid");
        shardId = new ShardId(index, 0);
        primaryTerm.set(randomLongBetween(1, Long.MAX_VALUE));
    }

    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testFollowingEngineRejectsNonFollowingIndex() throws IOException {
        final Settings.Builder builder =
                Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.version.created", Version.CURRENT);
        if (randomBoolean()) {
            builder.put("index.xpack.ccr.following_index", false);
        }
        final Settings settings = builder.build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FollowingEngine(engineConfig));
            assertThat(e, hasToString(containsString("a following engine can not be constructed for a non-following index")));
        }
    }

    public void testIndexSeqNoIsMaintained() throws IOException {
        final long seqNo = randomIntBetween(0, Integer.MAX_VALUE);
        runIndexTest(
                seqNo,
                Engine.Operation.Origin.PRIMARY,
                (followingEngine, index) -> {
                    final Engine.IndexResult result = followingEngine.index(index);
                    assertThat(result.getSeqNo(), equalTo(seqNo));
                });
    }

    /*
     * A following engine (whether or not it is an engine for a primary or replica shard) needs to maintain ordering semantics as the
     * operations presented to it can arrive out of order (while a leader engine that is for a primary shard dictates the order). This test
     * ensures that these semantics are maintained.
     */
    public void testOutOfOrderDocuments() throws IOException {
        final Settings settings =
                Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.version.created", Version.CURRENT)
                        .put("index.xpack.ccr.following_index", true)
                        .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final VersionType versionType =
                        randomFrom(VersionType.INTERNAL, VersionType.EXTERNAL, VersionType.EXTERNAL_GTE, VersionType.FORCE);
                final List<Engine.Operation> ops = EngineTestCase.generateSingleDocHistory(true, versionType, 2, 2, 20, "id");
                ops.stream().mapToLong(op -> op.seqNo()).max().ifPresent(followingEngine::advanceMaxSeqNoOfUpdatesOrDeletes);
                EngineTestCase.assertOpsOnReplica(ops, followingEngine, true, logger);
            }
        }
    }

    public void runIndexTest(
            final long seqNo,
            final Engine.Operation.Origin origin,
            final CheckedBiConsumer<FollowingEngine, Engine.Index, IOException> consumer) throws IOException {
        final Settings settings =
                Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.version.created", Version.CURRENT)
                        .put("index.xpack.ccr.following_index", true)
                        .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final Engine.Index index = indexForFollowing("id", seqNo, origin);
                consumer.accept(followingEngine, index);
            }
        }
    }

    public void testDeleteSeqNoIsMaintained() throws IOException {
        final long seqNo = randomIntBetween(0, Integer.MAX_VALUE);
        runDeleteTest(
                seqNo,
                Engine.Operation.Origin.PRIMARY,
                (followingEngine, delete) -> {
                    followingEngine.advanceMaxSeqNoOfUpdatesOrDeletes(randomLongBetween(seqNo, Long.MAX_VALUE));
                    final Engine.DeleteResult result = followingEngine.delete(delete);
                    assertThat(result.getSeqNo(), equalTo(seqNo));
                });
    }

    public void runDeleteTest(
            final long seqNo,
            final Engine.Operation.Origin origin,
            final CheckedBiConsumer<FollowingEngine, Engine.Delete, IOException> consumer) throws IOException {
        final Settings settings =
                Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put("index.version.created", Version.CURRENT)
                        .put("index.xpack.ccr.following_index", true)
                        .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                final String id = "id";
                final Engine.Delete delete = new Engine.Delete(
                        "type",
                        id,
                        new Term("_id", id),
                        seqNo,
                        primaryTerm.get(),
                        randomNonNegativeLong(),
                        VersionType.EXTERNAL,
                        origin,
                        System.currentTimeMillis());

                consumer.accept(followingEngine, delete);
            }
        }
    }

    public void testDoNotFillSeqNoGaps() throws Exception {
        final Settings settings =
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.version.created", Version.CURRENT)
                .put("index.xpack.ccr.following_index", true)
                .build();
        final IndexMetaData indexMetaData = IndexMetaData.builder(index.getName()).settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
        try (Store store = createStore(shardId, indexSettings, newDirectory())) {
            final EngineConfig engineConfig = engineConfig(shardId, indexSettings, threadPool, store, logger, xContentRegistry());
            try (FollowingEngine followingEngine = createEngine(store, engineConfig)) {
                followingEngine.index(indexForFollowing("id", 128, Engine.Operation.Origin.PRIMARY));
                int addedNoops = followingEngine.fillSeqNoGaps(primaryTerm.get());
                assertThat(addedNoops, equalTo(0));
            }
        }
    }

    private EngineConfig engineConfig(
            final ShardId shardId,
            final IndexSettings indexSettings,
            final ThreadPool threadPool,
            final Store store,
            final Logger logger,
            final NamedXContentRegistry xContentRegistry) throws IOException {
        final IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        final Path translogPath = createTempDir("translog");
        final TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        return new EngineConfig(
                shardId,
                "allocation-id",
                threadPool,
                indexSettings,
                null,
                store,
                newMergePolicy(),
                indexWriterConfig.getAnalyzer(),
                indexWriterConfig.getSimilarity(),
                new CodecService(null, logger),
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
                () -> SequenceNumbers.NO_OPS_PERFORMED,
                () -> primaryTerm.get(),
                EngineTestCase.tombstoneDocSupplier()
        );
    }

    private static Store createStore(
            final ShardId shardId, final IndexSettings indexSettings, final Directory directory) {
        return new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
    }

    private FollowingEngine createEngine(Store store, EngineConfig config) throws IOException {
        store.createEmpty();
        final String translogUuid = Translog.createEmptyTranslog(config.getTranslogConfig().getTranslogPath(),
                SequenceNumbers.NO_OPS_PERFORMED, shardId, 1L);
        store.associateIndexWithNewTranslog(translogUuid);
        FollowingEngine followingEngine = new FollowingEngine(config);
        TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), config.getIndexSettings());
        followingEngine.initializeMaxSeqNoOfUpdatesOrDeletes();
        followingEngine.recoverFromTranslog(translogHandler, Long.MAX_VALUE);
        return followingEngine;
    }

    private Engine.Index indexForFollowing(String id, long seqNo, Engine.Operation.Origin origin) {
        final long version = randomBoolean() ? 1 : randomNonNegativeLong();
        final ParsedDocument parsedDocument = EngineTestCase.createParsedDoc(id, null);
        return new Engine.Index(EngineTestCase.newUid(parsedDocument), parsedDocument, seqNo, primaryTerm.get(), version,
            VersionType.EXTERNAL, origin, System.currentTimeMillis(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, randomBoolean());
    }

    private Engine.Index indexForPrimary(String id) {
        final ParsedDocument parsedDoc = EngineTestCase.createParsedDoc(id, null);
        return new Engine.Index(EngineTestCase.newUid(parsedDoc), primaryTerm.get(), parsedDoc);
    }

    private Engine.Delete deleteForPrimary(String id) {
        final ParsedDocument parsedDoc = EngineTestCase.createParsedDoc(id, null);
        return new Engine.Delete(parsedDoc.type(), parsedDoc.id(), EngineTestCase.newUid(parsedDoc), primaryTerm.get());
    }

    public void testBasicOptimization() throws Exception {
        runFollowTest((leader, follower) -> {
            long numDocs = between(1, 100);
            for (int i = 0; i < numDocs; i++) {
                leader.index(indexForPrimary(Integer.toString(i)));
            }
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(-1L));
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(numDocs));
            assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));

            // Do not apply optimization for deletes or updates
            for (int i = 0; i < numDocs; i++) {
                if (randomBoolean()) {
                    leader.index(indexForPrimary(Integer.toString(i)));
                } else if (randomBoolean()) {
                    leader.delete(deleteForPrimary(Integer.toString(i)));
                }
            }
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(leader.getMaxSeqNoOfUpdatesOrDeletes()));
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(numDocs));
            assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));
            // Apply optimization for documents that do not exist
            long moreDocs = between(1, 100);
            Set<String> docIds = getDocIds(follower, true).stream().map(doc -> doc.getId()).collect(Collectors.toSet());
            for (int i = 0; i < moreDocs; i++) {
                String docId = randomValueOtherThanMany(docIds::contains, () -> Integer.toString(between(1, 1000)));
                docIds.add(docId);
                leader.index(indexForPrimary(docId));
            }
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(leader.getMaxSeqNoOfUpdatesOrDeletes()));
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(numDocs + moreDocs));
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
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo((long) numOps));
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
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            final List<Engine.Operation> appendOps = new ArrayList<>();
            for (int numAppends = scaledRandomIntBetween(0, 100), i = 0; i < numAppends; i++) {
                appendOps.add(indexForPrimary("append-" + i));
            }
            EngineTestCase.concurrentlyApplyOps(appendOps, leader);
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), greaterThanOrEqualTo((long) appendOps.size()));
        });
    }

    public void testOptimizeSingleDocSequentially() throws Exception {
        runFollowTest((leader, follower) -> {
            leader.index(indexForPrimary("id"));
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(1L));

            leader.delete(deleteForPrimary("id"));
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(1L));

            leader.index(indexForPrimary("id"));
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(2L));

            leader.index(indexForPrimary("id"));
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(2L));
        });
    }

    public void testOptimizeSingleDocConcurrently() throws Exception {
        List<Engine.Operation> ops = EngineTestCase.generateSingleDocHistory(false, randomFrom(VersionType.values()), 2, 10, 500, "id");
        Randomness.shuffle(ops);
        runFollowTest((leader, follower) -> {
            EngineTestCase.concurrentlyApplyOps(ops, leader);
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));
            long numOptimized = follower.getNumberOfOptimizedIndexing();

            leader.delete(deleteForPrimary("id"));
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(numOptimized));

            leader.index(indexForPrimary("id"));
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(numOptimized + 1L));

            leader.index(indexForPrimary("id"));
            follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            assertThat(follower.getNumberOfOptimizedIndexing(), equalTo(numOptimized + 1L));
        });
    }

    private void runFollowTest(CheckedBiConsumer<InternalEngine, FollowingEngine, Exception> task) throws Exception {
        final CheckedBiConsumer<InternalEngine, FollowingEngine, Exception> wrappedTask = (leader, follower) -> {
            Thread[] threads = new Thread[between(1, 8)];
            AtomicBoolean taskIsCompleted = new AtomicBoolean();
            AtomicLong lastFetchedSeqNo = new AtomicLong(follower.getLocalCheckpoint());
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
                follower.waitForOpsToComplete(leader.getLocalCheckpoint());
            } finally {
                taskIsCompleted.set(true);
                for (Thread thread : threads) {
                    thread.join();
                }
                assertThat(follower.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(leader.getMaxSeqNoOfUpdatesOrDeletes()));
                assertThat(getDocIds(follower, true), equalTo(getDocIds(leader, true)));
            }
        };

        Settings leaderSettings = Settings.builder()
            .put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT).put("index.soft_deletes.enabled", true).build();
        IndexMetaData leaderIndexMetaData = IndexMetaData.builder(index.getName()).settings(leaderSettings).build();
        IndexSettings leaderIndexSettings = new IndexSettings(leaderIndexMetaData, leaderSettings);
        try (Store leaderStore = createStore(shardId, leaderIndexSettings, newDirectory())) {
            leaderStore.createEmpty();
            EngineConfig leaderConfig = engineConfig(shardId, leaderIndexSettings, threadPool, leaderStore, logger, xContentRegistry());
            leaderStore.associateIndexWithNewTranslog(Translog.createEmptyTranslog(
                leaderConfig.getTranslogConfig().getTranslogPath(), SequenceNumbers.NO_OPS_PERFORMED, shardId, 1L));
            try (InternalEngine leaderEngine = new InternalEngine(leaderConfig)) {
                leaderEngine.initializeMaxSeqNoOfUpdatesOrDeletes();
                leaderEngine.skipTranslogRecovery();
                Settings followerSettings = Settings.builder()
                    .put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
                    .put("index.version.created", Version.CURRENT).put("index.xpack.ccr.following_index", true).build();
                IndexMetaData followerIndexMetaData = IndexMetaData.builder(index.getName()).settings(followerSettings).build();
                IndexSettings followerIndexSettings = new IndexSettings(followerIndexMetaData, leaderSettings);
                try (Store followerStore = createStore(shardId, followerIndexSettings, newDirectory())) {
                    EngineConfig followerConfig = engineConfig(
                        shardId, followerIndexSettings, threadPool, followerStore, logger, xContentRegistry());
                    try (FollowingEngine followingEngine = createEngine(followerStore, followerConfig)) {
                        wrappedTask.accept(leaderEngine, followingEngine);
                    }
                }
            }
        }
    }

    private void fetchOperations(AtomicBoolean stopped, AtomicLong lastFetchedSeqNo,
                                 InternalEngine leader, FollowingEngine follower) throws IOException {
        final MapperService mapperService = EngineTestCase.createMapperService("test");
        final TranslogHandler translogHandler = new TranslogHandler(xContentRegistry(), follower.config().getIndexSettings());
        while (stopped.get() == false) {
            final long checkpoint = leader.getLocalCheckpoint();
            final long lastSeqNo = lastFetchedSeqNo.get();
            if (lastSeqNo < checkpoint) {
                final long nextSeqNo = randomLongBetween(lastSeqNo + 1, checkpoint);
                if (lastFetchedSeqNo.compareAndSet(lastSeqNo, nextSeqNo)) {
                    // extends the fetch range so we may deliver some overlapping operations more than once.
                    final long fromSeqNo = randomLongBetween(Math.max(lastSeqNo - 5, 0), lastSeqNo + 1);
                    final long toSeqNo = randomLongBetween(nextSeqNo, Math.min(nextSeqNo + 5, checkpoint));
                    try (Translog.Snapshot snapshot =
                             shuffleSnapshot(leader.newChangesSnapshot("test", mapperService, fromSeqNo, toSeqNo, true))) {
                        follower.advanceMaxSeqNoOfUpdatesOrDeletes(leader.getMaxSeqNoOfUpdatesOrDeletes());
                        translogHandler.run(follower, snapshot);
                    }
                }
            }
        }
    }

    private Translog.Snapshot shuffleSnapshot(Translog.Snapshot snapshot) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>(snapshot.totalOperations());
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
}
