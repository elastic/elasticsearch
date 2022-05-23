/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.DocIdSeqNoAndSource;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.store.StoreUtils;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.FieldMaskingReader;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.store.MockFSDirectoryFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Assert;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.common.lucene.Lucene.cleanLuceneIndex;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends IndexShardTestCase {

    public static ShardStateMetadata load(Logger logger, Path... shardPaths) throws IOException {
        return ShardStateMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, shardPaths);
    }

    public static void write(ShardStateMetadata shardStateMetadata, Path... shardPaths) throws IOException {
        ShardStateMetadata.FORMAT.writeAndCleanup(shardStateMetadata, shardPaths);
    }

    public static Engine getEngineFromShard(IndexShard shard) {
        return shard.getEngineOrNull();
    }

    public void testWriteShardState() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            ShardId id = new ShardId("foo", "fooUUID", 1);
            boolean primary = randomBoolean();
            AllocationId allocationId = randomBoolean() ? null : randomAllocationId();
            ShardStateMetadata state1 = new ShardStateMetadata(primary, "fooUUID", allocationId);
            write(state1, env.availableShardPaths(id));
            ShardStateMetadata shardStateMetadata = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetadata, state1);

            ShardStateMetadata state2 = new ShardStateMetadata(primary, "fooUUID", allocationId);
            write(state2, env.availableShardPaths(id));
            shardStateMetadata = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetadata, state1);

            ShardStateMetadata state3 = new ShardStateMetadata(primary, "fooUUID", allocationId);
            write(state3, env.availableShardPaths(id));
            shardStateMetadata = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetadata, state3);
            assertEquals("fooUUID", state3.indexUUID);
        }
    }

    public void testPersistenceStateMetadataPersistence() throws Exception {
        IndexShard shard = newStartedShard();
        final Path shardStatePath = shard.shardPath().getShardStatePath();
        ShardStateMetadata shardStateMetadata = load(logger, shardStatePath);
        assertEquals(getShardStateMetadata(shard), shardStateMetadata);
        ShardRouting routing = shard.shardRouting;
        IndexShardTestCase.updateRoutingEntry(shard, routing);

        shardStateMetadata = load(logger, shardStatePath);
        assertEquals(shardStateMetadata, getShardStateMetadata(shard));
        assertEquals(
            shardStateMetadata,
            new ShardStateMetadata(routing.primary(), shard.indexSettings().getUUID(), routing.allocationId())
        );

        routing = TestShardRouting.relocate(shard.shardRouting, "some node", 42L);
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        shardStateMetadata = load(logger, shardStatePath);
        assertEquals(shardStateMetadata, getShardStateMetadata(shard));
        assertEquals(
            shardStateMetadata,
            new ShardStateMetadata(routing.primary(), shard.indexSettings().getUUID(), routing.allocationId())
        );
        closeShards(shard);
    }

    public void testFailShard() throws Exception {
        allowShardFailures();
        IndexShard shard = newStartedShard();
        final ShardPath shardPath = shard.shardPath();
        assertNotNull(shardPath);
        // fail shard
        shard.failShard("test shard fail", new CorruptIndexException("", ""));
        shard.close("do not assert history", false);
        shard.store().close();
        // check state file still exists
        ShardStateMetadata shardStateMetadata = load(logger, shardPath.getShardStatePath());
        assertEquals(shardStateMetadata, getShardStateMetadata(shard));
        // but index can't be opened for a failed shard
        assertThat(
            "store index should be corrupted",
            StoreUtils.canOpenIndex(
                logger,
                shardPath.resolveIndex(),
                shard.shardId(),
                (shardId, lockTimeoutMS, details) -> new DummyShardLock(shardId)
            ),
            equalTo(false)
        );
    }

    ShardStateMetadata getShardStateMetadata(IndexShard shard) {
        ShardRouting shardRouting = shard.routingEntry();
        if (shardRouting == null) {
            return null;
        } else {
            return new ShardStateMetadata(shardRouting.primary(), shard.indexSettings().getUUID(), shardRouting.allocationId());
        }
    }

    private AllocationId randomAllocationId() {
        AllocationId allocationId = AllocationId.newInitializing();
        if (randomBoolean()) {
            allocationId = AllocationId.newRelocation(allocationId);
        }
        return allocationId;
    }

    public void testShardStateMetaHashCodeEquals() {
        AllocationId allocationId = randomBoolean() ? null : randomAllocationId();
        ShardStateMetadata meta = new ShardStateMetadata(
            randomBoolean(),
            randomRealisticUnicodeOfCodepointLengthBetween(1, 10),
            allocationId
        );

        assertEquals(meta, new ShardStateMetadata(meta.primary, meta.indexUUID, meta.allocationId));
        assertEquals(meta.hashCode(), new ShardStateMetadata(meta.primary, meta.indexUUID, meta.allocationId).hashCode());

        assertFalse(meta.equals(new ShardStateMetadata(meta.primary == false, meta.indexUUID, meta.allocationId)));
        assertFalse(meta.equals(new ShardStateMetadata(meta.primary == false, meta.indexUUID + "foo", meta.allocationId)));
        assertFalse(meta.equals(new ShardStateMetadata(meta.primary == false, meta.indexUUID + "foo", randomAllocationId())));
        Set<Integer> hashCodes = new HashSet<>();
        for (int i = 0; i < 30; i++) { // just a sanity check that we impl hashcode
            allocationId = randomBoolean() ? null : randomAllocationId();
            meta = new ShardStateMetadata(randomBoolean(), randomRealisticUnicodeOfCodepointLengthBetween(1, 10), allocationId);
            hashCodes.add(meta.hashCode());
        }
        assertTrue("more than one unique hashcode expected but got: " + hashCodes.size(), hashCodes.size() > 1);

    }

    public void testClosesPreventsNewOperations() throws Exception {
        IndexShard indexShard = newStartedShard();
        closeShards(indexShard);
        assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
        expectThrows(IndexShardClosedException.class, () -> indexShard.acquirePrimaryOperationPermit(null, ThreadPool.Names.WRITE, ""));
        expectThrows(
            IndexShardClosedException.class,
            () -> indexShard.acquireAllPrimaryOperationsPermits(null, TimeValue.timeValueSeconds(30L))
        );
        expectThrows(
            IndexShardClosedException.class,
            () -> indexShard.acquireReplicaOperationPermit(
                indexShard.getPendingPrimaryTerm(),
                UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(),
                null,
                ThreadPool.Names.WRITE,
                ""
            )
        );
        expectThrows(
            IndexShardClosedException.class,
            () -> indexShard.acquireAllReplicaOperationsPermits(
                indexShard.getPendingPrimaryTerm(),
                UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(),
                null,
                TimeValue.timeValueSeconds(30L)
            )
        );
    }

    public void testRunUnderPrimaryPermitRunsUnderPrimaryPermit() throws IOException {
        final IndexShard indexShard = newStartedShard(true);
        try {
            assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
            indexShard.runUnderPrimaryPermit(
                () -> assertThat(indexShard.getActiveOperationsCount(), equalTo(1)),
                e -> fail(e.toString()),
                ThreadPool.Names.SAME,
                "test"
            );
            assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
        } finally {
            closeShards(indexShard);
        }
    }

    public void testRunUnderPrimaryPermitOnFailure() throws IOException {
        final IndexShard indexShard = newStartedShard(true);
        final AtomicBoolean invoked = new AtomicBoolean();
        try {
            indexShard.runUnderPrimaryPermit(() -> { throw new RuntimeException("failure"); }, e -> {
                assertThat(e, instanceOf(RuntimeException.class));
                assertThat(e.getMessage(), equalTo("failure"));
                invoked.set(true);
            }, ThreadPool.Names.SAME, "test");
            assertTrue(invoked.get());
        } finally {
            closeShards(indexShard);
        }
    }

    public void testRunUnderPrimaryPermitDelaysToExecutorWhenBlocked() throws Exception {
        final IndexShard indexShard = newStartedShard(true);
        try {
            final PlainActionFuture<Releasable> onAcquired = new PlainActionFuture<>();
            indexShard.acquireAllPrimaryOperationsPermits(onAcquired, new TimeValue(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
            final Releasable permit = onAcquired.actionGet();
            final CountDownLatch latch = new CountDownLatch(1);
            final String executorOnDelay = randomFrom(
                ThreadPool.Names.FLUSH,
                ThreadPool.Names.GENERIC,
                ThreadPool.Names.MANAGEMENT,
                ThreadPool.Names.SAME
            );
            indexShard.runUnderPrimaryPermit(() -> {
                final String expectedThreadPoolName = executorOnDelay.equals(ThreadPool.Names.SAME)
                    ? "generic"
                    : executorOnDelay.toLowerCase(Locale.ROOT);
                assertThat(Thread.currentThread().getName(), containsString(expectedThreadPoolName));
                latch.countDown();
            }, e -> fail(e.toString()), executorOnDelay, "test");
            permit.close();
            latch.await();
            // we could race and assert on the count before the permit is returned
            assertBusy(() -> assertThat(indexShard.getActiveOperationsCount(), equalTo(0)));
        } finally {
            closeShards(indexShard);
        }
    }

    public void testRejectOperationPermitWithHigherTermWhenNotStarted() throws IOException {
        IndexShard indexShard = newShard(false);
        expectThrows(
            IndexShardNotStartedException.class,
            () -> randomReplicaOperationPermitAcquisition(
                indexShard,
                indexShard.getPendingPrimaryTerm() + randomIntBetween(1, 100),
                UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(),
                PlainActionFuture.newFuture(),
                ""
            )
        );
        closeShards(indexShard);
    }

    public void testPrimaryPromotionDelaysOperations() throws IOException, BrokenBarrierException, InterruptedException {
        final IndexShard indexShard = newShard(false);
        recoveryEmptyReplica(indexShard, randomBoolean());

        final int operations = scaledRandomIntBetween(1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(1 + operations);
        final CountDownLatch latch = new CountDownLatch(operations);
        final CountDownLatch operationLatch = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < operations; i++) {
            final String id = "t_" + i;
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                indexShard.acquireReplicaOperationPermit(
                    indexShard.getPendingPrimaryTerm(),
                    indexShard.getLastKnownGlobalCheckpoint(),
                    indexShard.getMaxSeqNoOfUpdatesOrDeletes(),
                    new ActionListener<Releasable>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            latch.countDown();
                            try {
                                operationLatch.await();
                            } catch (final InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            releasable.close();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    ThreadPool.Names.WRITE,
                    id
                );
            });
            thread.start();
            threads.add(thread);
        }

        barrier.await();
        latch.await();

        final ShardRouting replicaRouting = indexShard.routingEntry();
        promoteReplica(
            indexShard,
            Collections.singleton(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(replicaRouting).build()
        );

        final int delayedOperations = scaledRandomIntBetween(1, 64);
        final CyclicBarrier delayedOperationsBarrier = new CyclicBarrier(1 + delayedOperations);
        final CountDownLatch delayedOperationsLatch = new CountDownLatch(delayedOperations);
        final AtomicLong counter = new AtomicLong();
        final List<Thread> delayedThreads = new ArrayList<>();
        for (int i = 0; i < delayedOperations; i++) {
            final String id = "d_" + i;
            final Thread thread = new Thread(() -> {
                try {
                    delayedOperationsBarrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                indexShard.acquirePrimaryOperationPermit(new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        counter.incrementAndGet();
                        releasable.close();
                        delayedOperationsLatch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new RuntimeException(e);
                    }
                }, ThreadPool.Names.WRITE, id);
            });
            thread.start();
            delayedThreads.add(thread);
        }

        delayedOperationsBarrier.await();

        assertThat(counter.get(), equalTo(0L));

        operationLatch.countDown();
        for (final Thread thread : threads) {
            thread.join();
        }

        delayedOperationsLatch.await();

        assertThat(counter.get(), equalTo((long) delayedOperations));

        for (final Thread thread : delayedThreads) {
            thread.join();
        }

        closeShards(indexShard);
    }

    /**
     * This test makes sure that people can use the shard routing entry + take an operation permit to check whether a shard was already
     * promoted to a primary.
     */
    public void testPublishingOrderOnPromotion() throws IOException, InterruptedException, BrokenBarrierException {
        final IndexShard indexShard = newShard(false);
        recoveryEmptyReplica(indexShard, randomBoolean());
        final long promotedTerm = indexShard.getPendingPrimaryTerm() + 1;
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean stop = new AtomicBoolean();
        final Thread thread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (stop.get() == false) {
                if (indexShard.routingEntry().primary()) {
                    assertThat(indexShard.getPendingPrimaryTerm(), equalTo(promotedTerm));
                    final PlainActionFuture<Releasable> permitAcquiredFuture = new PlainActionFuture<>();
                    indexShard.acquirePrimaryOperationPermit(permitAcquiredFuture, ThreadPool.Names.SAME, "bla");
                    try (Releasable ignored = permitAcquiredFuture.actionGet()) {
                        assertThat(indexShard.getReplicationGroup(), notNullValue());
                    }
                }
            }
        });
        thread.start();

        barrier.await();
        final ShardRouting replicaRouting = indexShard.routingEntry();
        promoteReplica(
            indexShard,
            Collections.singleton(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(replicaRouting).build()
        );

        stop.set(true);
        thread.join();
        closeShards(indexShard);
    }

    public void testPrimaryFillsSeqNoGapsOnPromotion() throws Exception {
        final IndexShard indexShard = newShard(false);
        recoveryEmptyReplica(indexShard, randomBoolean());

        // most of the time this is large enough that most of the time there will be at least one gap
        final int operations = 1024 - scaledRandomIntBetween(0, 1024);
        final Result result = indexOnReplicaWithGaps(indexShard, operations, Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED));

        final int maxSeqNo = result.maxSeqNo;

        // promote the replica
        final ShardRouting replicaRouting = indexShard.routingEntry();
        promoteReplica(
            indexShard,
            Collections.singleton(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(replicaRouting).build()
        );

        /*
         * This operation completing means that the delay operation executed as part of increasing the primary term has completed and the
         * gaps are filled.
         */
        final CountDownLatch latch = new CountDownLatch(1);
        indexShard.acquirePrimaryOperationPermit(new ActionListener<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                releasable.close();
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        }, ThreadPool.Names.GENERIC, "");

        latch.await();
        assertThat(indexShard.getLocalCheckpoint(), equalTo((long) maxSeqNo));
        closeShards(indexShard);
    }

    public void testPrimaryPromotionRollsGeneration() throws Exception {
        final IndexShard indexShard = newStartedShard(false);

        final long currentTranslogGeneration = getTranslog(indexShard).getGeneration().translogFileGeneration;

        // promote the replica
        final ShardRouting replicaRouting = indexShard.routingEntry();
        final long newPrimaryTerm = indexShard.getPendingPrimaryTerm() + between(1, 10000);
        final ShardRouting primaryRouting = newShardRouting(
            replicaRouting.shardId(),
            replicaRouting.currentNodeId(),
            null,
            true,
            ShardRoutingState.STARTED,
            replicaRouting.allocationId()
        );
        indexShard.updateShardState(
            primaryRouting,
            newPrimaryTerm,
            (shard, listener) -> {},
            0L,
            Collections.singleton(primaryRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(primaryRouting.shardId()).addShard(primaryRouting).build()
        );

        /*
         * This operation completing means that the delay operation executed as part of increasing the primary term has completed and the
         * translog generation has rolled.
         */
        final CountDownLatch latch = new CountDownLatch(1);
        indexShard.acquirePrimaryOperationPermit(new ActionListener<Releasable>() {
            @Override
            public void onResponse(Releasable releasable) {
                releasable.close();
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        }, ThreadPool.Names.GENERIC, "");

        latch.await();
        assertThat(getTranslog(indexShard).getGeneration().translogFileGeneration, equalTo(currentTranslogGeneration + 1));
        assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard)), equalTo(newPrimaryTerm));

        closeShards(indexShard);
    }

    public void testOperationPermitsOnPrimaryShards() throws Exception {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final IndexShard indexShard;

        final boolean isPrimaryMode;
        if (randomBoolean()) {
            // relocation target
            indexShard = newShard(
                newShardRouting(
                    shardId,
                    "local_node",
                    "other node",
                    true,
                    ShardRoutingState.INITIALIZING,
                    AllocationId.newRelocation(AllocationId.newInitializing())
                )
            );
            assertEquals(0, indexShard.getActiveOperationsCount());
            isPrimaryMode = false;
        } else if (randomBoolean()) {
            // simulate promotion
            indexShard = newStartedShard(false);
            ShardRouting replicaRouting = indexShard.routingEntry();
            ShardRouting primaryRouting = newShardRouting(
                replicaRouting.shardId(),
                replicaRouting.currentNodeId(),
                null,
                true,
                ShardRoutingState.STARTED,
                replicaRouting.allocationId()
            );
            final long newPrimaryTerm = indexShard.getPendingPrimaryTerm() + between(1, 1000);
            CountDownLatch latch = new CountDownLatch(1);
            indexShard.updateShardState(primaryRouting, newPrimaryTerm, (shard, listener) -> {
                assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard)), equalTo(newPrimaryTerm));
                latch.countDown();
            },
                0L,
                Collections.singleton(indexShard.routingEntry().allocationId().getId()),
                new IndexShardRoutingTable.Builder(indexShard.shardId()).addShard(primaryRouting).build()
            );
            latch.await();
            assertThat(indexShard.getActiveOperationsCount(), is(oneOf(0, IndexShard.OPERATIONS_BLOCKED)));
            if (randomBoolean()) {
                assertBusy(() -> assertEquals(0, indexShard.getActiveOperationsCount()));
            }
            isPrimaryMode = true;
        } else {
            indexShard = newStartedShard(true);
            assertEquals(0, indexShard.getActiveOperationsCount());
            isPrimaryMode = true;
        }
        assert indexShard.getReplicationTracker().isPrimaryMode() == isPrimaryMode;
        final long pendingPrimaryTerm = indexShard.getPendingPrimaryTerm();
        if (isPrimaryMode) {
            Releasable operation1 = acquirePrimaryOperationPermitBlockingly(indexShard);
            assertEquals(1, indexShard.getActiveOperationsCount());
            Releasable operation2 = acquirePrimaryOperationPermitBlockingly(indexShard);
            assertEquals(2, indexShard.getActiveOperationsCount());

            Releasables.close(operation1, operation2);
            assertEquals(0, indexShard.getActiveOperationsCount());
        } else {
            indexShard.acquirePrimaryOperationPermit(new ActionListener<>() {
                @Override
                public void onResponse(final Releasable releasable) {
                    throw new AssertionError();
                }

                @Override
                public void onFailure(final Exception e) {
                    assertThat(e, instanceOf(ShardNotInPrimaryModeException.class));
                    assertThat(e, hasToString(containsString("shard is not in primary mode")));
                }
            }, ThreadPool.Names.SAME, "test");

            final CountDownLatch latch = new CountDownLatch(1);
            indexShard.acquireAllPrimaryOperationsPermits(new ActionListener<>() {
                @Override
                public void onResponse(final Releasable releasable) {
                    throw new AssertionError();
                }

                @Override
                public void onFailure(final Exception e) {
                    assertThat(e, instanceOf(ShardNotInPrimaryModeException.class));
                    assertThat(e, hasToString(containsString("shard is not in primary mode")));
                    latch.countDown();
                }
            }, TimeValue.timeValueSeconds(30));
            latch.await();
        }

        if (Assertions.ENABLED && indexShard.routingEntry().isRelocationTarget() == false) {
            assertThat(
                expectThrows(
                    AssertionError.class,
                    () -> indexShard.acquireReplicaOperationPermit(
                        pendingPrimaryTerm,
                        indexShard.getLastKnownGlobalCheckpoint(),
                        indexShard.getMaxSeqNoOfUpdatesOrDeletes(),
                        new ActionListener<Releasable>() {
                            @Override
                            public void onResponse(Releasable releasable) {
                                fail();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                fail();
                            }
                        },
                        ThreadPool.Names.WRITE,
                        ""
                    )
                ).getMessage(),
                containsString("in primary mode cannot be a replication target")
            );
        }

        closeShards(indexShard);
    }

    public void testAcquirePrimaryAllOperationsPermits() throws Exception {
        final IndexShard indexShard = newStartedShard(true);
        assertEquals(0, indexShard.getActiveOperationsCount());

        final CountDownLatch allPermitsAcquired = new CountDownLatch(1);

        final Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final List<PlainActionFuture<Releasable>> futures = new ArrayList<>(threads.length);
        final AtomicArray<Tuple<Boolean, Exception>> results = new AtomicArray<>(threads.length);
        final CountDownLatch allOperationsDone = new CountDownLatch(threads.length);

        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            final boolean singlePermit = randomBoolean();

            final PlainActionFuture<Releasable> future = new PlainActionFuture<Releasable>() {
                @Override
                public void onResponse(final Releasable releasable) {
                    if (singlePermit) {
                        assertThat(indexShard.getActiveOperationsCount(), greaterThan(0));
                    } else {
                        assertThat(indexShard.getActiveOperationsCount(), equalTo(IndexShard.OPERATIONS_BLOCKED));
                    }
                    releasable.close();
                    super.onResponse(releasable);
                    results.setOnce(threadId, Tuple.tuple(Boolean.TRUE, null));
                    allOperationsDone.countDown();
                }

                @Override
                public void onFailure(final Exception e) {
                    results.setOnce(threadId, Tuple.tuple(Boolean.FALSE, e));
                    allOperationsDone.countDown();
                }
            };
            futures.add(threadId, future);

            threads[threadId] = new Thread(() -> {
                try {
                    allPermitsAcquired.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (singlePermit) {
                    indexShard.acquirePrimaryOperationPermit(future, ThreadPool.Names.WRITE, "");
                } else {
                    indexShard.acquireAllPrimaryOperationsPermits(future, TimeValue.timeValueHours(1L));
                }
            });
            threads[threadId].start();
        }

        final AtomicBoolean blocked = new AtomicBoolean();
        final CountDownLatch allPermitsTerminated = new CountDownLatch(1);

        final PlainActionFuture<Releasable> futureAllPermits = new PlainActionFuture<Releasable>() {
            @Override
            public void onResponse(final Releasable releasable) {
                try {
                    blocked.set(true);
                    allPermitsAcquired.countDown();
                    super.onResponse(releasable);
                    allPermitsTerminated.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        indexShard.acquireAllPrimaryOperationsPermits(futureAllPermits, TimeValue.timeValueSeconds(30L));
        allPermitsAcquired.await();
        assertTrue(blocked.get());
        assertEquals(IndexShard.OPERATIONS_BLOCKED, indexShard.getActiveOperationsCount());
        assertTrue("Expected no results, operations are blocked", results.asList().isEmpty());
        futures.forEach(future -> assertFalse(future.isDone()));

        allPermitsTerminated.countDown();

        final Releasable allPermits = futureAllPermits.get();
        assertTrue(futureAllPermits.isDone());

        assertTrue("Expected no results, operations are blocked", results.asList().isEmpty());
        futures.forEach(future -> assertFalse(future.isDone()));

        Releasables.close(allPermits);
        allOperationsDone.await();
        for (Thread thread : threads) {
            thread.join();
        }

        futures.forEach(future -> assertTrue(future.isDone()));
        assertEquals(threads.length, results.asList().size());
        results.asList().forEach(result -> {
            assertTrue(result.v1());
            assertNull(result.v2());
        });

        closeShards(indexShard);
    }

    private Releasable acquirePrimaryOperationPermitBlockingly(IndexShard indexShard) throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquirePrimaryOperationPermit(fut, ThreadPool.Names.WRITE, "");
        return fut.get();
    }

    private Releasable acquireReplicaOperationPermitBlockingly(IndexShard indexShard, long opPrimaryTerm) throws ExecutionException,
        InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquireReplicaOperationPermit(
            opPrimaryTerm,
            indexShard.getLastKnownGlobalCheckpoint(),
            randomNonNegativeLong(),
            fut,
            ThreadPool.Names.WRITE,
            ""
        );
        return fut.get();
    }

    public void testOperationPermitOnReplicaShards() throws Exception {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final IndexShard indexShard;
        final boolean engineClosed;
        switch (randomInt(2)) {
            case 0 -> {
                // started replica
                indexShard = newStartedShard(false);
                engineClosed = false;
            }
            case 1 -> {
                // initializing replica / primary
                final boolean relocating = randomBoolean();
                ShardRouting routing = newShardRouting(
                    shardId,
                    "local_node",
                    relocating ? "sourceNode" : null,
                    relocating ? randomBoolean() : false,
                    ShardRoutingState.INITIALIZING,
                    relocating ? AllocationId.newRelocation(AllocationId.newInitializing()) : AllocationId.newInitializing()
                );
                indexShard = newShard(routing);
                engineClosed = true;
            }
            case 2 -> {
                // relocation source
                indexShard = newStartedShard(true);
                ShardRouting routing = indexShard.routingEntry();
                final ShardRouting newRouting = newShardRouting(
                    routing.shardId(),
                    routing.currentNodeId(),
                    "otherNode",
                    true,
                    ShardRoutingState.RELOCATING,
                    AllocationId.newRelocation(routing.allocationId())
                );
                IndexShardTestCase.updateRoutingEntry(indexShard, newRouting);
                blockingCallRelocated(indexShard, newRouting, (primaryContext, listener) -> listener.onResponse(null));
                engineClosed = false;
            }
            default -> throw new UnsupportedOperationException("get your numbers straight");
        }
        final ShardRouting shardRouting = indexShard.routingEntry();
        logger.info("shard routing to {}", shardRouting);

        assertEquals(0, indexShard.getActiveOperationsCount());
        if (shardRouting.primary() == false && Assertions.ENABLED) {
            AssertionError e = expectThrows(
                AssertionError.class,
                () -> indexShard.acquirePrimaryOperationPermit(null, ThreadPool.Names.WRITE, "")
            );
            assertThat(e, hasToString(containsString("acquirePrimaryOperationPermit should only be called on primary shard")));

            e = expectThrows(
                AssertionError.class,
                () -> indexShard.acquireAllPrimaryOperationsPermits(null, TimeValue.timeValueSeconds(30L))
            );
            assertThat(e, hasToString(containsString("acquireAllPrimaryOperationsPermits should only be called on primary shard")));
        }

        final long primaryTerm = indexShard.getPendingPrimaryTerm();
        final long translogGen = engineClosed ? -1 : getTranslog(indexShard).getGeneration().translogFileGeneration;

        final Releasable operation1;
        final Releasable operation2;
        if (engineClosed == false) {
            operation1 = acquireReplicaOperationPermitBlockingly(indexShard, primaryTerm);
            assertEquals(1, indexShard.getActiveOperationsCount());
            operation2 = acquireReplicaOperationPermitBlockingly(indexShard, primaryTerm);
            assertEquals(2, indexShard.getActiveOperationsCount());
        } else {
            operation1 = null;
            operation2 = null;
        }

        {
            final AtomicBoolean onResponse = new AtomicBoolean();
            final AtomicReference<Exception> onFailure = new AtomicReference<>();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final long newPrimaryTerm = primaryTerm + 1 + randomInt(20);
            if (engineClosed == false) {
                assertThat(indexShard.getLocalCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
                assertThat(indexShard.getLastKnownGlobalCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
            }
            final long newGlobalCheckPoint;
            if (engineClosed || randomBoolean()) {
                newGlobalCheckPoint = SequenceNumbers.NO_OPS_PERFORMED;
            } else {
                long localCheckPoint = indexShard.getLastKnownGlobalCheckpoint() + randomInt(100);
                // advance local checkpoint
                for (int i = 0; i <= localCheckPoint; i++) {
                    indexShard.markSeqNoAsNoop(i, indexShard.getOperationPrimaryTerm(), "dummy doc");
                }
                indexShard.sync(); // advance local checkpoint
                newGlobalCheckPoint = randomIntBetween((int) indexShard.getLastKnownGlobalCheckpoint(), (int) localCheckPoint);
            }
            final long expectedLocalCheckpoint;
            if (newGlobalCheckPoint == UNASSIGNED_SEQ_NO) {
                expectedLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
            } else {
                expectedLocalCheckpoint = newGlobalCheckPoint;
            }
            // but you can not increment with a new primary term until the operations on the older primary term complete
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ActionListener<Releasable> listener = new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        assertThat(indexShard.getPendingPrimaryTerm(), equalTo(newPrimaryTerm));
                        assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard)), equalTo(newPrimaryTerm));
                        assertThat(indexShard.getLocalCheckpoint(), equalTo(expectedLocalCheckpoint));
                        assertThat(indexShard.getLastKnownGlobalCheckpoint(), equalTo(newGlobalCheckPoint));
                        onResponse.set(true);
                        releasable.close();
                        finish();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailure.set(e);
                        finish();
                    }

                    private void finish() {
                        try {
                            barrier.await();
                        } catch (final BrokenBarrierException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                try {
                    randomReplicaOperationPermitAcquisition(
                        indexShard,
                        newPrimaryTerm,
                        newGlobalCheckPoint,
                        randomNonNegativeLong(),
                        listener,
                        ""
                    );
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
            thread.start();
            barrier.await();
            if (indexShard.state() == IndexShardState.CREATED || indexShard.state() == IndexShardState.RECOVERING) {
                barrier.await();
                assertThat(indexShard.getPendingPrimaryTerm(), equalTo(primaryTerm));
                assertFalse(onResponse.get());
                assertThat(onFailure.get(), instanceOf(IndexShardNotStartedException.class));
                Releasables.close(operation1);
                Releasables.close(operation2);
            } else {
                // our operation should be blocked until the previous operations complete
                assertFalse(onResponse.get());
                assertNull(onFailure.get());
                assertThat(indexShard.getOperationPrimaryTerm(), equalTo(primaryTerm));
                assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard)), equalTo(primaryTerm));
                Releasables.close(operation1);
                // our operation should still be blocked
                assertFalse(onResponse.get());
                assertNull(onFailure.get());
                assertThat(indexShard.getOperationPrimaryTerm(), equalTo(primaryTerm));
                assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard)), equalTo(primaryTerm));
                Releasables.close(operation2);
                barrier.await();
                // now lock acquisition should have succeeded
                assertThat(indexShard.getOperationPrimaryTerm(), equalTo(newPrimaryTerm));
                assertThat(indexShard.getPendingPrimaryTerm(), equalTo(newPrimaryTerm));
                assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard)), equalTo(newPrimaryTerm));
                if (engineClosed) {
                    assertFalse(onResponse.get());
                    assertThat(onFailure.get(), instanceOf(AlreadyClosedException.class));
                } else {
                    assertTrue(onResponse.get());
                    assertNull(onFailure.get());
                    assertThat(
                        getTranslog(indexShard).getGeneration().translogFileGeneration,
                        // if rollback happens we roll translog twice: one when we flush a commit before opening a read-only engine
                        // and one after replaying translog (upto the global checkpoint); otherwise we roll translog once.
                        either(equalTo(translogGen + 1)).or(equalTo(translogGen + 2))
                    );
                    assertThat(indexShard.getLocalCheckpoint(), equalTo(expectedLocalCheckpoint));
                    assertThat(indexShard.getLastKnownGlobalCheckpoint(), equalTo(newGlobalCheckPoint));
                }
            }
            thread.join();
            assertEquals(0, indexShard.getActiveOperationsCount());
        }

        {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicBoolean onResponse = new AtomicBoolean();
            final AtomicBoolean onFailure = new AtomicBoolean();
            final AtomicReference<Exception> onFailureException = new AtomicReference<>();
            ActionListener<Releasable> onLockAcquired = new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    onResponse.set(true);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    onFailure.set(true);
                    onFailureException.set(e);
                    latch.countDown();
                }
            };

            final long oldPrimaryTerm = indexShard.getPendingPrimaryTerm() - 1;
            randomReplicaOperationPermitAcquisition(
                indexShard,
                oldPrimaryTerm,
                indexShard.getLastKnownGlobalCheckpoint(),
                randomNonNegativeLong(),
                onLockAcquired,
                ""
            );
            latch.await();
            assertFalse(onResponse.get());
            assertTrue(onFailure.get());
            assertThat(onFailureException.get(), instanceOf(IllegalStateException.class));
            assertThat(onFailureException.get(), hasToString(containsString("operation primary term [" + oldPrimaryTerm + "] is too old")));
        }

        closeShard(indexShard, false); // skip asserting translog and Lucene as we rolled back Lucene but did not execute resync
    }

    public void testAcquireReplicaPermitAdvanceMaxSeqNoOfUpdates() throws Exception {
        IndexShard replica = newStartedShard(false);
        assertThat(replica.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        long currentMaxSeqNoOfUpdates = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        replica.advanceMaxSeqNoOfUpdatesOrDeletes(currentMaxSeqNoOfUpdates);

        long newMaxSeqNoOfUpdates = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        randomReplicaOperationPermitAcquisition(
            replica,
            replica.getOperationPrimaryTerm(),
            replica.getLastKnownGlobalCheckpoint(),
            newMaxSeqNoOfUpdates,
            fut,
            ""
        );
        try (Releasable ignored = fut.actionGet()) {
            assertThat(replica.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(Math.max(currentMaxSeqNoOfUpdates, newMaxSeqNoOfUpdates)));
        }
        closeShards(replica);
    }

    public void testGlobalCheckpointSync() throws IOException {
        // create the primary shard with a callback that sets a boolean when the global checkpoint sync is invoked
        final ShardId shardId = new ShardId("index", "_na_", 0);
        final ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(8),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder(shardRouting.getIndexName()).settings(settings).primaryTerm(0, 1);
        final AtomicBoolean synced = new AtomicBoolean();
        final IndexShard primaryShard = newShard(
            shardRouting,
            indexMetadata.build(),
            null,
            new InternalEngineFactory(),
            () -> synced.set(true),
            RetentionLeaseSyncer.EMPTY
        );
        // add a replica
        recoverShardFromStore(primaryShard);
        final IndexShard replicaShard = newShard(shardId, false);
        recoverReplica(replicaShard, primaryShard, true);
        final int maxSeqNo = randomIntBetween(0, 128);
        for (int i = 0; i <= maxSeqNo; i++) {
            EngineTestCase.generateNewSeqNo(primaryShard.getEngine());
        }
        final long checkpoint = rarely() ? maxSeqNo - scaledRandomIntBetween(0, maxSeqNo) : maxSeqNo;

        // set up local checkpoints on the shard copies
        primaryShard.updateLocalCheckpointForShard(shardRouting.allocationId().getId(), checkpoint);
        final int replicaLocalCheckpoint = randomIntBetween(0, Math.toIntExact(checkpoint));
        final String replicaAllocationId = replicaShard.routingEntry().allocationId().getId();
        primaryShard.updateLocalCheckpointForShard(replicaAllocationId, replicaLocalCheckpoint);

        // initialize the local knowledge on the primary of the persisted global checkpoint on the replica shard
        final int replicaGlobalCheckpoint = randomIntBetween(
            Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED),
            Math.toIntExact(primaryShard.getLastKnownGlobalCheckpoint())
        );
        primaryShard.updateGlobalCheckpointForShard(replicaAllocationId, replicaGlobalCheckpoint);

        // initialize the local knowledge on the primary of the persisted global checkpoint on the primary
        primaryShard.updateGlobalCheckpointForShard(shardRouting.allocationId().getId(), primaryShard.getLastKnownGlobalCheckpoint());

        // simulate a background maybe sync; it should only run if the knowledge on the replica of the global checkpoint lags the primary
        primaryShard.maybeSyncGlobalCheckpoint("test");
        assertThat(
            synced.get(),
            equalTo(maxSeqNo == primaryShard.getLastKnownGlobalCheckpoint() && (replicaGlobalCheckpoint < checkpoint))
        );

        // simulate that the background sync advanced the global checkpoint on the replica
        primaryShard.updateGlobalCheckpointForShard(replicaAllocationId, primaryShard.getLastKnownGlobalCheckpoint());

        // reset our boolean so that we can assert after another simulated maybe sync
        synced.set(false);

        primaryShard.maybeSyncGlobalCheckpoint("test");

        // this time there should not be a sync since all the replica copies are caught up with the primary
        assertFalse(synced.get());

        closeShards(replicaShard, primaryShard);
    }

    public void testClosedIndicesSkipSyncGlobalCheckpoint() throws Exception {
        ShardId shardId = new ShardId("index", "_na_", 0);
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            )
            .state(IndexMetadata.State.CLOSE)
            .primaryTerm(0, 1);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            shardId,
            randomAlphaOfLength(8),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        AtomicBoolean synced = new AtomicBoolean();
        IndexShard primaryShard = newShard(
            shardRouting,
            indexMetadata.build(),
            null,
            new InternalEngineFactory(),
            () -> synced.set(true),
            RetentionLeaseSyncer.EMPTY
        );
        recoverShardFromStore(primaryShard);
        IndexShard replicaShard = newShard(shardId, false);
        recoverReplica(replicaShard, primaryShard, true);
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(primaryShard, "_doc", Integer.toString(i));
        }
        assertThat(primaryShard.getLocalCheckpoint(), equalTo(numDocs - 1L));
        primaryShard.updateLocalCheckpointForShard(replicaShard.shardRouting.allocationId().getId(), primaryShard.getLocalCheckpoint());
        long globalCheckpointOnReplica = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, primaryShard.getLocalCheckpoint());
        primaryShard.updateGlobalCheckpointForShard(replicaShard.shardRouting.allocationId().getId(), globalCheckpointOnReplica);
        primaryShard.maybeSyncGlobalCheckpoint("test");
        assertFalse("closed indices should skip global checkpoint sync", synced.get());
        closeShards(primaryShard, replicaShard);
    }

    public void testRestoreLocalHistoryFromTranslogOnPromotion() throws IOException, InterruptedException {
        final IndexShard indexShard = newStartedShard(false);
        final int operations = randomBoolean() ? scaledRandomIntBetween(0, 1024) : 1024 - scaledRandomIntBetween(0, 1024);
        indexOnReplicaWithGaps(indexShard, operations, Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED));

        final long maxSeqNo = indexShard.seqNoStats().getMaxSeqNo();
        final long globalCheckpointOnReplica = randomLongBetween(UNASSIGNED_SEQ_NO, indexShard.getLocalCheckpoint());
        indexShard.updateGlobalCheckpointOnReplica(globalCheckpointOnReplica, "test");

        final long globalCheckpoint = randomLongBetween(UNASSIGNED_SEQ_NO, indexShard.getLocalCheckpoint());
        final long maxSeqNoOfUpdatesOrDeletes = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, maxSeqNo);
        final long maxSeqNoOfUpdatesOrDeletesBeforeRollback = indexShard.getMaxSeqNoOfUpdatesOrDeletes();
        final Set<String> docsBeforeRollback = getShardDocUIDs(indexShard);
        final CountDownLatch latch = new CountDownLatch(1);
        randomReplicaOperationPermitAcquisition(
            indexShard,
            indexShard.getPendingPrimaryTerm() + 1,
            globalCheckpoint,
            maxSeqNoOfUpdatesOrDeletes,
            new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {

                }
            },
            ""
        );

        latch.await();
        long globalCheckpointOnPromotedReplica = Math.max(globalCheckpointOnReplica, globalCheckpoint);
        long expectedMaxSeqNoOfUpdatesOrDeletes = globalCheckpointOnPromotedReplica < maxSeqNo
            ? maxSeqNo
            : Math.max(maxSeqNoOfUpdatesOrDeletesBeforeRollback, maxSeqNoOfUpdatesOrDeletes);
        assertThat(indexShard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(expectedMaxSeqNoOfUpdatesOrDeletes));
        final ShardRouting newRouting = indexShard.routingEntry().moveActiveReplicaToPrimary();
        final CountDownLatch resyncLatch = new CountDownLatch(1);
        indexShard.updateShardState(
            newRouting,
            indexShard.getPendingPrimaryTerm() + 1,
            (s, r) -> resyncLatch.countDown(),
            1L,
            Collections.singleton(newRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(newRouting.shardId()).addShard(newRouting).build()
        );
        resyncLatch.await();
        assertThat(indexShard.getLocalCheckpoint(), equalTo(maxSeqNo));
        assertThat(indexShard.seqNoStats().getMaxSeqNo(), equalTo(maxSeqNo));
        assertThat(getShardDocUIDs(indexShard), equalTo(docsBeforeRollback));
        assertThat(indexShard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(expectedMaxSeqNoOfUpdatesOrDeletes));
        closeShard(indexShard, false);
    }

    public void testRollbackReplicaEngineOnPromotion() throws IOException, InterruptedException {
        final IndexShard indexShard = newStartedShard(false);

        // most of the time this is large enough that most of the time there will be at least one gap
        final int operations = 1024 - scaledRandomIntBetween(0, 1024);
        indexOnReplicaWithGaps(indexShard, operations, Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED));

        final long globalCheckpointOnReplica = randomLongBetween(UNASSIGNED_SEQ_NO, indexShard.getLocalCheckpoint());
        indexShard.updateGlobalCheckpointOnReplica(globalCheckpointOnReplica, "test");
        final long globalCheckpoint = randomLongBetween(UNASSIGNED_SEQ_NO, indexShard.getLocalCheckpoint());
        Set<String> docsBelowGlobalCheckpoint = getShardDocUIDs(indexShard).stream()
            .filter(id -> Long.parseLong(id) <= Math.max(globalCheckpointOnReplica, globalCheckpoint))
            .collect(Collectors.toSet());
        final CountDownLatch latch = new CountDownLatch(1);
        final boolean shouldRollback = Math.max(globalCheckpoint, globalCheckpointOnReplica) < indexShard.seqNoStats().getMaxSeqNo()
            && indexShard.seqNoStats().getMaxSeqNo() != SequenceNumbers.NO_OPS_PERFORMED;
        final Engine beforeRollbackEngine = indexShard.getEngine();
        final long newMaxSeqNoOfUpdates = randomLongBetween(indexShard.getMaxSeqNoOfUpdatesOrDeletes(), Long.MAX_VALUE);
        randomReplicaOperationPermitAcquisition(
            indexShard,
            indexShard.getPendingPrimaryTerm() + 1,
            globalCheckpoint,
            newMaxSeqNoOfUpdates,
            new ActionListener<Releasable>() {
                @Override
                public void onResponse(final Releasable releasable) {
                    releasable.close();
                    latch.countDown();
                }

                @Override
                public void onFailure(final Exception e) {

                }
            },
            ""
        );

        latch.await();
        if (globalCheckpointOnReplica == UNASSIGNED_SEQ_NO && globalCheckpoint == UNASSIGNED_SEQ_NO) {
            assertThat(indexShard.getLocalCheckpoint(), equalTo(SequenceNumbers.NO_OPS_PERFORMED));
        } else {
            assertThat(indexShard.getLocalCheckpoint(), equalTo(Math.max(globalCheckpoint, globalCheckpointOnReplica)));
        }
        assertThat(getShardDocUIDs(indexShard), equalTo(docsBelowGlobalCheckpoint));
        if (shouldRollback) {
            assertThat(indexShard.getEngine(), not(sameInstance(beforeRollbackEngine)));
        } else {
            assertThat(indexShard.getEngine(), sameInstance(beforeRollbackEngine));
        }
        assertThat(indexShard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(newMaxSeqNoOfUpdates));
        // ensure that after the local checkpoint throw back and indexing again, the local checkpoint advances
        final Result result = indexOnReplicaWithGaps(indexShard, operations, Math.toIntExact(indexShard.getLocalCheckpoint()));
        assertThat(indexShard.getLocalCheckpoint(), equalTo((long) result.localCheckpoint));
        closeShard(indexShard, false);
    }

    public void testConcurrentTermIncreaseOnReplicaShard() throws BrokenBarrierException, InterruptedException, IOException {
        final IndexShard indexShard = newStartedShard(false);

        final CyclicBarrier barrier = new CyclicBarrier(3);
        final CountDownLatch latch = new CountDownLatch(2);

        final long primaryTerm = indexShard.getPendingPrimaryTerm();
        final AtomicLong counter = new AtomicLong();
        final AtomicReference<Exception> onFailure = new AtomicReference<>();

        final LongFunction<Runnable> function = increment -> () -> {
            assert increment > 0;
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            indexShard.acquireReplicaOperationPermit(
                primaryTerm + increment,
                indexShard.getLastKnownGlobalCheckpoint(),
                randomNonNegativeLong(),
                new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        counter.incrementAndGet();
                        assertThat(indexShard.getOperationPrimaryTerm(), equalTo(primaryTerm + increment));
                        latch.countDown();
                        releasable.close();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailure.set(e);
                        latch.countDown();
                    }
                },
                ThreadPool.Names.WRITE,
                ""
            );
        };

        final long firstIncrement = 1 + (randomBoolean() ? 0 : 1);
        final long secondIncrement = 1 + (randomBoolean() ? 0 : 1);
        final Thread first = new Thread(function.apply(firstIncrement));
        final Thread second = new Thread(function.apply(secondIncrement));

        first.start();
        second.start();

        // the two threads synchronize attempting to acquire an operation permit
        barrier.await();

        // we wait for both operations to complete
        latch.await();

        first.join();
        second.join();

        final Exception e;
        if ((e = onFailure.get()) != null) {
            /*
             * If one thread tried to set the primary term to a higher value than the other thread and the thread with the higher term won
             * the race, then the other thread lost the race and only one operation should have been executed.
             */
            assertThat(e, instanceOf(IllegalStateException.class));
            assertThat(e, hasToString(matches("operation primary term \\[\\d+\\] is too old")));
            assertThat(counter.get(), equalTo(1L));
        } else {
            assertThat(counter.get(), equalTo(2L));
        }

        assertThat(indexShard.getPendingPrimaryTerm(), equalTo(primaryTerm + Math.max(firstIncrement, secondIncrement)));
        assertThat(indexShard.getOperationPrimaryTerm(), equalTo(indexShard.getPendingPrimaryTerm()));

        closeShards(indexShard);
    }

    /***
     * test one can snapshot the store at various lifecycle stages
     */
    public void testSnapshotStore() throws IOException {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0");
        flushShard(shard);

        final IndexShard newShard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

        Store.MetadataSnapshot snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_3"));

        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_3"));

        assertTrue(recoverFromStore(newShard));

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_3"));

        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_3"));

        newShard.close("test", false);

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_3"));

        closeShards(newShard);
    }

    public void testAsyncFsync() throws InterruptedException, IOException {
        IndexShard shard = newStartedShard();
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch latch = new CountDownLatch(thread.length);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.countDown();
                        latch.await();
                        for (int i = 0; i < 10000; i++) {
                            semaphore.acquire();
                            shard.sync(new Translog.Location(randomLong(), randomLong(), randomInt()), (ex) -> semaphore.release());
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            thread[i].start();
        }

        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
        assertTrue(semaphore.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS));

        closeShards(shard);
    }

    public void testShardStats() throws IOException {

        IndexShard shard = newStartedShard();
        ShardStats stats = new ShardStats(
            shard.routingEntry(),
            shard.shardPath(),
            new CommonStats(new IndicesQueryCache(Settings.EMPTY), shard, new CommonStatsFlags()),
            shard.commitStats(),
            shard.seqNoStats(),
            shard.getRetentionLeaseStats()
        );
        assertEquals(shard.shardPath().getRootDataPath().toString(), stats.getDataPath());
        assertEquals(shard.shardPath().getRootStatePath().toString(), stats.getStatePath());
        assertEquals(shard.shardPath().isCustomDataPath(), stats.isCustomDataPath());

        // try to serialize it to ensure values survive the serialization
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        stats = new ShardStats(in);

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        String xContent = Strings.toString(builder);
        StringBuilder expectedSubSequence = new StringBuilder("\"shard_path\":{\"state_path\":\"");
        expectedSubSequence.append(shard.shardPath().getRootStatePath().toString());
        expectedSubSequence.append("\",\"data_path\":\"");
        expectedSubSequence.append(shard.shardPath().getRootDataPath().toString());
        expectedSubSequence.append("\",\"is_custom_data_path\":").append(shard.shardPath().isCustomDataPath()).append("}");
        if (Constants.WINDOWS) {
            // Some path weirdness on windows
        } else {
            assertTrue(xContent.contains(expectedSubSequence));
        }
        closeShards(shard);
    }

    public void testShardStatsWithFailures() throws IOException {
        allowShardFailures();
        final ShardId shardId = new ShardId("index", "_na_", 0);
        final ShardRouting shardRouting = newShardRouting(
            shardId,
            "node",
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        final NodeEnvironment.DataPath dataPath = new NodeEnvironment.DataPath(createTempDir());

        ShardPath shardPath = new ShardPath(false, dataPath.resolve(shardId), dataPath.resolve(shardId), shardId);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(shardRouting.getIndexName()).settings(settings).primaryTerm(0, 1).build();

        // Override two Directory methods to make them fail at our will
        // We use AtomicReference here to inject failure in the middle of the test not immediately
        // We use Supplier<IOException> instead of IOException to produce meaningful stacktrace
        // (remember stack trace is filled when exception is instantiated)
        AtomicReference<Supplier<IOException>> exceptionToThrow = new AtomicReference<>();
        AtomicBoolean throwWhenMarkingStoreCorrupted = new AtomicBoolean(false);
        Directory directory = new FilterDirectory(newFSDirectory(shardPath.resolveIndex())) {
            // fileLength method is called during storeStats try block
            // it's not called when store is marked as corrupted
            @Override
            public long fileLength(String name) throws IOException {
                Supplier<IOException> ex = exceptionToThrow.get();
                if (ex == null) {
                    return super.fileLength(name);
                } else {
                    throw ex.get();
                }
            }

            // listAll method is called when marking store as corrupted
            @Override
            public String[] listAll() throws IOException {
                Supplier<IOException> ex = exceptionToThrow.get();
                if (throwWhenMarkingStoreCorrupted.get() && ex != null) {
                    throw ex.get();
                } else {
                    return super.listAll();
                }
            }
        };

        try (Store store = createStore(shardId, new IndexSettings(metadata, Settings.EMPTY), directory)) {
            IndexShard shard = newShard(
                shardRouting,
                shardPath,
                metadata,
                i -> store,
                null,
                new InternalEngineFactory(),
                () -> {},
                RetentionLeaseSyncer.EMPTY,
                EMPTY_EVENT_LISTENER
            );
            AtomicBoolean failureCallbackTriggered = new AtomicBoolean(false);
            shard.addShardFailureCallback((ig) -> failureCallbackTriggered.set(true));

            recoverShardFromStore(shard);

            final boolean corruptIndexException = randomBoolean();

            if (corruptIndexException) {
                exceptionToThrow.set(() -> new CorruptIndexException("Test CorruptIndexException", "Test resource"));
                throwWhenMarkingStoreCorrupted.set(randomBoolean());
            } else {
                exceptionToThrow.set(() -> new IOException("Test IOException"));
            }
            ElasticsearchException e = expectThrows(ElasticsearchException.class, shard::storeStats);
            assertTrue(failureCallbackTriggered.get());

            if (corruptIndexException && throwWhenMarkingStoreCorrupted.get() == false) {
                assertTrue(store.isMarkedCorrupted());
            }
        }
    }

    public void testRefreshMetric() throws IOException {
        IndexShard shard = newStartedShard();
        // refresh on: finalize and end of recovery
        // finalizing a replica involves two refreshes with soft deletes because of estimateNumberOfHistoryOperations()
        final long initialRefreshes = shard.routingEntry().primary() || shard.indexSettings().isSoftDeleteEnabled() == false ? 2L : 3L;
        assertThat(shard.refreshStats().getTotal(), equalTo(initialRefreshes));
        long initialTotalTime = shard.refreshStats().getTotalTimeInMillis();
        // check time advances
        for (int i = 1; shard.refreshStats().getTotalTimeInMillis() == initialTotalTime; i++) {
            indexDoc(shard, "_doc", "test");
            assertThat(shard.refreshStats().getTotal(), equalTo(initialRefreshes + i - 1));
            shard.refresh("test");
            assertThat(shard.refreshStats().getTotal(), equalTo(initialRefreshes + i));
            assertThat(shard.refreshStats().getTotalTimeInMillis(), greaterThanOrEqualTo(initialTotalTime));
        }
        long refreshCount = shard.refreshStats().getTotal();
        indexDoc(shard, "_doc", "test");
        try (Engine.GetResult ignored = shard.get(new Engine.Get(true, false, "test"))) {
            assertThat(shard.refreshStats().getTotal(), equalTo(refreshCount + 1));
        }
        indexDoc(shard, "_doc", "test");
        shard.writeIndexingBuffer();
        assertThat(shard.refreshStats().getTotal(), equalTo(refreshCount + 2));
        closeShards(shard);
    }

    public void testExternalRefreshMetric() throws IOException {
        IndexShard shard = newStartedShard();
        assertThat(shard.refreshStats().getExternalTotal(), equalTo(2L)); // refresh on: finalize and end of recovery
        long initialTotalTime = shard.refreshStats().getExternalTotalTimeInMillis();
        // check time advances
        for (int i = 1; shard.refreshStats().getExternalTotalTimeInMillis() == initialTotalTime; i++) {
            indexDoc(shard, "_doc", "test");
            assertThat(shard.refreshStats().getExternalTotal(), equalTo(2L + i - 1));
            shard.refresh("test");
            assertThat(shard.refreshStats().getExternalTotal(), equalTo(2L + i));
            assertThat(shard.refreshStats().getExternalTotalTimeInMillis(), greaterThanOrEqualTo(initialTotalTime));
        }
        final long externalRefreshCount = shard.refreshStats().getExternalTotal();
        final long extraInternalRefreshes = shard.routingEntry().primary() || shard.indexSettings().isSoftDeleteEnabled() == false ? 0 : 1;
        indexDoc(shard, "_doc", "test");
        try (Engine.GetResult ignored = shard.get(new Engine.Get(true, false, "test"))) {
            assertThat(shard.refreshStats().getExternalTotal(), equalTo(externalRefreshCount));
            assertThat(shard.refreshStats().getExternalTotal(), equalTo(shard.refreshStats().getTotal() - 1 - extraInternalRefreshes));
        }
        indexDoc(shard, "_doc", "test");
        shard.writeIndexingBuffer();
        assertThat(shard.refreshStats().getExternalTotal(), equalTo(externalRefreshCount));
        assertThat(shard.refreshStats().getExternalTotal(), equalTo(shard.refreshStats().getTotal() - 2 - extraInternalRefreshes));
        closeShards(shard);
    }

    public void testIndexingOperationsListeners() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0", "{\"foo\" : \"bar\"}");
        shard.updateLocalCheckpointForShard(shard.shardRouting.allocationId().getId(), 0);
        AtomicInteger preIndex = new AtomicInteger();
        AtomicInteger postIndexCreate = new AtomicInteger();
        AtomicInteger postIndexUpdate = new AtomicInteger();
        AtomicInteger postIndexException = new AtomicInteger();
        AtomicInteger preDelete = new AtomicInteger();
        AtomicInteger postDelete = new AtomicInteger();
        AtomicInteger postDeleteException = new AtomicInteger();
        shard.close("simon says", true);
        shard = reinitShard(shard, new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                preIndex.incrementAndGet();
                return operation;
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
                switch (result.getResultType()) {
                    case SUCCESS:
                        if (result.isCreated()) {
                            postIndexCreate.incrementAndGet();
                        } else {
                            postIndexUpdate.incrementAndGet();
                        }
                        break;
                    case FAILURE:
                        postIndex(shardId, index, result.getFailure());
                        break;
                    default:
                        fail("unexpected result type:" + result.getResultType());
                }
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
                postIndexException.incrementAndGet();
            }

            @Override
            public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
                preDelete.incrementAndGet();
                return delete;
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
                switch (result.getResultType()) {
                    case SUCCESS -> postDelete.incrementAndGet();
                    case FAILURE -> postDelete(shardId, delete, result.getFailure());
                    default -> fail("unexpected result type:" + result.getResultType());
                }
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
                postDeleteException.incrementAndGet();

            }
        });
        recoverShardFromStore(shard);

        indexDoc(shard, "_doc", "1");
        assertEquals(1, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(0, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(0, postDelete.get());
        assertEquals(0, postDeleteException.get());

        indexDoc(shard, "_doc", "1");
        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(0, postDelete.get());
        assertEquals(0, postDeleteException.get());

        deleteDoc(shard, "1");

        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(1, preDelete.get());
        assertEquals(1, postDelete.get());
        assertEquals(0, postDeleteException.get());

        shard.close("Unexpected close", true);
        shard.state = IndexShardState.STARTED; // It will generate exception

        try {
            indexDoc(shard, "_doc", "1");
            fail();
        } catch (AlreadyClosedException e) {

        }

        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(1, preDelete.get());
        assertEquals(1, postDelete.get());
        assertEquals(0, postDeleteException.get());
        try {
            deleteDoc(shard, "1");
            fail();
        } catch (AlreadyClosedException e) {

        }

        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(1, preDelete.get());
        assertEquals(1, postDelete.get());
        assertEquals(0, postDeleteException.get());

        closeShards(shard);
    }

    public void testLockingBeforeAndAfterRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        CountDownLatch latch = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            latch.countDown();
            blockingCallRelocated(shard, routing, (primaryContext, listener) -> listener.onResponse(null));
        });

        try (Releasable ignored = acquirePrimaryOperationPermitBlockingly(shard)) {
            // start finalization of recovery
            recoveryThread.start();
            latch.await();
            // recovery can only be finalized after we release the current primaryOperationLock
            assertFalse(shard.isRelocatedPrimary());
        }
        // recovery can be now finalized
        recoveryThread.join();
        assertTrue(shard.isRelocatedPrimary());
        final ExecutionException e = expectThrows(ExecutionException.class, () -> acquirePrimaryOperationPermitBlockingly(shard));
        assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
        assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));

        closeShards(shard);
    }

    public void testDelayedOperationsBeforeAndAfterRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        final CountDownLatch startRecovery = new CountDownLatch(1);
        final CountDownLatch relocationStarted = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            try {
                startRecovery.await();
                shard.relocated(routing.getTargetRelocatingShard().allocationId().getId(), (primaryContext, listener) -> {
                    relocationStarted.countDown();
                    listener.onResponse(null);
                }, ActionListener.noop());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        recoveryThread.start();

        final int numberOfAcquisitions = randomIntBetween(1, 10);
        final List<Runnable> assertions = new ArrayList<>(numberOfAcquisitions);
        final int recoveryIndex = randomIntBetween(0, numberOfAcquisitions - 1);

        for (int i = 0; i < numberOfAcquisitions; i++) {
            final PlainActionFuture<Releasable> onLockAcquired;
            if (i < recoveryIndex) {
                final AtomicBoolean invoked = new AtomicBoolean();
                onLockAcquired = new PlainActionFuture<>() {

                    @Override
                    public void onResponse(Releasable releasable) {
                        invoked.set(true);
                        releasable.close();
                        super.onResponse(releasable);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError();
                    }

                };
                assertions.add(() -> assertTrue(invoked.get()));
            } else if (recoveryIndex == i) {
                startRecovery.countDown();
                relocationStarted.await();
                onLockAcquired = new PlainActionFuture<>();
                assertions.add(() -> {
                    final ExecutionException e = expectThrows(ExecutionException.class, () -> onLockAcquired.get(30, TimeUnit.SECONDS));
                    assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
                    assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));
                });
            } else {
                onLockAcquired = new PlainActionFuture<>();
                assertions.add(() -> {
                    final ExecutionException e = expectThrows(ExecutionException.class, () -> onLockAcquired.get(30, TimeUnit.SECONDS));
                    assertThat(e.getCause(), instanceOf(ShardNotInPrimaryModeException.class));
                    assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));
                });
            }

            shard.acquirePrimaryOperationPermit(onLockAcquired, ThreadPool.Names.WRITE, "i_" + i);
        }

        for (final Runnable assertion : assertions) {
            assertion.run();
        }

        recoveryThread.join();

        closeShards(shard);
    }

    public void testStressRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true);
        assertFalse(shard.isRelocatedPrimary());
        final ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        final int numThreads = randomIntBetween(2, 4);
        Thread[] indexThreads = new Thread[numThreads];
        CountDownLatch allPrimaryOperationLocksAcquired = new CountDownLatch(numThreads);
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < indexThreads.length; i++) {
            indexThreads[i] = new Thread() {
                @Override
                public void run() {
                    try (Releasable operationLock = acquirePrimaryOperationPermitBlockingly(shard)) {
                        allPrimaryOperationLocksAcquired.countDown();
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            indexThreads[i].start();
        }
        AtomicBoolean relocated = new AtomicBoolean();
        final Thread recoveryThread = new Thread(() -> {
            blockingCallRelocated(shard, routing, (primaryContext, listener) -> listener.onResponse(null));
            relocated.set(true);
        });
        // ensure we wait for all primary operation locks to be acquired
        allPrimaryOperationLocksAcquired.await();
        // start recovery thread
        recoveryThread.start();
        assertThat(relocated.get(), equalTo(false));
        assertThat(shard.getActiveOperationsCount(), greaterThan(0));
        // ensure we only transition after pending operations completed
        assertFalse(shard.isRelocatedPrimary());
        // complete pending operations
        barrier.await();
        // complete recovery/relocation
        recoveryThread.join();
        // ensure relocated successfully once pending operations are done
        assertThat(relocated.get(), equalTo(true));
        assertTrue(shard.isRelocatedPrimary());
        assertThat(shard.getActiveOperationsCount(), equalTo(0));

        for (Thread indexThread : indexThreads) {
            indexThread.join();
        }

        closeShards(shard);
    }

    public void testRelocatedShardCanNotBeRevived() throws IOException {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting originalRouting = shard.routingEntry();
        final ShardRouting routing = ShardRoutingHelper.relocate(originalRouting, "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        blockingCallRelocated(shard, routing, (primaryContext, listener) -> listener.onResponse(null));
        expectThrows(IllegalIndexShardStateException.class, () -> IndexShardTestCase.updateRoutingEntry(shard, originalRouting));
        closeShards(shard);
    }

    public void testShardCanNotBeMarkedAsRelocatedIfRelocationCancelled() throws IOException {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting originalRouting = shard.routingEntry();
        final ShardRouting relocationRouting = ShardRoutingHelper.relocate(originalRouting, "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, relocationRouting);
        IndexShardTestCase.updateRoutingEntry(shard, originalRouting);
        expectThrows(
            IllegalIndexShardStateException.class,
            () -> blockingCallRelocated(shard, relocationRouting, (primaryContext, listener) -> fail("should not be called"))
        );
        closeShards(shard);
    }

    public void testRelocatedShardCanNotBeRevivedConcurrently() throws IOException, InterruptedException, BrokenBarrierException {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting originalRouting = shard.routingEntry();
        final ShardRouting relocationRouting = ShardRoutingHelper.relocate(originalRouting, "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, relocationRouting);
        CyclicBarrier cyclicBarrier = new CyclicBarrier(3);
        AtomicReference<Exception> relocationException = new AtomicReference<>();
        Thread relocationThread = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                relocationException.set(e);
            }

            @Override
            protected void doRun() throws Exception {
                cyclicBarrier.await();
                blockingCallRelocated(shard, relocationRouting, (primaryContext, listener) -> listener.onResponse(null));
            }
        });
        relocationThread.start();
        AtomicReference<Exception> cancellingException = new AtomicReference<>();
        Thread cancellingThread = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                cancellingException.set(e);
            }

            @Override
            protected void doRun() throws Exception {
                cyclicBarrier.await();
                IndexShardTestCase.updateRoutingEntry(shard, originalRouting);
            }
        });
        cancellingThread.start();
        cyclicBarrier.await();
        relocationThread.join();
        cancellingThread.join();
        if (shard.isRelocatedPrimary()) {
            logger.debug("shard was relocated successfully");
            assertThat(cancellingException.get(), instanceOf(IllegalIndexShardStateException.class));
            assertThat("current routing:" + shard.routingEntry(), shard.routingEntry().relocating(), equalTo(true));
            assertThat(relocationException.get(), nullValue());
        } else {
            logger.debug("shard relocation was cancelled");
            assertThat(
                relocationException.get(),
                either(instanceOf(IllegalIndexShardStateException.class)).or(instanceOf(IllegalStateException.class))
            );
            assertThat("current routing:" + shard.routingEntry(), shard.routingEntry().relocating(), equalTo(false));
            assertThat(cancellingException.get(), nullValue());

        }
        closeShards(shard);
    }

    public void testRelocateMissingTarget() throws Exception {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting original = shard.routingEntry();
        final ShardRouting toNode1 = ShardRoutingHelper.relocate(original, "node_1");
        IndexShardTestCase.updateRoutingEntry(shard, toNode1);
        IndexShardTestCase.updateRoutingEntry(shard, original);
        final ShardRouting toNode2 = ShardRoutingHelper.relocate(original, "node_2");
        IndexShardTestCase.updateRoutingEntry(shard, toNode2);
        final AtomicBoolean relocated = new AtomicBoolean();
        final IllegalStateException error = expectThrows(
            IllegalStateException.class,
            () -> blockingCallRelocated(shard, toNode1, (ctx, listener) -> relocated.set(true))
        );
        assertThat(
            error.getMessage(),
            equalTo(
                "relocation target ["
                    + toNode1.getTargetRelocatingShard().allocationId().getId()
                    + "] is no longer part of the replication group"
            )
        );
        assertFalse(relocated.get());
        blockingCallRelocated(shard, toNode2, (ctx, listener) -> {
            relocated.set(true);
            listener.onResponse(null);
        });
        assertTrue(relocated.get());
        closeShards(shard);
    }

    public void testRecoverFromStoreWithOutOfOrderDelete() throws IOException {
        /*
         * The flow of this test:
         * - delete #1
         * - roll generation (to create gen 2)
         * - index #0
         * - index #3
         * - flush (commit point has max_seqno 3, and local checkpoint 1 -> points at gen 2, previous commit point is maintained)
         * - index #2
         * - index #5
         * - If flush and then recover from the existing store, delete #1 will be removed while index #0 is still retained and replayed.
         */
        final IndexShard shard = newStartedShard(false);
        long primaryTerm = shard.getOperationPrimaryTerm();
        shard.advanceMaxSeqNoOfUpdatesOrDeletes(1); // manually advance msu for this delete
        shard.applyDeleteOperationOnReplica(1, primaryTerm, 2, "id");
        shard.getEngine().rollTranslogGeneration(); // isolate the delete in it's own generation
        shard.applyIndexOperationOnReplica(
            0,
            primaryTerm,
            1,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse("id", new BytesArray("{}"), XContentType.JSON)
        );
        shard.applyIndexOperationOnReplica(
            3,
            primaryTerm,
            3,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse("id-3", new BytesArray("{}"), XContentType.JSON)
        );
        // Flushing a new commit with local checkpoint=1 allows to skip the translog gen #1 in recovery.
        shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
        shard.applyIndexOperationOnReplica(
            2,
            primaryTerm,
            3,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse("id-2", new BytesArray("{}"), XContentType.JSON)
        );
        shard.applyIndexOperationOnReplica(
            5,
            primaryTerm,
            1,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse("id-5", new BytesArray("{}"), XContentType.JSON)
        );
        shard.sync(); // advance local checkpoint

        final int translogOps;
        final int replayedOps;
        if (randomBoolean()) {
            // Advance the global checkpoint to remove the 1st commit; this shard will recover the 2nd commit.
            shard.updateGlobalCheckpointOnReplica(3, "test");
            logger.info("--> flushing shard");
            shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            translogOps = 4; // delete #1 won't be replayed.
            replayedOps = 3;
        } else {
            if (randomBoolean()) {
                shard.getEngine().rollTranslogGeneration();
            }
            translogOps = 5;
            replayedOps = 5;
        }

        final ShardRouting replicaRouting = shard.routingEntry();
        IndexShard newShard = reinitShard(
            shard,
            newShardRouting(
                replicaRouting.shardId(),
                replicaRouting.currentNodeId(),
                true,
                ShardRoutingState.INITIALIZING,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE
            )
        );
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(recoverFromStore(newShard));
        assertEquals(replayedOps, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        updateRoutingEntry(newShard, ShardRoutingHelper.moveToStarted(newShard.routingEntry()));
        assertDocCount(newShard, 3);
        closeShards(newShard);
    }

    public void testRecoverFromStore() throws IOException {
        final IndexShard shard = newStartedShard(true);
        int totalOps = randomInt(10);
        int translogOps = totalOps;
        for (int i = 0; i < totalOps; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        if (randomBoolean()) {
            shard.updateLocalCheckpointForShard(shard.shardRouting.allocationId().getId(), totalOps - 1);
            flushShard(shard);
            translogOps = 0;
        }
        String historyUUID = shard.getHistoryUUID();
        IndexShard newShard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(recoverFromStore(newShard));
        assertEquals(translogOps, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        assertEquals(100.0f, newShard.recoveryState().getTranslog().recoveredPercent(), 0.01f);
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        // check that local checkpoint of new primary is properly tracked after recovery
        assertThat(newShard.getLocalCheckpoint(), equalTo(totalOps - 1L));
        assertThat(
            newShard.getReplicationTracker()
                .getTrackedLocalCheckpointForShard(newShard.routingEntry().allocationId().getId())
                .getLocalCheckpoint(),
            equalTo(totalOps - 1L)
        );
        assertThat(newShard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(totalOps - 1L));
        assertDocCount(newShard, totalOps);
        assertThat(newShard.getHistoryUUID(), equalTo(historyUUID));
        closeShards(newShard);
    }

    public void testRecoverFromStalePrimaryForceNewHistoryUUID() throws IOException {
        final IndexShard shard = newStartedShard(true);
        int totalOps = randomInt(10);
        for (int i = 0; i < totalOps; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        if (randomBoolean()) {
            shard.updateLocalCheckpointForShard(shard.shardRouting.allocationId().getId(), totalOps - 1);
            flushShard(shard);
        }
        String historyUUID = shard.getHistoryUUID();
        IndexShard newShard = reinitShard(
            shard,
            newShardRouting(
                shard.shardId(),
                shard.shardRouting.currentNodeId(),
                true,
                ShardRoutingState.INITIALIZING,
                RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE
            )
        );
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(recoverFromStore(newShard));
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, totalOps);
        assertThat(newShard.getHistoryUUID(), not(equalTo(historyUUID)));
        closeShards(newShard);
    }

    public void testPrimaryHandOffUpdatesLocalCheckpoint() throws IOException {
        final IndexShard primarySource = newStartedShard(true);
        int totalOps = randomInt(10);
        for (int i = 0; i < totalOps; i++) {
            indexDoc(primarySource, "_doc", Integer.toString(i));
        }
        IndexShardTestCase.updateRoutingEntry(primarySource, primarySource.routingEntry().relocate(randomAlphaOfLength(10), -1));
        final IndexShard primaryTarget = newShard(primarySource.routingEntry().getTargetRelocatingShard());
        updateMappings(primaryTarget, primarySource.indexSettings().getIndexMetadata());
        recoverReplica(primaryTarget, primarySource, true);

        // check that local checkpoint of new primary is properly tracked after primary relocation
        assertThat(primaryTarget.getLocalCheckpoint(), equalTo(totalOps - 1L));
        assertThat(
            primaryTarget.getReplicationTracker()
                .getTrackedLocalCheckpointForShard(primaryTarget.routingEntry().allocationId().getId())
                .getLocalCheckpoint(),
            equalTo(totalOps - 1L)
        );
        assertDocCount(primaryTarget, totalOps);
        closeShards(primarySource, primaryTarget);
    }

    /* This test just verifies that we fill up local checkpoint up to max seen seqID on primary recovery */
    public void testRecoverFromStoreWithNoOps() throws IOException {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0");
        indexDoc(shard, "_doc", "1");
        // start a replica shard and index the second doc
        final IndexShard otherShard = newStartedShard(false);
        updateMappings(otherShard, shard.indexSettings().getIndexMetadata());
        SourceToParse sourceToParse = new SourceToParse("1", new BytesArray("{}"), XContentType.JSON);
        otherShard.applyIndexOperationOnReplica(
            1,
            otherShard.getOperationPrimaryTerm(),
            1,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            sourceToParse
        );

        final ShardRouting primaryShardRouting = shard.routingEntry();
        IndexShard newShard = reinitShard(
            otherShard,
            ShardRoutingHelper.initWithSameId(primaryShardRouting, RecoverySource.ExistingStoreRecoverySource.INSTANCE)
        );
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(recoverFromStore(newShard));
        assertEquals(1, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(1, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(1, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        assertEquals(100.0f, newShard.recoveryState().getTranslog().recoveredPercent(), 0.01f);
        try (Translog.Snapshot snapshot = getTranslog(newShard).newSnapshot()) {
            Translog.Operation operation;
            int numNoops = 0;
            while ((operation = snapshot.next()) != null) {
                if (operation.opType() == Translog.Operation.Type.NO_OP) {
                    numNoops++;
                    assertEquals(newShard.getPendingPrimaryTerm(), operation.primaryTerm());
                    assertEquals(0, operation.seqNo());
                }
            }
            assertEquals(1, numNoops);
        }
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 1);
        assertDocCount(shard, 2);

        for (int i = 0; i < 2; i++) {
            newShard = reinitShard(
                newShard,
                ShardRoutingHelper.initWithSameId(primaryShardRouting, RecoverySource.ExistingStoreRecoverySource.INSTANCE)
            );
            newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
            assertTrue(recoverFromStore(newShard));
            try (Translog.Snapshot snapshot = getTranslog(newShard).newSnapshot()) {
                assertThat(snapshot.totalOperations(), equalTo(newShard.indexSettings.isSoftDeleteEnabled() ? 0 : 2));
            }
        }
        closeShards(newShard, shard);
    }

    public void testRecoverFromCleanStore() throws IOException {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0");
        if (randomBoolean()) {
            flushShard(shard);
        }
        final ShardRouting shardRouting = shard.routingEntry();
        IndexShard newShard = reinitShard(
            shard,
            ShardRoutingHelper.initWithSameId(shardRouting, RecoverySource.EmptyStoreRecoverySource.INSTANCE)
        );

        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(recoverFromStore(newShard));
        assertEquals(0, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(0, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(0, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        assertEquals(100.0f, newShard.recoveryState().getTranslog().recoveredPercent(), 0.01f);
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 0);
        closeShards(newShard);
    }

    public void testFailIfIndexNotPresentInRecoverFromStore() throws Exception {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0");
        if (randomBoolean()) {
            flushShard(shard);
        }

        Store store = shard.store();
        store.incRef();
        closeShards(shard);
        cleanLuceneIndex(store.directory());
        store.decRef();
        IndexShard newShard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        ShardRouting routing = newShard.routingEntry();
        newShard.markAsRecovering("store", new RecoveryState(routing, localNode, null));
        try {
            recoverFromStore(newShard);
            fail("index not there!");
        } catch (IndexShardRecoveryException ex) {
            assertTrue(ex.getMessage().contains("failed to fetch index version after copying it over"));
        }

        routing = ShardRoutingHelper.moveToUnassigned(routing, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "because I say so"));
        routing = ShardRoutingHelper.initialize(routing, newShard.routingEntry().currentNodeId());
        assertTrue("it's already recovering, we should ignore new ones", newShard.ignoreRecoveryAttempt());
        try {
            newShard.markAsRecovering("store", new RecoveryState(routing, localNode, null));
            fail("we are already recovering, can't mark again");
        } catch (IllegalIndexShardStateException e) {
            // OK!
        }

        newShard = reinitShard(newShard, ShardRoutingHelper.initWithSameId(routing, RecoverySource.EmptyStoreRecoverySource.INSTANCE));
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue("recover even if there is nothing to recover", recoverFromStore(newShard));

        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 0);
        // we can't issue this request through a client because of the inconsistencies we created with the cluster state
        // doing it directly instead
        indexDoc(newShard, "_doc", "0");
        newShard.refresh("test");
        assertDocCount(newShard, 1);

        closeShards(newShard);
    }

    public void testRecoverFromStoreRemoveStaleOperations() throws Exception {
        final IndexShard shard = newStartedShard(false);
        final String indexName = shard.shardId().getIndexName();
        // Index #0, index #1
        shard.applyIndexOperationOnReplica(
            0,
            primaryTerm,
            1,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse("doc-0", new BytesArray("{}"), XContentType.JSON)
        );
        flushShard(shard);
        shard.updateGlobalCheckpointOnReplica(0, "test"); // stick the global checkpoint here.
        shard.applyIndexOperationOnReplica(
            1,
            primaryTerm,
            1,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse("doc-1", new BytesArray("{}"), XContentType.JSON)
        );
        flushShard(shard);
        assertThat(getShardDocUIDs(shard), containsInAnyOrder("doc-0", "doc-1"));
        shard.getEngine().rollTranslogGeneration();
        shard.markSeqNoAsNoop(1, primaryTerm, "test");
        shard.applyIndexOperationOnReplica(
            2,
            primaryTerm,
            1,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse("doc-2", new BytesArray("{}"), XContentType.JSON)
        );
        flushShard(shard);
        assertThat(getShardDocUIDs(shard), containsInAnyOrder("doc-0", "doc-1", "doc-2"));
        closeShard(shard, false);
        // Recovering from store should discard doc #1
        final ShardRouting replicaRouting = shard.routingEntry();
        final IndexMetadata newShardIndexMetadata = IndexMetadata.builder(shard.indexSettings().getIndexMetadata())
            .primaryTerm(replicaRouting.shardId().id(), shard.getOperationPrimaryTerm() + 1)
            .build();
        closeShards(shard);
        IndexShard newShard = newShard(
            newShardRouting(
                replicaRouting.shardId(),
                replicaRouting.currentNodeId(),
                true,
                ShardRoutingState.INITIALIZING,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE
            ),
            shard.shardPath(),
            newShardIndexMetadata,
            null,
            null,
            shard.getEngineFactory(),
            shard.getGlobalCheckpointSyncer(),
            shard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(recoverFromStore(newShard));
        assertThat(getShardDocUIDs(newShard), containsInAnyOrder("doc-0", "doc-2"));
        closeShards(newShard);
    }

    public void testRecoveryFailsAfterMovingToRelocatedState() throws IOException {
        final IndexShard shard = newStartedShard(true);
        ShardRouting origRouting = shard.routingEntry();
        assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        ShardRouting inRecoveryRouting = ShardRoutingHelper.relocate(origRouting, "some_node");
        IndexShardTestCase.updateRoutingEntry(shard, inRecoveryRouting);
        blockingCallRelocated(shard, inRecoveryRouting, (primaryContext, listener) -> listener.onResponse(null));
        assertTrue(shard.isRelocatedPrimary());
        try {
            IndexShardTestCase.updateRoutingEntry(shard, origRouting);
            fail("Expected IndexShardRelocatedException");
        } catch (IndexShardRelocatedException expected) {}

        closeShards(shard);
    }

    public void testRestoreShard() throws IOException {
        final IndexShard source = newStartedShard(true);
        IndexShard target = newStartedShard(true);

        indexDoc(source, "_doc", "0");
        EngineTestCase.generateNewSeqNo(source.getEngine()); // create a gap in the history
        indexDoc(source, "_doc", "2");
        if (randomBoolean()) {
            source.refresh("test");
        }
        indexDoc(target, "_doc", "1");
        target.refresh("test");
        assertDocs(target, "1");
        flushShard(source); // only flush source
        ShardRouting routing = ShardRoutingHelper.initWithSameId(
            target.routingEntry(),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        final Snapshot snapshot = new Snapshot("foo", new SnapshotId("bar", UUIDs.randomBase64UUID()));
        routing = ShardRoutingHelper.newWithRestoreSource(
            routing,
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                snapshot,
                Version.CURRENT,
                new IndexId("test", UUIDs.randomBase64UUID(random()))
            )
        );
        target = reinitShard(target, routing);
        Store sourceStore = source.store();
        Store targetStore = target.store();

        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        target.markAsRecovering("store", new RecoveryState(routing, localNode, null));
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        target.restoreFromRepository(new RestoreOnlyRepository("test") {
            @Override
            public void restoreShard(
                Store store,
                SnapshotId snapshotId,
                IndexId indexId,
                ShardId snapshotShardId,
                RecoveryState recoveryState,
                ActionListener<Void> listener
            ) {
                ActionListener.completeWith(listener, () -> {
                    cleanLuceneIndex(targetStore.directory());
                    for (String file : sourceStore.directory().listAll()) {
                        if (file.equals("write.lock") || file.startsWith("extra")) {
                            continue;
                        }
                        targetStore.directory().copyFrom(sourceStore.directory(), file, file, IOContext.DEFAULT);
                    }
                    recoveryState.getIndex().setFileDetailsComplete();
                    return null;
                });
            }
        }, future);
        assertTrue(future.actionGet());
        assertThat(target.getLocalCheckpoint(), equalTo(2L));
        assertThat(target.seqNoStats().getMaxSeqNo(), equalTo(2L));
        assertThat(target.seqNoStats().getGlobalCheckpoint(), equalTo(0L));
        IndexShardTestCase.updateRoutingEntry(target, routing.moveToStarted());
        assertThat(
            target.getReplicationTracker()
                .getTrackedLocalCheckpointForShard(target.routingEntry().allocationId().getId())
                .getLocalCheckpoint(),
            equalTo(2L)
        );
        assertThat(target.seqNoStats().getGlobalCheckpoint(), equalTo(2L));

        assertDocs(target, "0", "2");

        closeShard(source, false);
        closeShards(target);
    }

    public void testReaderWrapperIsUsed() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0", "{\"foo\" : \"bar\"}");
        indexDoc(shard, "_doc", "1", "{\"foobar\" : \"bar\"}");
        shard.refresh("test");

        try (Engine.GetResult getResult = shard.get(new Engine.Get(false, false, "1"))) {
            assertTrue(getResult.exists());
            assertNotNull(getResult.searcher());
        }
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            TopDocs search = searcher.search(new TermQuery(new Term("foo", "bar")), 10);
            assertEquals(search.totalHits.value, 1);
            search = searcher.search(new TermQuery(new Term("foobar", "bar")), 10);
            assertEquals(search.totalHits.value, 1);
        }
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = reader -> new FieldMaskingReader("foo", reader);
        closeShards(shard);
        IndexShard newShard = newShard(
            ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.ExistingStoreRecoverySource.INSTANCE),
            shard.shardPath(),
            shard.indexSettings().getIndexMetadata(),
            null,
            wrapper,
            new InternalEngineFactory(),
            () -> {},
            RetentionLeaseSyncer.EMPTY,
            EMPTY_EVENT_LISTENER
        );

        recoverShardFromStore(newShard);

        try (Engine.Searcher searcher = newShard.acquireSearcher("test")) {
            TopDocs search = searcher.search(new TermQuery(new Term("foo", "bar")), 10);
            assertEquals(search.totalHits.value, 0);
            search = searcher.search(new TermQuery(new Term("foobar", "bar")), 10);
            assertEquals(search.totalHits.value, 1);
        }
        try (Engine.GetResult getResult = newShard.get(new Engine.Get(false, false, "1"))) {
            assertTrue(getResult.exists());
            assertNotNull(getResult.searcher()); // make sure get uses the wrapped reader
            assertTrue(getResult.searcher().getIndexReader() instanceof FieldMaskingReader);
        }

        closeShards(newShard);
    }

    public void testReaderWrapperWorksWithGlobalOrdinals() throws IOException {
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = reader -> new FieldMaskingReader("foo", reader);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text", "fielddata": true }}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, wrapper);
        recoverShardFromStore(shard);
        indexDoc(shard, "_doc", "0", "{\"foo\" : \"bar\"}");
        shard.refresh("created segment 1");
        indexDoc(shard, "_doc", "1", "{\"foobar\" : \"bar\"}");
        shard.refresh("created segment 2");

        // test global ordinals are evicted
        MappedFieldType foo = shard.mapperService().fieldType("foo");
        IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(
            shard.indexSettings.getNodeSettings(),
            new IndexFieldDataCache.Listener() {
            }
        );
        IndexFieldDataService indexFieldDataService = new IndexFieldDataService(
            shard.indexSettings,
            indicesFieldDataCache,
            new NoneCircuitBreakerService()
        );
        IndexFieldData.Global<?> ifd = indexFieldDataService.getForField(
            foo,
            "test",
            () -> { throw new UnsupportedOperationException("search lookup not available"); }
        );
        FieldDataStats before = shard.fieldData().stats("foo");
        assertThat(before.getMemorySizeInBytes(), equalTo(0L));
        FieldDataStats after = null;
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            assertThat("we have to have more than one segment", searcher.getDirectoryReader().leaves().size(), greaterThan(1));
            ifd.loadGlobal(searcher.getDirectoryReader());
            after = shard.fieldData().stats("foo");
            assertEquals(after.getEvictions(), before.getEvictions());
            // If a field doesn't exist an empty IndexFieldData is returned and that isn't cached:
            assertThat(after.getMemorySizeInBytes(), equalTo(0L));
        }
        assertEquals(shard.fieldData().stats("foo").getEvictions(), before.getEvictions());
        assertEquals(shard.fieldData().stats("foo").getMemorySizeInBytes(), after.getMemorySizeInBytes());
        shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
        shard.refresh("test");
        assertEquals(shard.fieldData().stats("foo").getMemorySizeInBytes(), before.getMemorySizeInBytes());
        assertEquals(shard.fieldData().stats("foo").getEvictions(), before.getEvictions());

        closeShards(shard);
    }

    public void testIndexingOperationListenersIsInvokedOnRecovery() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0", "{\"foo\" : \"bar\"}");
        deleteDoc(shard, "0");
        indexDoc(shard, "_doc", "1", "{\"foo\" : \"bar\"}");
        shard.refresh("test");

        final AtomicInteger preIndex = new AtomicInteger();
        final AtomicInteger postIndex = new AtomicInteger();
        final AtomicInteger preDelete = new AtomicInteger();
        final AtomicInteger postDelete = new AtomicInteger();
        IndexingOperationListener listener = new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                preIndex.incrementAndGet();
                return operation;
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
                postIndex.incrementAndGet();
            }

            @Override
            public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
                preDelete.incrementAndGet();
                return delete;
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
                postDelete.incrementAndGet();

            }
        };
        final IndexShard newShard = reinitShard(shard, listener);
        recoverShardFromStore(newShard);
        IndexingStats indexingStats = newShard.indexingStats();
        // ensure we are not influencing the indexing stats
        assertEquals(0, indexingStats.getTotal().getDeleteCount());
        assertEquals(0, indexingStats.getTotal().getDeleteCurrent());
        assertEquals(0, indexingStats.getTotal().getIndexCount());
        assertEquals(0, indexingStats.getTotal().getIndexCurrent());
        assertEquals(0, indexingStats.getTotal().getIndexFailedCount());
        assertEquals(2, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(1, preDelete.get());
        assertEquals(1, postDelete.get());

        closeShards(newShard);
    }

    public void testSearchIsReleaseIfWrapperFails() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0", "{\"foo\" : \"bar\"}");
        shard.refresh("test");
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = reader -> { throw new RuntimeException("boom"); };

        closeShards(shard);
        IndexShard newShard = newShard(
            ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.ExistingStoreRecoverySource.INSTANCE),
            shard.shardPath(),
            shard.indexSettings().getIndexMetadata(),
            null,
            wrapper,
            new InternalEngineFactory(),
            () -> {},
            RetentionLeaseSyncer.EMPTY,
            EMPTY_EVENT_LISTENER
        );

        recoverShardFromStore(newShard);

        try {
            newShard.acquireSearcher("test");
            fail("exception expected");
        } catch (RuntimeException ex) {
            //
        }
        closeShards(newShard);
    }

    public void testTranslogRecoverySyncsTranslog() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text"}}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);

        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        IndexShard replica = newShard(primary.shardId(), false, "n2", metadata, null);
        recoverReplica(replica, primary, (shard, discoveryNode) -> new RecoveryTarget(shard, discoveryNode, null, null, recoveryListener) {
            @Override
            public void indexTranslogOperations(
                final List<Translog.Operation> operations,
                final int totalTranslogOps,
                final long maxSeenAutoIdTimestamp,
                final long maxSeqNoOfUpdatesOrDeletes,
                final RetentionLeases retentionLeases,
                final long mappingVersion,
                final ActionListener<Long> listener
            ) {
                super.indexTranslogOperations(
                    operations,
                    totalTranslogOps,
                    maxSeenAutoIdTimestamp,
                    maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases,
                    mappingVersion,
                    ActionListener.wrap(r -> {
                        assertFalse(replica.isSyncNeeded());
                        listener.onResponse(r);
                    }, listener::onFailure)
                );
            }
        }, true, true);

        closeShards(primary, replica);
    }

    public void testRecoverFromTranslog() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test")
            .putMapping("""
                { "properties": { "foo":  { "type": "text"}}}""")
            .settings(settings)
            .primaryTerm(0, randomLongBetween(1, Long.MAX_VALUE))
            .build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        List<Translog.Index> operations = new ArrayList<>();
        int numTotalEntries = randomIntBetween(0, 10);
        int numCorruptEntries = 0;
        for (int i = 0; i < numTotalEntries; i++) {
            if (randomBoolean()) {
                operations.add(
                    new Translog.Index(
                        "1",
                        0,
                        primary.getPendingPrimaryTerm(),
                        1,
                        "{\"foo\" : \"bar\"}".getBytes(Charset.forName("UTF-8")),
                        null,
                        -1
                    )
                );
            } else {
                // corrupt entry
                operations.add(
                    new Translog.Index(
                        "2",
                        1,
                        primary.getPendingPrimaryTerm(),
                        1,
                        "{\"foo\" : \"bar}".getBytes(Charset.forName("UTF-8")),
                        null,
                        -1
                    )
                );
                numCorruptEntries++;
            }
        }
        Translog.Snapshot snapshot = TestTranslog.newSnapshotFromOperations(operations);
        primary.markAsRecovering(
            "store",
            new RecoveryState(primary.routingEntry(), getFakeDiscoNode(primary.routingEntry().currentNodeId()), null)
        );
        recoverFromStore(primary);

        primary.recoveryState().getTranslog().totalOperations(snapshot.totalOperations());
        primary.recoveryState().getTranslog().totalOperationsOnStart(snapshot.totalOperations());
        primary.state = IndexShardState.RECOVERING; // translog recovery on the next line would otherwise fail as we are in POST_RECOVERY
        primary.runTranslogRecovery(
            primary.getEngine(),
            snapshot,
            Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
            primary.recoveryState().getTranslog()::incrementRecoveredOperations
        );
        assertThat(primary.recoveryState().getTranslog().recoveredOperations(), equalTo(numTotalEntries - numCorruptEntries));

        closeShards(primary);
    }

    public void testShardActiveDuringInternalRecovery() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "_doc", "0");
        shard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        shard.markAsRecovering("for testing", new RecoveryState(shard.routingEntry(), localNode, null));
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(shard.isActive());
        shard.prepareForIndexRecovery();
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(shard.isActive());
        shard.recoveryState().getIndex().setFileDetailsComplete();
        shard.openEngineAndRecoverFromTranslog();
        // Shard should now be active since we did recover:
        assertTrue(shard.isActive());
        closeShards(shard);
    }

    public void testShardActiveDuringPeerRecovery() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text"}}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);

        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        IndexShard replica = newShard(primary.shardId(), false, "n2", metadata, null);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(replica.isActive());
        recoverReplica(replica, primary, (shard, discoveryNode) -> new RecoveryTarget(shard, discoveryNode, null, null, recoveryListener) {
            @Override
            public void indexTranslogOperations(
                final List<Translog.Operation> operations,
                final int totalTranslogOps,
                final long maxAutoIdTimestamp,
                final long maxSeqNoOfUpdatesOrDeletes,
                final RetentionLeases retentionLeases,
                final long mappingVersion,
                final ActionListener<Long> listener
            ) {
                super.indexTranslogOperations(
                    operations,
                    totalTranslogOps,
                    maxAutoIdTimestamp,
                    maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases,
                    mappingVersion,
                    ActionListener.wrap(checkpoint -> {
                        listener.onResponse(checkpoint);
                        // Shard should now be active since we did recover:
                        assertTrue(replica.isActive());
                    }, listener::onFailure)
                );
            }
        }, false, true);

        closeShards(primary, replica);
    }

    public void testRefreshListenersDuringPeerRecovery() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text"}}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);

        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        Consumer<IndexShard> assertListenerCalled = shard -> {
            AtomicBoolean called = new AtomicBoolean();
            shard.addRefreshListener(null, b -> {
                assertFalse(b);
                called.set(true);
            });

            PlainActionFuture<Void> listener = PlainActionFuture.newFuture();
            shard.addRefreshListener(10, listener);
            expectThrows(IllegalIndexShardStateException.class, listener::actionGet);
        };
        IndexShard replica = newShard(primary.shardId(), false, "n2", metadata, null);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        assertListenerCalled.accept(replica);
        recoverReplica(replica, primary, (shard, discoveryNode) -> new RecoveryTarget(shard, discoveryNode, null, null, recoveryListener) {
            // we're only checking that listeners are called when the engine is open, before there is no point
            @Override
            public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
                super.prepareForTranslogOperations(totalTranslogOps, ActionListener.wrap(r -> {
                    assertListenerCalled.accept(replica);
                    listener.onResponse(r);
                }, listener::onFailure));
            }

            @Override
            public void indexTranslogOperations(
                final List<Translog.Operation> operations,
                final int totalTranslogOps,
                final long maxAutoIdTimestamp,
                final long maxSeqNoOfUpdatesOrDeletes,
                final RetentionLeases retentionLeases,
                final long mappingVersion,
                final ActionListener<Long> listener
            ) {
                super.indexTranslogOperations(
                    operations,
                    totalTranslogOps,
                    maxAutoIdTimestamp,
                    maxSeqNoOfUpdatesOrDeletes,
                    retentionLeases,
                    mappingVersion,
                    ActionListener.wrap(r -> {
                        assertListenerCalled.accept(replica);
                        listener.onResponse(r);
                    }, listener::onFailure)
                );
            }

            @Override
            public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
                super.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, ActionListener.wrap(r -> {
                    assertListenerCalled.accept(replica);
                    listener.onResponse(r);
                }, listener::onFailure));
            }
        }, false, true);

        closeShards(primary, replica);
    }

    public void testRecoverFromLocalShard() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("source")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\"}}}")
            .settings(settings)
            .primaryTerm(0, 1)
            .build();

        IndexShard sourceShard = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(sourceShard);

        indexDoc(sourceShard, "_doc", "0", "{\"foo\" : \"bar\"}");
        indexDoc(sourceShard, "_doc", "1", "{\"foo\" : \"bar\"}");
        sourceShard.refresh("test");

        ShardRouting targetRouting = newShardRouting(
            new ShardId("index_1", "index_1", 0),
            "n1",
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.LocalShardsRecoverySource.INSTANCE
        );

        final IndexShard targetShard;
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Map<String, MappingMetadata> requestedMappingUpdates = ConcurrentCollections.newConcurrentMap();
        {
            targetShard = newShard(targetRouting);
            targetShard.markAsRecovering("store", new RecoveryState(targetShard.routingEntry(), localNode, null));

            Consumer<MappingMetadata> mappingConsumer = mapping -> { assertNull(requestedMappingUpdates.put("_doc", mapping)); };

            final IndexShard differentIndex = newShard(new ShardId("index_2", "index_2", 0), true);
            recoverShardFromStore(differentIndex);
            expectThrows(IllegalArgumentException.class, () -> {
                final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
                targetShard.recoverFromLocalShards(mappingConsumer, Arrays.asList(sourceShard, differentIndex), future);
                future.actionGet();
            });
            closeShards(differentIndex);

            final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
            targetShard.recoverFromLocalShards(mappingConsumer, Arrays.asList(sourceShard), future);
            assertTrue(future.actionGet());
            RecoveryState recoveryState = targetShard.recoveryState();
            assertEquals(RecoveryState.Stage.DONE, recoveryState.getStage());
            assertTrue(recoveryState.getIndex().fileDetails().size() > 0);
            for (RecoveryState.FileDetail file : recoveryState.getIndex().fileDetails()) {
                if (file.reused()) {
                    assertEquals(file.recovered(), 0);
                } else {
                    assertEquals(file.recovered(), file.length());
                }
            }
            // check that local checkpoint of new primary is properly tracked after recovery
            assertThat(targetShard.getLocalCheckpoint(), equalTo(1L));
            assertThat(targetShard.getReplicationTracker().getGlobalCheckpoint(), equalTo(1L));
            IndexShardTestCase.updateRoutingEntry(targetShard, ShardRoutingHelper.moveToStarted(targetShard.routingEntry()));
            assertThat(
                targetShard.getReplicationTracker()
                    .getTrackedLocalCheckpointForShard(targetShard.routingEntry().allocationId().getId())
                    .getLocalCheckpoint(),
                equalTo(1L)
            );
            assertDocCount(targetShard, 2);
        }
        // now check that it's persistent ie. that the added shards are committed
        {
            final IndexShard newShard = reinitShard(targetShard);
            recoverShardFromStore(newShard);
            assertDocCount(newShard, 2);
            closeShards(newShard);
        }

        assertThat(requestedMappingUpdates, hasKey("_doc"));
        assertThat(requestedMappingUpdates.get("_doc").source().string(), equalTo("""
            {"properties":{"foo":{"type":"text"}}}"""));

        closeShards(sourceShard, targetShard);
    }

    public void testCompletionStatsMarksSearcherAccessed() throws Exception {
        IndexShard indexShard = null;
        try {
            indexShard = newStartedShard();
            IndexShard shard = indexShard;
            assertBusy(() -> {
                ThreadPool threadPool = shard.getThreadPool();
                assertThat(threadPool.relativeTimeInMillis(), greaterThan(shard.getLastSearcherAccess()));
            });
            long prevAccessTime = shard.getLastSearcherAccess();
            indexShard.completionStats();
            assertThat("searcher was marked as accessed", shard.getLastSearcherAccess(), equalTo(prevAccessTime));
        } finally {
            closeShards(indexShard);
        }
    }

    public void testDocStats() throws Exception {
        IndexShard indexShard = null;
        try {
            indexShard = newStartedShard(
                false,
                Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0).build()
            );
            final long numDocs = randomIntBetween(2, 32); // at least two documents so we have docs to delete
            final long numDocsToDelete = randomLongBetween(1, numDocs);
            for (int i = 0; i < numDocs; i++) {
                final String id = Integer.toString(i);
                indexDoc(indexShard, "_doc", id);
            }
            if (randomBoolean()) {
                indexShard.refresh("test");
            } else {
                indexShard.flush(new FlushRequest());
            }
            {
                IndexShard shard = indexShard;
                assertBusy(() -> {
                    ThreadPool threadPool = shard.getThreadPool();
                    assertThat(threadPool.relativeTimeInMillis(), greaterThan(shard.getLastSearcherAccess()));
                });
                long prevAccessTime = shard.getLastSearcherAccess();
                final DocsStats docsStats = indexShard.docStats();
                assertThat("searcher was marked as accessed", shard.getLastSearcherAccess(), equalTo(prevAccessTime));
                assertThat(docsStats.getCount(), equalTo(numDocs));
                try (Engine.Searcher searcher = indexShard.acquireSearcher("test")) {
                    assertTrue(searcher.getIndexReader().numDocs() <= docsStats.getCount());
                }
                assertThat(docsStats.getDeleted(), equalTo(0L));
                assertThat(docsStats.getTotalSizeInBytes(), greaterThan(0L));
            }

            final List<Integer> ids = randomSubsetOf(
                Math.toIntExact(numDocsToDelete),
                IntStream.range(0, Math.toIntExact(numDocs)).boxed().toList()
            );
            for (final Integer i : ids) {
                final String id = Integer.toString(i);
                deleteDoc(indexShard, id);
                indexDoc(indexShard, "_doc", id);
            }
            // Need to update and sync the global checkpoint and the retention leases for the soft-deletes retention MergePolicy.
            final long newGlobalCheckpoint = indexShard.getLocalCheckpoint();
            if (indexShard.routingEntry().primary()) {
                indexShard.updateLocalCheckpointForShard(indexShard.routingEntry().allocationId().getId(), indexShard.getLocalCheckpoint());
                indexShard.updateGlobalCheckpointForShard(
                    indexShard.routingEntry().allocationId().getId(),
                    indexShard.getLocalCheckpoint()
                );
                indexShard.syncRetentionLeases();
            } else {
                indexShard.updateGlobalCheckpointOnReplica(newGlobalCheckpoint, "test");

                final RetentionLeases retentionLeases = indexShard.getRetentionLeases();
                indexShard.updateRetentionLeasesOnReplica(
                    new RetentionLeases(
                        retentionLeases.primaryTerm(),
                        retentionLeases.version() + 1,
                        retentionLeases.leases()
                            .stream()
                            .map(
                                lease -> new RetentionLease(
                                    lease.id(),
                                    newGlobalCheckpoint + 1,
                                    lease.timestamp(),
                                    ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
                                )
                            )
                            .toList()
                    )
                );
            }
            indexShard.sync();
            // flush the buffered deletes
            final FlushRequest flushRequest = new FlushRequest();
            flushRequest.force(false);
            flushRequest.waitIfOngoing(false);
            indexShard.flush(flushRequest);

            if (randomBoolean()) {
                indexShard.refresh("test");
            }
            {
                final DocsStats docStats = indexShard.docStats();
                try (Engine.Searcher searcher = indexShard.acquireSearcher("test")) {
                    assertTrue(searcher.getIndexReader().numDocs() <= docStats.getCount());
                }
                assertThat(docStats.getCount(), equalTo(numDocs));
            }

            // merge them away
            final ForceMergeRequest forceMergeRequest = new ForceMergeRequest();
            forceMergeRequest.maxNumSegments(1);
            indexShard.forceMerge(forceMergeRequest);

            if (randomBoolean()) {
                indexShard.refresh("test");
            } else {
                indexShard.flush(new FlushRequest());
            }
            {
                final DocsStats docStats = indexShard.docStats();
                assertThat(docStats.getCount(), equalTo(numDocs));
                assertThat(docStats.getDeleted(), equalTo(0L));
                assertThat(docStats.getTotalSizeInBytes(), greaterThan(0L));
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testEstimateTotalDocSize() throws Exception {
        IndexShard indexShard = null;
        try {
            indexShard = newStartedShard(true);

            int numDoc = randomIntBetween(100, 200);
            for (int i = 0; i < numDoc; i++) {
                String doc = Strings.toString(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .field("count", randomInt())
                        .field("point", randomFloat())
                        .field("description", randomUnicodeOfCodepointLength(100))
                        .endObject()
                );
                indexDoc(indexShard, "_doc", Integer.toString(i), doc);
            }

            assertThat("Without flushing, segment sizes should be zero", indexShard.docStats().getTotalSizeInBytes(), equalTo(0L));

            if (randomBoolean()) {
                indexShard.flush(new FlushRequest());
            } else {
                indexShard.refresh("test");
            }
            {
                final DocsStats docsStats = indexShard.docStats();
                final StoreStats storeStats = indexShard.storeStats();
                assertThat(storeStats.sizeInBytes(), greaterThan(numDoc * 100L)); // A doc should be more than 100 bytes.

                assertThat(
                    "Estimated total document size is too small compared with the stored size",
                    docsStats.getTotalSizeInBytes(),
                    greaterThanOrEqualTo(storeStats.sizeInBytes() * 80 / 100)
                );
                assertThat(
                    "Estimated total document size is too large compared with the stored size",
                    docsStats.getTotalSizeInBytes(),
                    lessThanOrEqualTo(storeStats.sizeInBytes() * 120 / 100)
                );
            }

            // Do some updates and deletes, then recheck the correlation again.
            for (int i = 0; i < numDoc / 2; i++) {
                if (randomBoolean()) {
                    deleteDoc(indexShard, Integer.toString(i));
                } else {
                    indexDoc(indexShard, "_doc", Integer.toString(i), "{\"foo\": \"bar\"}");
                }
            }
            if (randomBoolean()) {
                indexShard.flush(new FlushRequest());
            } else {
                indexShard.refresh("test");
            }
            {
                final DocsStats docsStats = indexShard.docStats();
                final StoreStats storeStats = indexShard.storeStats();
                assertThat(
                    "Estimated total document size is too small compared with the stored size",
                    docsStats.getTotalSizeInBytes(),
                    greaterThanOrEqualTo(storeStats.sizeInBytes() * 80 / 100)
                );
                assertThat(
                    "Estimated total document size is too large compared with the stored size",
                    docsStats.getTotalSizeInBytes(),
                    lessThanOrEqualTo(storeStats.sizeInBytes() * 120 / 100)
                );
            }

        } finally {
            closeShards(indexShard);
        }
    }

    /**
     * here we are simulating the scenario that happens when we do async shard fetching from GatewaySerivce while we are finishing
     * a recovery and concurrently clean files. This should always be possible without any exception. Yet there was a bug where IndexShard
     * acquired the index writer lock before it called into the store that has it's own locking for metadata reads
     */
    public void testReadSnapshotConcurrently() throws IOException, InterruptedException {
        IndexShard indexShard = newStartedShard();
        indexDoc(indexShard, "_doc", "0", "{}");
        if (randomBoolean()) {
            indexShard.refresh("test");
        }
        indexDoc(indexShard, "_doc", "1", "{}");
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final IndexShard newShard = reinitShard(indexShard);
        Store.MetadataSnapshot storeFileMetadatas = newShard.snapshotStoreMetadata();
        assertTrue("at least 2 files, commit and data: " + storeFileMetadatas.toString(), storeFileMetadatas.size() > 1);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        expectThrows(AlreadyClosedException.class, () -> newShard.getEngine()); // no engine
        Thread thread = new Thread(() -> {
            latch.countDown();
            while (stop.get() == false) {
                try {
                    Store.MetadataSnapshot readMeta = newShard.snapshotStoreMetadata();
                    assertEquals(0, storeFileMetadatas.recoveryDiff(readMeta).different.size());
                    assertEquals(0, storeFileMetadatas.recoveryDiff(readMeta).missing.size());
                    assertEquals(storeFileMetadatas.size(), storeFileMetadatas.recoveryDiff(readMeta).identical.size());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        });
        thread.start();
        latch.await();

        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            newShard.store().cleanupAndVerify("test", storeFileMetadatas);
        }
        assertTrue(stop.compareAndSet(false, true));
        thread.join();
        closeShards(newShard);
    }

    public void testIndexCheckOnStartup() throws Exception {
        final IndexShard indexShard = newStartedShard(true);

        final long numDocs = between(10, 100);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, "_doc", Long.toString(i), "{}");
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final ShardPath shardPath = indexShard.shardPath();

        final Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);
        CorruptionUtils.corruptIndex(random(), indexPath, false);

        final AtomicInteger corruptedMarkerCount = new AtomicInteger();
        final SimpleFileVisitor<Path> corruptedVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isRegularFile(file) && file.getFileName().toString().startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                    corruptedMarkerCount.incrementAndGet();
                }
                return FileVisitResult.CONTINUE;
            }
        };
        Files.walkFileTree(indexPath, corruptedVisitor);

        assertThat("corruption marker should not be there", corruptedMarkerCount.get(), equalTo(0));

        final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
            indexShard.routingEntry(),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        // start shard and perform index check on startup. It enforce shard to fail due to corrupted index files
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexShard.indexSettings().getIndexMetadata())
            .settings(
                Settings.builder()
                    .put(indexShard.indexSettings.getSettings())
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom("true", "checksum"))
            )
            .build();

        IndexShard corruptedShard = newShard(
            shardRouting,
            shardPath,
            indexMetadata,
            null,
            null,
            indexShard.engineFactory,
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );

        final MockLogAppender appender = new MockLogAppender();
        appender.start();
        Loggers.addAppender(LogManager.getLogger(IndexShard.class), appender);
        try {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "expensive checks warning",
                    "org.elasticsearch.index.shard.IndexShard",
                    Level.WARN,
                    "performing expensive diagnostic checks during shard startup [index.shard.check_on_startup=*]; these checks "
                        + "should only be enabled temporarily, you must remove this index setting as soon as possible"
                )
            );

            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "failure message",
                    "org.elasticsearch.index.shard.IndexShard",
                    Level.WARN,
                    "check index [failure]*"
                )
            );

            final IndexShardRecoveryException indexShardRecoveryException = expectThrows(
                IndexShardRecoveryException.class,
                () -> newStartedShard(p -> corruptedShard, true)
            );
            assertThat(indexShardRecoveryException.getMessage(), equalTo("failed recovery"));

            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(LogManager.getLogger(IndexShard.class), appender);
            appender.stop();
        }

        // check that corrupt marker is there
        Files.walkFileTree(indexPath, corruptedVisitor);
        assertThat("store has to be marked as corrupted", corruptedMarkerCount.get(), equalTo(1));

        // Close the directory under the shard first because it's probably a MockDirectoryWrapper which throws exceptions when corrupt
        try {
            ((FilterDirectory) corruptedShard.store().directory()).getDelegate().close();
        } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException | RuntimeException e) {
            // ignored
        }

        closeShards(corruptedShard);
    }

    public void testShardDoesNotStartIfCorruptedMarkerIsPresent() throws Exception {
        final IndexShard indexShard = newStartedShard(true);

        final long numDocs = between(10, 100);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, "_doc", Long.toString(i), "{}");
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final ShardPath shardPath = indexShard.shardPath();

        final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
            indexShard.routingEntry(),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        final IndexMetadata indexMetadata = indexShard.indexSettings().getIndexMetadata();

        final Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);

        // create corrupted marker
        final String corruptionMessage = "fake ioexception";
        try (Store store = createStore(indexShard.indexSettings(), shardPath)) {
            store.markStoreCorrupted(new IOException(corruptionMessage));
        }

        // try to start shard on corrupted files
        final IndexShard corruptedShard = newShard(
            shardRouting,
            shardPath,
            indexMetadata,
            null,
            null,
            indexShard.engineFactory,
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );

        final IndexShardRecoveryException exception1 = expectThrows(
            IndexShardRecoveryException.class,
            () -> newStartedShard(p -> corruptedShard, true)
        );
        assertThat(exception1.getCause().getMessage(), equalTo(corruptionMessage + " (resource=preexisting_corruption)"));
        closeShards(corruptedShard);

        final AtomicInteger corruptedMarkerCount = new AtomicInteger();
        final SimpleFileVisitor<Path> corruptedVisitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isRegularFile(file) && file.getFileName().toString().startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                    corruptedMarkerCount.incrementAndGet();
                }
                return FileVisitResult.CONTINUE;
            }
        };
        Files.walkFileTree(indexPath, corruptedVisitor);
        assertThat("store has to be marked as corrupted", corruptedMarkerCount.get(), equalTo(1));

        // try to start another time shard on corrupted files
        final IndexShard corruptedShard2 = newShard(
            shardRouting,
            shardPath,
            indexMetadata,
            null,
            null,
            indexShard.engineFactory,
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );

        final IndexShardRecoveryException exception2 = expectThrows(
            IndexShardRecoveryException.class,
            () -> newStartedShard(p -> corruptedShard2, true)
        );
        assertThat(exception2.getCause().getMessage(), equalTo(corruptionMessage + " (resource=preexisting_corruption)"));
        closeShards(corruptedShard2);

        // check that corrupt marker is there
        corruptedMarkerCount.set(0);
        Files.walkFileTree(indexPath, corruptedVisitor);
        assertThat("store still has a single corrupt marker", corruptedMarkerCount.get(), equalTo(1));
    }

    /**
     * Simulates a scenario that happens when we are async fetching snapshot metadata from GatewayService
     * and checking index concurrently. This should always be possible without any exception.
     */
    public void testReadSnapshotAndCheckIndexConcurrently() throws Exception {
        final boolean isPrimary = randomBoolean();
        IndexShard indexShard = newStartedShard(isPrimary);
        final long numDocs = between(10, 100);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, "_doc", Long.toString(i), "{}");
            if (randomBoolean()) {
                indexShard.refresh("test");
            }
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
            indexShard.routingEntry(),
            isPrimary ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE
        );
        final IndexMetadata indexMetadata = IndexMetadata.builder(indexShard.indexSettings().getIndexMetadata())
            .settings(
                Settings.builder()
                    .put(indexShard.indexSettings.getSettings())
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom("false", "true", "checksum"))
            )
            .build();
        final IndexShard newShard = newShard(
            shardRouting,
            indexShard.shardPath(),
            indexMetadata,
            null,
            null,
            indexShard.engineFactory,
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );

        Store.MetadataSnapshot storeFileMetadatas = newShard.snapshotStoreMetadata();
        assertTrue("at least 2 files, commit and data: " + storeFileMetadatas.toString(), storeFileMetadatas.size() > 1);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        Thread snapshotter = new Thread(() -> {
            latch.countDown();
            while (stop.get() == false) {
                try {
                    Store.MetadataSnapshot readMeta = newShard.snapshotStoreMetadata();
                    assertThat(readMeta.numDocs(), equalTo(numDocs));
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).different.size(), equalTo(0));
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).missing.size(), equalTo(0));
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).identical.size(), equalTo(storeFileMetadatas.size()));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        });
        snapshotter.start();

        if (isPrimary) {
            newShard.markAsRecovering(
                "store",
                new RecoveryState(newShard.routingEntry(), getFakeDiscoNode(newShard.routingEntry().currentNodeId()), null)
            );
        } else {
            newShard.markAsRecovering(
                "peer",
                new RecoveryState(
                    newShard.routingEntry(),
                    getFakeDiscoNode(newShard.routingEntry().currentNodeId()),
                    getFakeDiscoNode(newShard.routingEntry().currentNodeId())
                )
            );
        }
        int iters = iterations(10, 100);
        latch.await();
        for (int i = 0; i < iters; i++) {
            newShard.checkIndex();
        }
        assertTrue(stop.compareAndSet(false, true));
        snapshotter.join();
        closeShards(newShard);
    }

    class Result {
        private final int localCheckpoint;
        private final int maxSeqNo;

        Result(final int localCheckpoint, final int maxSeqNo) {
            this.localCheckpoint = localCheckpoint;
            this.maxSeqNo = maxSeqNo;
        }
    }

    /**
     * Index on the specified shard while introducing sequence number gaps.
     *
     * @param indexShard the shard
     * @param operations the number of operations
     * @param offset     the starting sequence number
     * @return a pair of the maximum sequence number and whether or not a gap was introduced
     * @throws IOException if an I/O exception occurs while indexing on the shard
     */
    private Result indexOnReplicaWithGaps(final IndexShard indexShard, final int operations, final int offset) throws IOException {
        int localCheckpoint = offset;
        int max = offset;
        boolean gap = false;
        Set<String> ids = new HashSet<>();
        for (int i = offset + 1; i < operations; i++) {
            if (rarely() == false || i == operations - 1) { // last operation can't be a gap as it's not a gap anymore
                final String id = ids.isEmpty() || randomBoolean() ? Integer.toString(i) : randomFrom(ids);
                if (ids.add(id) == false) { // this is an update
                    indexShard.advanceMaxSeqNoOfUpdatesOrDeletes(i);
                }
                SourceToParse sourceToParse = new SourceToParse(id, new BytesArray("{}"), XContentType.JSON);
                indexShard.applyIndexOperationOnReplica(
                    i,
                    indexShard.getOperationPrimaryTerm(),
                    1,
                    IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                    false,
                    sourceToParse
                );
                if (gap == false && i == localCheckpoint + 1) {
                    localCheckpoint++;
                }
                max = i;
            } else {
                gap = true;
            }
            if (rarely()) {
                indexShard.flush(new FlushRequest());
            }
        }
        indexShard.sync(); // advance local checkpoint
        assert localCheckpoint == indexShard.getLocalCheckpoint();
        assert gap == false || (localCheckpoint != max);
        return new Result(localCheckpoint, max);
    }

    public void testIsSearchIdle() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text"}}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        assertTrue(primary.getEngine().refreshNeeded());
        assertTrue(primary.scheduledRefresh());
        assertFalse(primary.isSearchIdle());

        IndexScopedSettings scopedSettings = primary.indexSettings().getScopedSettings();
        settings = Settings.builder().put(settings).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO).build();
        scopedSettings.applySettings(settings);
        assertTrue(primary.isSearchIdle());

        settings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.timeValueMinutes(1))
            .build();
        scopedSettings.applySettings(settings);
        assertFalse(primary.isSearchIdle());

        settings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.timeValueMillis(10))
            .build();
        scopedSettings.applySettings(settings);

        assertBusy(() -> assertTrue(primary.isSearchIdle()));
        do {
            // now loop until we are fast enough... shouldn't take long
            primary.awaitShardSearchActive(aBoolean -> {});
        } while (primary.isSearchIdle());

        assertBusy(() -> assertTrue(primary.isSearchIdle()));
        do {
            // now loop until we are fast enough... shouldn't take long
            primary.acquireSearcher("test").close();
        } while (primary.isSearchIdle());
        closeShards(primary);
    }

    public void testScheduledRefresh() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text"}}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        assertTrue(primary.getEngine().refreshNeeded());
        assertTrue(primary.scheduledRefresh());
        IndexScopedSettings scopedSettings = primary.indexSettings().getScopedSettings();
        settings = Settings.builder().put(settings).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO).build();
        scopedSettings.applySettings(settings);

        assertFalse(primary.getEngine().refreshNeeded());
        indexDoc(primary, "_doc", "1", "{\"foo\" : \"bar\"}");
        assertTrue(primary.getEngine().refreshNeeded());
        long lastSearchAccess = primary.getLastSearcherAccess();
        assertFalse(primary.scheduledRefresh());
        assertEquals(lastSearchAccess, primary.getLastSearcherAccess());
        // wait until the thread-pool has moved the timestamp otherwise we can't assert on this below
        assertBusy(() -> assertThat(primary.getThreadPool().relativeTimeInMillis(), greaterThan(lastSearchAccess)));
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            primary.awaitShardSearchActive(refreshed -> {
                assertTrue(refreshed);
                try (Engine.Searcher searcher = primary.acquireSearcher("test")) {
                    assertEquals(2, searcher.getIndexReader().numDocs());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertNotEquals(
            "awaitShardSearchActive must access a searcher to remove search idle state",
            lastSearchAccess,
            primary.getLastSearcherAccess()
        );
        assertTrue(lastSearchAccess < primary.getLastSearcherAccess());
        try (Engine.Searcher searcher = primary.acquireSearcher("test")) {
            assertEquals(1, searcher.getIndexReader().numDocs());
        }
        assertTrue(primary.getEngine().refreshNeeded());
        assertTrue(primary.scheduledRefresh());
        latch.await();
        CountDownLatch latch1 = new CountDownLatch(1);
        primary.awaitShardSearchActive(refreshed -> {
            assertFalse(refreshed);
            try (Engine.Searcher searcher = primary.acquireSearcher("test")) {
                assertEquals(2, searcher.getIndexReader().numDocs());
            } finally {
                latch1.countDown();
            }

        });
        latch1.await();

        indexDoc(primary, "_doc", "2", "{\"foo\" : \"bar\"}");
        assertFalse(primary.scheduledRefresh());
        assertTrue(primary.isSearchIdle());
        primary.flushOnIdle(0);
        assertTrue(primary.scheduledRefresh()); // make sure we refresh once the shard is inactive
        try (Engine.Searcher searcher = primary.acquireSearcher("test")) {
            assertEquals(3, searcher.getIndexReader().numDocs());
        }
        closeShards(primary);
    }

    public void testRefreshIsNeededWithRefreshListeners() throws IOException, InterruptedException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test").putMapping("""
            { "properties": { "foo":  { "type": "text"}}}""").settings(settings).primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        indexDoc(primary, "_doc", "0", "{\"foo\" : \"bar\"}");
        assertTrue(primary.getEngine().refreshNeeded());
        assertTrue(primary.scheduledRefresh());
        Engine.IndexResult doc = indexDoc(primary, "_doc", "1", "{\"foo\" : \"bar\"}");
        CountDownLatch latch = new CountDownLatch(1);
        if (randomBoolean()) {
            primary.addRefreshListener(doc.getTranslogLocation(), r -> latch.countDown());
        } else {
            primary.addRefreshListener(doc.getSeqNo(), new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
        assertEquals(1, latch.getCount());
        assertTrue(primary.getEngine().refreshNeeded());
        assertTrue(primary.scheduledRefresh());
        latch.await();

        IndexScopedSettings scopedSettings = primary.indexSettings().getScopedSettings();
        settings = Settings.builder().put(settings).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO).build();
        scopedSettings.applySettings(settings);

        doc = indexDoc(primary, "_doc", "2", "{\"foo\" : \"bar\"}");
        CountDownLatch latch1 = new CountDownLatch(1);
        if (randomBoolean()) {
            primary.addRefreshListener(doc.getTranslogLocation(), r -> latch1.countDown());
        } else {
            primary.addRefreshListener(doc.getSeqNo(), ActionListener.wrap(latch1::countDown));
        }
        assertEquals(1, latch1.getCount());
        assertTrue(primary.getEngine().refreshNeeded());
        assertTrue(primary.scheduledRefresh());
        latch1.await();
        closeShards(primary);
    }

    public void testFlushOnIdle() throws Exception {
        IndexShard shard = newStartedShard();
        for (int i = 0; i < 3; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
            shard.refresh("test"); // produce segments
        }
        List<Segment> segments = shard.segments();
        Set<String> names = new HashSet<>();
        for (Segment segment : segments) {
            assertFalse(segment.committed);
            assertTrue(segment.search);
            names.add(segment.getName());
        }
        assertEquals(3, segments.size());
        shard.flush(new FlushRequest());
        shard.forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(false));
        shard.refresh("test");
        segments = shard.segments();
        for (Segment segment : segments) {
            if (names.contains(segment.getName())) {
                assertTrue(segment.committed);
                assertFalse(segment.search);
            } else {
                assertFalse(segment.committed);
                assertTrue(segment.search);
            }
        }
        assertEquals(4, segments.size());

        shard.flushOnIdle(0);
        assertFalse(shard.isActive());

        assertBusy(() -> { // flush happens in the background using the flush threadpool
            List<Segment> segmentsAfterFlush = shard.segments();
            assertEquals(1, segmentsAfterFlush.size());
            for (Segment segment : segmentsAfterFlush) {
                assertTrue(segment.committed);
                assertTrue(segment.search);
            }
        });
        closeShards(shard);
    }

    public void testOnCloseStats() throws IOException {
        final IndexShard indexShard = newStartedShard(true);

        for (int i = 0; i < 3; i++) {
            indexDoc(indexShard, "_doc", "" + i, "{\"foo\" : \"" + randomAlphaOfLength(10) + "\"}");
            indexShard.refresh("test"); // produce segments
        }

        // check stats on closed and on opened shard
        if (randomBoolean()) {
            closeShards(indexShard);

            expectThrows(AlreadyClosedException.class, () -> indexShard.seqNoStats());
            expectThrows(AlreadyClosedException.class, () -> indexShard.commitStats());
            expectThrows(AlreadyClosedException.class, () -> indexShard.storeStats());

        } else {
            final SeqNoStats seqNoStats = indexShard.seqNoStats();
            assertThat(seqNoStats.getLocalCheckpoint(), equalTo(2L));

            final CommitStats commitStats = indexShard.commitStats();
            assertThat(commitStats.getGeneration(), equalTo(2L));

            final StoreStats storeStats = indexShard.storeStats();

            assertThat(storeStats.sizeInBytes(), greaterThan(0L));

            closeShards(indexShard);
        }

    }

    public void testSupplyTombstoneDoc() throws Exception {
        IndexShard shard = newStartedShard();
        String id = randomRealisticUnicodeOfLengthBetween(1, 10);
        ParsedDocument deleteTombstone = ParsedDocument.deleteTombstone(id);
        assertThat(deleteTombstone.docs(), hasSize(1));
        LuceneDocument deleteDoc = deleteTombstone.docs().get(0);
        assertThat(
            deleteDoc.getFields().stream().map(IndexableField::name).toList(),
            containsInAnyOrder(
                IdFieldMapper.NAME,
                VersionFieldMapper.NAME,
                SeqNoFieldMapper.NAME,
                SeqNoFieldMapper.NAME,
                SeqNoFieldMapper.PRIMARY_TERM_NAME,
                SeqNoFieldMapper.TOMBSTONE_NAME
            )
        );
        assertThat(deleteDoc.getField(IdFieldMapper.NAME).binaryValue(), equalTo(Uid.encodeId(id)));
        assertThat(deleteDoc.getField(SeqNoFieldMapper.TOMBSTONE_NAME).numericValue().longValue(), equalTo(1L));

        final String reason = randomUnicodeOfLength(200);
        ParsedDocument noopTombstone = ParsedDocument.noopTombstone(reason);
        assertThat(noopTombstone.docs(), hasSize(1));
        LuceneDocument noopDoc = noopTombstone.docs().get(0);
        assertThat(
            noopDoc.getFields().stream().map(IndexableField::name).toList(),
            containsInAnyOrder(
                VersionFieldMapper.NAME,
                SourceFieldMapper.NAME,
                SeqNoFieldMapper.TOMBSTONE_NAME,
                SeqNoFieldMapper.NAME,
                SeqNoFieldMapper.NAME,
                SeqNoFieldMapper.PRIMARY_TERM_NAME
            )
        );
        assertThat(noopDoc.getField(SeqNoFieldMapper.TOMBSTONE_NAME).numericValue().longValue(), equalTo(1L));
        assertThat(noopDoc.getField(SourceFieldMapper.NAME).binaryValue(), equalTo(new BytesRef(reason)));

        closeShards(shard);
    }

    public void testResetEngine() throws Exception {
        IndexShard shard = newStartedShard(false);
        indexOnReplicaWithGaps(shard, between(0, 1000), Math.toIntExact(shard.getLocalCheckpoint()));
        long maxSeqNoBeforeRollback = shard.seqNoStats().getMaxSeqNo();
        final long globalCheckpoint = randomLongBetween(shard.getLastKnownGlobalCheckpoint(), shard.getLocalCheckpoint());
        shard.updateGlobalCheckpointOnReplica(globalCheckpoint, "test");
        Set<String> docBelowGlobalCheckpoint = getShardDocUIDs(shard).stream()
            .filter(id -> Long.parseLong(id) <= globalCheckpoint)
            .collect(Collectors.toSet());
        TranslogStats translogStats = shard.translogStats();
        AtomicBoolean done = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            latch.countDown();
            int hitClosedExceptions = 0;
            while (done.get() == false) {
                try {
                    List<String> exposedDocIds = EngineTestCase.getDocIds(getEngine(shard), rarely())
                        .stream()
                        .map(DocIdSeqNoAndSource::id)
                        .toList();
                    assertThat(
                        "every operations before the global checkpoint must be reserved",
                        docBelowGlobalCheckpoint,
                        everyItem(is(in(exposedDocIds)))
                    );
                } catch (AlreadyClosedException ignored) {
                    hitClosedExceptions++;
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            // engine reference was switched twice: current read/write engine -> ready-only engine -> new read/write engine
            assertThat(hitClosedExceptions, lessThanOrEqualTo(2));
        });
        thread.start();
        latch.await();

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllReplicaOperationsPermits(shard.getOperationPrimaryTerm(), globalCheckpoint, 0L, ActionListener.wrap(r -> {
            try {
                shard.resetEngineToGlobalCheckpoint();
            } finally {
                r.close();
                engineResetLatch.countDown();
            }
        }, Assert::assertNotNull), TimeValue.timeValueMinutes(1L));
        engineResetLatch.await();
        assertThat(getShardDocUIDs(shard), equalTo(docBelowGlobalCheckpoint));
        assertThat(shard.seqNoStats().getMaxSeqNo(), equalTo(globalCheckpoint));
        if (shard.indexSettings.isSoftDeleteEnabled()) {
            // we might have trimmed some operations if the translog retention policy is ignored (when soft-deletes enabled).
            assertThat(shard.translogStats().estimatedNumberOfOperations(), lessThanOrEqualTo(translogStats.estimatedNumberOfOperations()));
        } else {
            assertThat(shard.translogStats().estimatedNumberOfOperations(), equalTo(translogStats.estimatedNumberOfOperations()));
        }
        assertThat(shard.getMaxSeqNoOfUpdatesOrDeletes(), equalTo(maxSeqNoBeforeRollback));
        done.set(true);
        thread.join();
        closeShard(shard, false);
    }

    /**
     * This test simulates a scenario seen rarely in ConcurrentSeqNoVersioningIT. Closing a shard while engine is inside
     * resetEngineToGlobalCheckpoint can lead to check index failure in integration tests.
     */
    public void testCloseShardWhileResettingEngine() throws Exception {
        CountDownLatch readyToCloseLatch = new CountDownLatch(1);
        CountDownLatch closeDoneLatch = new CountDownLatch(1);
        IndexShard shard = newStartedShard(false, Settings.EMPTY, config -> new InternalEngine(config) {
            @Override
            public InternalEngine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo)
                throws IOException {
                readyToCloseLatch.countDown();
                try {
                    closeDoneLatch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return super.recoverFromTranslog(translogRecoveryRunner, recoverUpToSeqNo);
            }
        });

        Thread closeShardThread = new Thread(() -> {
            try {
                readyToCloseLatch.await();
                shard.close("testing", false);
                // in integration tests, this is done as a listener on IndexService.
                MockFSDirectoryFactory.checkIndex(logger, shard.store(), shard.shardId);
            } catch (InterruptedException | IOException e) {
                throw new AssertionError(e);
            } finally {
                closeDoneLatch.countDown();
            }
        });

        closeShardThread.start();

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllReplicaOperationsPermits(
            shard.getOperationPrimaryTerm(),
            shard.getLastKnownGlobalCheckpoint(),
            0L,
            ActionListener.wrap(r -> {
                try (r) {
                    shard.resetEngineToGlobalCheckpoint();
                } finally {
                    engineResetLatch.countDown();
                }
            }, Assert::assertNotNull),
            TimeValue.timeValueMinutes(1L)
        );

        engineResetLatch.await();

        closeShardThread.join();

        // close store.
        closeShard(shard, false);
    }

    /**
     * This test simulates a scenario seen rarely in ConcurrentSeqNoVersioningIT. While engine is inside
     * resetEngineToGlobalCheckpoint snapshot metadata could fail
     */
    public void testSnapshotWhileResettingEngine() throws Exception {
        CountDownLatch readyToSnapshotLatch = new CountDownLatch(1);
        CountDownLatch snapshotDoneLatch = new CountDownLatch(1);
        IndexShard shard = newStartedShard(false, Settings.EMPTY, config -> new InternalEngine(config) {
            @Override
            public InternalEngine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo)
                throws IOException {
                InternalEngine internalEngine = super.recoverFromTranslog(translogRecoveryRunner, recoverUpToSeqNo);
                readyToSnapshotLatch.countDown();
                try {
                    snapshotDoneLatch.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return internalEngine;
            }
        });

        indexOnReplicaWithGaps(shard, between(0, 1000), Math.toIntExact(shard.getLocalCheckpoint()));
        final long globalCheckpoint = randomLongBetween(shard.getLastKnownGlobalCheckpoint(), shard.getLocalCheckpoint());
        shard.updateGlobalCheckpointOnReplica(globalCheckpoint, "test");

        Thread snapshotThread = new Thread(() -> {
            try {
                readyToSnapshotLatch.await();
                shard.snapshotStoreMetadata();
                try (Engine.IndexCommitRef indexCommitRef = shard.acquireLastIndexCommit(randomBoolean())) {
                    shard.store().getMetadata(indexCommitRef.getIndexCommit());
                }
                try (Engine.IndexCommitRef indexCommitRef = shard.acquireSafeIndexCommit()) {
                    shard.store().getMetadata(indexCommitRef.getIndexCommit());
                }
            } catch (InterruptedException | IOException e) {
                throw new AssertionError(e);
            } finally {
                snapshotDoneLatch.countDown();
            }
        });

        snapshotThread.start();

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllReplicaOperationsPermits(
            shard.getOperationPrimaryTerm(),
            shard.getLastKnownGlobalCheckpoint(),
            0L,
            ActionListener.wrap(r -> {
                try (r) {
                    shard.resetEngineToGlobalCheckpoint();
                } finally {
                    engineResetLatch.countDown();
                }
            }, Assert::assertNotNull),
            TimeValue.timeValueMinutes(1L)
        );

        engineResetLatch.await();

        snapshotThread.join();

        closeShard(shard, false);
    }

    public void testResetEngineWithBrokenTranslog() throws Exception {
        IndexShard shard = newStartedShard(false);
        updateMappings(
            shard,
            IndexMetadata.builder(shard.indexSettings.getIndexMetadata())
                .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\"}}}")
                .build()
        );
        final List<Translog.Index> operations = Stream.concat(
            IntStream.range(0, randomIntBetween(0, 10))
                .mapToObj(
                    n -> new Translog.Index(
                        "1",
                        0,
                        shard.getPendingPrimaryTerm(),
                        1,
                        "{\"foo\" : \"bar\"}".getBytes(StandardCharsets.UTF_8),
                        null,
                        -1
                    )
                ),
            // entries with corrupted source
            IntStream.range(0, randomIntBetween(1, 10))
                .mapToObj(
                    n -> new Translog.Index(
                        "1",
                        0,
                        shard.getPendingPrimaryTerm(),
                        1,
                        "{\"foo\" : \"bar}".getBytes(StandardCharsets.UTF_8),
                        null,
                        -1
                    )
                )
        ).collect(Collectors.toCollection(ArrayList::new));
        Randomness.shuffle(operations);
        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllReplicaOperationsPermits(
            shard.getOperationPrimaryTerm(),
            shard.getLastKnownGlobalCheckpoint(),
            0L,
            ActionListener.wrap(r -> {
                try (r) {
                    Translog.Snapshot snapshot = TestTranslog.newSnapshotFromOperations(operations);
                    final MapperParsingException error = expectThrows(
                        MapperParsingException.class,
                        () -> shard.runTranslogRecovery(shard.getEngine(), snapshot, Engine.Operation.Origin.LOCAL_RESET, () -> {})
                    );
                    assertThat(error.getMessage(), containsString("failed to parse field [foo] of type [text]"));
                } finally {
                    engineResetLatch.countDown();
                }
            }, e -> { throw new AssertionError(e); }),
            TimeValue.timeValueMinutes(1)
        );
        engineResetLatch.await();
        closeShards(shard);
    }

    public void testConcurrentAcquireAllReplicaOperationsPermitsWithPrimaryTermUpdate() throws Exception {
        final IndexShard replica = newStartedShard(false);
        indexOnReplicaWithGaps(replica, between(0, 1000), Math.toIntExact(replica.getLocalCheckpoint()));

        final int nbTermUpdates = randomIntBetween(1, 5);

        for (int i = 0; i < nbTermUpdates; i++) {
            long opPrimaryTerm = replica.getOperationPrimaryTerm() + 1;
            final long globalCheckpoint = replica.getLastKnownGlobalCheckpoint();
            final long maxSeqNoOfUpdatesOrDeletes = replica.getMaxSeqNoOfUpdatesOrDeletes();

            final int operations = scaledRandomIntBetween(5, 32);
            final CyclicBarrier barrier = new CyclicBarrier(1 + operations);
            final CountDownLatch latch = new CountDownLatch(operations);

            final Thread[] threads = new Thread[operations];
            for (int j = 0; j < operations; j++) {
                threads[j] = new Thread(() -> {
                    try {
                        barrier.await();
                    } catch (final BrokenBarrierException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    replica.acquireAllReplicaOperationsPermits(
                        opPrimaryTerm,
                        globalCheckpoint,
                        maxSeqNoOfUpdatesOrDeletes,
                        new ActionListener<Releasable>() {
                            @Override
                            public void onResponse(final Releasable releasable) {
                                try (Releasable ignored = releasable) {
                                    assertThat(replica.getPendingPrimaryTerm(), greaterThanOrEqualTo(opPrimaryTerm));
                                    assertThat(replica.getOperationPrimaryTerm(), equalTo(opPrimaryTerm));
                                } finally {
                                    latch.countDown();
                                }
                            }

                            @Override
                            public void onFailure(final Exception e) {
                                try {
                                    throw new RuntimeException(e);
                                } finally {
                                    latch.countDown();
                                }
                            }
                        },
                        TimeValue.timeValueMinutes(30L)
                    );
                });
                threads[j].start();
            }
            barrier.await();
            latch.await();

            for (Thread thread : threads) {
                thread.join();
            }
        }

        closeShard(replica, false);
    }

    @Override
    public Settings threadPoolSettings() {
        return Settings.builder().put(super.threadPoolSettings()).put("thread_pool.estimated_time_interval", "5ms").build();
    }

    public void testTypelessGet() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("index")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\"}}}")
            .settings(settings)
            .primaryTerm(0, 1)
            .build();
        IndexShard shard = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(shard);
        Engine.IndexResult indexResult = indexDoc(shard, "_doc", "0", "{\"foo\" : \"bar\"}");
        assertTrue(indexResult.isCreated());

        org.elasticsearch.index.engine.Engine.GetResult getResult = shard.get(new Engine.Get(true, true, "0"));
        assertTrue(getResult.exists());
        getResult.close();

        closeShards(shard);
    }

    /**
     * Randomizes the usage of {@link IndexShard#acquireReplicaOperationPermit(long, long, long, ActionListener, String, Object)} and
     * {@link IndexShard#acquireAllReplicaOperationsPermits(long, long, long, ActionListener, TimeValue)} in order to acquire a permit.
     */
    private void randomReplicaOperationPermitAcquisition(
        final IndexShard indexShard,
        final long opPrimaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes,
        final ActionListener<Releasable> listener,
        final String info
    ) {
        if (randomBoolean()) {
            final String executor = ThreadPool.Names.WRITE;
            indexShard.acquireReplicaOperationPermit(opPrimaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, listener, executor, info);
        } else {
            final TimeValue timeout = TimeValue.timeValueSeconds(30L);
            indexShard.acquireAllReplicaOperationsPermits(opPrimaryTerm, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, listener, timeout);
        }
    }

    public void testDoNotTrimCommitsWhenOpenReadOnlyEngine() throws Exception {
        final IndexShard shard = newStartedShard(false, Settings.EMPTY, new InternalEngineFactory());
        long numDocs = randomLongBetween(1, 20);
        long seqNo = 0;
        for (long i = 0; i < numDocs; i++) {
            if (rarely()) {
                seqNo++; // create gaps in sequence numbers
            }
            shard.applyIndexOperationOnReplica(
                seqNo,
                shard.getOperationPrimaryTerm(),
                1,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                new SourceToParse(Long.toString(i), new BytesArray("{}"), XContentType.JSON)
            );
            shard.updateGlobalCheckpointOnReplica(shard.getLocalCheckpoint(), "test");
            if (randomInt(100) < 10) {
                shard.flush(new FlushRequest());
            }
            seqNo++;
        }
        shard.flush(new FlushRequest());
        assertThat(shard.docStats().getCount(), equalTo(numDocs));
        final ShardRouting replicaRouting = shard.routingEntry();
        ShardRouting readonlyShardRouting = newShardRouting(
            replicaRouting.shardId(),
            replicaRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        final IndexShard readonlyShard = reinitShard(
            shard,
            readonlyShardRouting,
            shard.indexSettings.getIndexMetadata(),
            engineConfig -> new ReadOnlyEngine(engineConfig, null, null, true, Function.identity(), true, randomBoolean()) {
                @Override
                protected void ensureMaxSeqNoEqualsToGlobalCheckpoint(SeqNoStats seqNoStats) {
                    // just like a following shard, we need to skip this check for now.
                }
            }
        );
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        readonlyShard.markAsRecovering("store", new RecoveryState(readonlyShard.routingEntry(), localNode, null));
        recoverFromStore(readonlyShard);
        assertThat(readonlyShard.docStats().getCount(), equalTo(numDocs));
        closeShards(readonlyShard);
    }

    public void testCloseShardWhileEngineIsWarming() throws Exception {
        CountDownLatch warmerStarted = new CountDownLatch(1);
        CountDownLatch warmerBlocking = new CountDownLatch(1);
        IndexShard shard = newShard(true, Settings.EMPTY, config -> {
            Engine.Warmer warmer = reader -> {
                try {
                    warmerStarted.countDown();
                    warmerBlocking.await();
                    config.getWarmer().warm(reader);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            };
            EngineConfig configWithWarmer = new EngineConfig(
                config.getShardId(),
                config.getThreadPool(),
                config.getIndexSettings(),
                warmer,
                config.getStore(),
                config.getMergePolicy(),
                config.getAnalyzer(),
                config.getSimilarity(),
                new CodecService(null),
                config.getEventListener(),
                config.getQueryCache(),
                config.getQueryCachingPolicy(),
                config.getTranslogConfig(),
                config.getFlushMergesAfter(),
                config.getExternalRefreshListener(),
                config.getInternalRefreshListener(),
                config.getIndexSort(),
                config.getCircuitBreakerService(),
                config.getGlobalCheckpointSupplier(),
                config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(),
                IndexModule.DEFAULT_SNAPSHOT_COMMIT_SUPPLIER,
                config.getLeafSorter()
            );
            return new InternalEngine(configWithWarmer);
        });
        Thread recoveryThread = new Thread(() -> expectThrows(AlreadyClosedException.class, () -> recoverShardFromStore(shard)));
        recoveryThread.start();
        try {
            warmerStarted.await();
            shard.close("testing", false);
            assertThat(shard.state, equalTo(IndexShardState.CLOSED));
        } finally {
            warmerBlocking.countDown();
        }
        recoveryThread.join();
        shard.store().close();
    }

    public void testRecordsForceMerges() throws IOException {
        IndexShard shard = newStartedShard(true);
        final String initialForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(initialForceMergeUUID, nullValue());
        final ForceMergeRequest firstForceMergeRequest = new ForceMergeRequest().maxNumSegments(1);
        shard.forceMerge(firstForceMergeRequest);
        final String secondForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(secondForceMergeUUID, notNullValue());
        assertThat(secondForceMergeUUID, equalTo(firstForceMergeRequest.forceMergeUUID()));
        final ForceMergeRequest secondForceMergeRequest = new ForceMergeRequest().maxNumSegments(1);
        shard.forceMerge(secondForceMergeRequest);
        final String thirdForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(thirdForceMergeUUID, notNullValue());
        assertThat(thirdForceMergeUUID, not(equalTo(secondForceMergeUUID)));
        assertThat(thirdForceMergeUUID, equalTo(secondForceMergeRequest.forceMergeUUID()));
        closeShards(shard);
    }

    private static void blockingCallRelocated(
        IndexShard indexShard,
        ShardRouting routing,
        BiConsumer<ReplicationTracker.PrimaryContext, ActionListener<Void>> consumer
    ) {
        PlainActionFuture.<Void, RuntimeException>get(
            f -> indexShard.relocated(routing.getTargetRelocatingShard().allocationId().getId(), consumer, f)
        );
    }
}
