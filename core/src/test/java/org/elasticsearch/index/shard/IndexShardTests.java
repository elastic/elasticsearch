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
package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogTests;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.FieldMaskingReader;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.lucene.Lucene.cleanLuceneIndex;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.VersionType.EXTERNAL;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends IndexShardTestCase {

    public static ShardStateMetaData load(Logger logger, Path... shardPaths) throws IOException {
        return ShardStateMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, shardPaths);
    }

    public static void write(ShardStateMetaData shardStateMetaData,
                             Path... shardPaths) throws IOException {
        ShardStateMetaData.FORMAT.write(shardStateMetaData, shardPaths);
    }

    public static Engine getEngineFromShard(IndexShard shard) {
        return shard.getEngineOrNull();
    }

    public void testWriteShardState() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            ShardId id = new ShardId("foo", "fooUUID", 1);
            boolean primary = randomBoolean();
            AllocationId allocationId = randomBoolean() ? null : randomAllocationId();
            ShardStateMetaData state1 = new ShardStateMetaData(primary, "fooUUID", allocationId);
            write(state1, env.availableShardPaths(id));
            ShardStateMetaData shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state1);

            ShardStateMetaData state2 = new ShardStateMetaData(primary, "fooUUID", allocationId);
            write(state2, env.availableShardPaths(id));
            shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state1);

            ShardStateMetaData state3 = new ShardStateMetaData(primary, "fooUUID", allocationId);
            write(state3, env.availableShardPaths(id));
            shardStateMetaData = load(logger, env.availableShardPaths(id));
            assertEquals(shardStateMetaData, state3);
            assertEquals("fooUUID", state3.indexUUID);
        }
    }

    public void testPersistenceStateMetadataPersistence() throws Exception {
        IndexShard shard = newStartedShard();
        final Path shardStatePath = shard.shardPath().getShardStatePath();
        ShardStateMetaData shardStateMetaData = load(logger, shardStatePath);
        assertEquals(getShardStateMetadata(shard), shardStateMetaData);
        ShardRouting routing = shard.shardRouting;
        shard.updateRoutingEntry(routing);

        shardStateMetaData = load(logger, shardStatePath);
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData,
            new ShardStateMetaData(routing.primary(), shard.indexSettings().getUUID(), routing.allocationId()));

        routing = TestShardRouting.relocate(shard.shardRouting, "some node", 42L);
        shard.updateRoutingEntry(routing);
        shardStateMetaData = load(logger, shardStatePath);
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        assertEquals(shardStateMetaData,
            new ShardStateMetaData(routing.primary(), shard.indexSettings().getUUID(), routing.allocationId()));
        closeShards(shard);
    }

    public void testFailShard() throws Exception {
        IndexShard shard = newStartedShard();
        final ShardPath shardPath = shard.shardPath();
        assertNotNull(shardPath);
        // fail shard
        shard.failShard("test shard fail", new CorruptIndexException("", ""));
        closeShards(shard);
        // check state file still exists
        ShardStateMetaData shardStateMetaData = load(logger, shardPath.getShardStatePath());
        assertEquals(shardStateMetaData, getShardStateMetadata(shard));
        // but index can't be opened for a failed shard
        assertThat("store index should be corrupted", Store.canOpenIndex(logger, shardPath.resolveIndex(), shard.shardId(),
            (shardId, lockTimeoutMS) -> new DummyShardLock(shardId)),
            equalTo(false));
    }

    ShardStateMetaData getShardStateMetadata(IndexShard shard) {
        ShardRouting shardRouting = shard.routingEntry();
        if (shardRouting == null) {
            return null;
        } else {
            return new ShardStateMetaData(shardRouting.primary(), shard.indexSettings().getUUID(), shardRouting.allocationId());
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
        ShardStateMetaData meta = new ShardStateMetaData(randomBoolean(),
            randomRealisticUnicodeOfCodepointLengthBetween(1, 10), allocationId);

        assertEquals(meta, new ShardStateMetaData(meta.primary, meta.indexUUID, meta.allocationId));
        assertEquals(meta.hashCode(),
            new ShardStateMetaData(meta.primary, meta.indexUUID, meta.allocationId).hashCode());

        assertFalse(meta.equals(new ShardStateMetaData(!meta.primary, meta.indexUUID, meta.allocationId)));
        assertFalse(meta.equals(new ShardStateMetaData(!meta.primary, meta.indexUUID + "foo", meta.allocationId)));
        assertFalse(meta.equals(new ShardStateMetaData(!meta.primary, meta.indexUUID + "foo", randomAllocationId())));
        Set<Integer> hashCodes = new HashSet<>();
        for (int i = 0; i < 30; i++) { // just a sanity check that we impl hashcode
            allocationId = randomBoolean() ? null : randomAllocationId();
            meta = new ShardStateMetaData(randomBoolean(),
                randomRealisticUnicodeOfCodepointLengthBetween(1, 10), allocationId);
            hashCodes.add(meta.hashCode());
        }
        assertTrue("more than one unique hashcode expected but got: " + hashCodes.size(), hashCodes.size() > 1);

    }

    public void testClosesPreventsNewOperations() throws InterruptedException, ExecutionException, IOException {
        IndexShard indexShard = newStartedShard();
        closeShards(indexShard);
        assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
        try {
            indexShard.acquirePrimaryOperationPermit(null, ThreadPool.Names.INDEX);
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
        try {
            indexShard.acquireReplicaOperationPermit(indexShard.getPrimaryTerm(), null, ThreadPool.Names.INDEX);
            fail("we should not be able to increment anymore");
        } catch (IndexShardClosedException e) {
            // expected
        }
    }

    public void testPrimaryPromotionDelaysOperations() throws IOException, BrokenBarrierException, InterruptedException {
        final IndexShard indexShard = newStartedShard(false);

        final int operations = scaledRandomIntBetween(1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(1 + operations);
        final CountDownLatch latch = new CountDownLatch(operations);
        final CountDownLatch operationLatch = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < operations; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                indexShard.acquireReplicaOperationPermit(
                        indexShard.getPrimaryTerm(),
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
                        ThreadPool.Names.INDEX);
            });
            thread.start();
            threads.add(thread);
        }

        barrier.await();
        latch.await();

        // promote the replica
        final ShardRouting replicaRouting = indexShard.routingEntry();
        final ShardRouting primaryRouting =
                TestShardRouting.newShardRouting(
                        replicaRouting.shardId(),
                        replicaRouting.currentNodeId(),
                        null,
                        true,
                        ShardRoutingState.STARTED,
                        replicaRouting.allocationId());
        indexShard.updateRoutingEntry(primaryRouting);
        indexShard.updatePrimaryTerm(indexShard.getPrimaryTerm() + 1);

        final int delayedOperations = scaledRandomIntBetween(1, 64);
        final CyclicBarrier delayedOperationsBarrier = new CyclicBarrier(1 + delayedOperations);
        final CountDownLatch delayedOperationsLatch = new CountDownLatch(delayedOperations);
        final AtomicLong counter = new AtomicLong();
        final List<Thread> delayedThreads = new ArrayList<>();
        for (int i = 0; i < delayedOperations; i++) {
            final Thread thread = new Thread(() -> {
                try {
                    delayedOperationsBarrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                indexShard.acquirePrimaryOperationPermit(
                        new ActionListener<Releasable>() {
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
                        },
                        ThreadPool.Names.INDEX);
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

    public void testPrimaryFillsSeqNoGapsOnPromotion() throws Exception {
        final IndexShard indexShard = newStartedShard(false);

        // most of the time this is large enough that most of the time there will be at least one gap
        final int operations = 1024 - scaledRandomIntBetween(0, 1024);
        int max = Math.toIntExact(SequenceNumbersService.NO_OPS_PERFORMED);
        boolean gap = false;
        for (int i = 0; i < operations; i++) {
            final String id = Integer.toString(i);
            final ParsedDocument doc = testParsedDocument(id, "test", null, new ParseContext.Document(), new BytesArray("{}"), null);
            if (!rarely()) {
                final Term uid = new Term("_id", doc.id());
                final Engine.Index index =
                        new Engine.Index(uid, doc, i, indexShard.getPrimaryTerm(), 1, EXTERNAL, REPLICA, System.nanoTime(), -1, false);
                indexShard.index(index);
                max = i;
            } else {
                gap = true;
            }
        }

        final int maxSeqNo = max;
        if (gap) {
            assertThat(indexShard.getLocalCheckpoint(), not(equalTo(maxSeqNo)));
        }

        // promote the replica
        final ShardRouting replicaRouting = indexShard.routingEntry();
        final ShardRouting primaryRouting =
                TestShardRouting.newShardRouting(
                        replicaRouting.shardId(),
                        replicaRouting.currentNodeId(),
                        null,
                        true,
                        ShardRoutingState.STARTED,
                        replicaRouting.allocationId());
        indexShard.updateRoutingEntry(primaryRouting);
        indexShard.updatePrimaryTerm(indexShard.getPrimaryTerm() + 1);

        /*
         * This operation completing means that the delay operation executed as part of increasing the primary term has completed and the
         * gaps are filled.
         */
        final CountDownLatch latch = new CountDownLatch(1);
        indexShard.acquirePrimaryOperationPermit(
                new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        releasable.close();
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                ThreadPool.Names.GENERIC);

        latch.await();
        assertThat(indexShard.getLocalCheckpoint(), equalTo((long) maxSeqNo));
        closeShards(indexShard);
    }

    public void testOperationPermitsOnPrimaryShards() throws InterruptedException, ExecutionException, IOException {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final IndexShard indexShard;

        if (randomBoolean()) {
            // relocation target
            indexShard = newShard(TestShardRouting.newShardRouting(shardId, "local_node", "other node",
                true, ShardRoutingState.INITIALIZING, AllocationId.newRelocation(AllocationId.newInitializing())));
        } else if (randomBoolean()) {
            // simulate promotion
            indexShard = newStartedShard(false);
            ShardRouting replicaRouting = indexShard.routingEntry();
            ShardRouting primaryRouting = TestShardRouting.newShardRouting(replicaRouting.shardId(), replicaRouting.currentNodeId(), null,
                true, ShardRoutingState.STARTED, replicaRouting.allocationId());
            indexShard.updateRoutingEntry(primaryRouting);
            indexShard.updatePrimaryTerm(indexShard.getPrimaryTerm() + 1);
        } else {
            indexShard = newStartedShard(true);
        }
        final long primaryTerm = indexShard.getPrimaryTerm();
        assertEquals(0, indexShard.getActiveOperationsCount());
        if (indexShard.routingEntry().isRelocationTarget() == false) {
            try {
                indexShard.acquireReplicaOperationPermit(primaryTerm, null, ThreadPool.Names.INDEX);
                fail("shard shouldn't accept operations as replica");
            } catch (IllegalStateException ignored) {

            }
        }
        Releasable operation1 = acquirePrimaryOperationPermitBlockingly(indexShard);
        assertEquals(1, indexShard.getActiveOperationsCount());
        Releasable operation2 = acquirePrimaryOperationPermitBlockingly(indexShard);
        assertEquals(2, indexShard.getActiveOperationsCount());

        Releasables.close(operation1, operation2);
        assertEquals(0, indexShard.getActiveOperationsCount());

        closeShards(indexShard);
    }

    private Releasable acquirePrimaryOperationPermitBlockingly(IndexShard indexShard) throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquirePrimaryOperationPermit(fut, ThreadPool.Names.INDEX);
        return fut.get();
    }

    private Releasable acquireReplicaOperationPermitBlockingly(IndexShard indexShard, long opPrimaryTerm)
        throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquireReplicaOperationPermit(opPrimaryTerm, fut, ThreadPool.Names.INDEX);
        return fut.get();
    }

    public void testOperationPermitOnReplicaShards() throws InterruptedException, ExecutionException, IOException, BrokenBarrierException {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final IndexShard indexShard;
        final boolean engineClosed;
        switch (randomInt(2)) {
            case 0:
                // started replica
                indexShard = newStartedShard(false);
                engineClosed = false;
                break;
            case 1: {
                // initializing replica / primary
                final boolean relocating = randomBoolean();
                ShardRouting routing = TestShardRouting.newShardRouting(shardId, "local_node",
                    relocating ? "sourceNode" : null,
                    relocating ? randomBoolean() : false,
                    ShardRoutingState.INITIALIZING,
                    relocating ? AllocationId.newRelocation(AllocationId.newInitializing()) : AllocationId.newInitializing());
                indexShard = newShard(routing);
                engineClosed = true;
                break;
            }
            case 2: {
                // relocation source
                indexShard = newStartedShard(true);
                ShardRouting routing = indexShard.routingEntry();
                routing = TestShardRouting.newShardRouting(routing.shardId(), routing.currentNodeId(), "otherNode",
                    true, ShardRoutingState.RELOCATING, AllocationId.newRelocation(routing.allocationId()));
                indexShard.updateRoutingEntry(routing);
                indexShard.relocated("test");
                engineClosed = false;
                break;
            }
            default:
                throw new UnsupportedOperationException("get your numbers straight");

        }
        final ShardRouting shardRouting = indexShard.routingEntry();
        logger.info("shard routing to {}", shardRouting);

        assertEquals(0, indexShard.getActiveOperationsCount());
        if (shardRouting.primary() == false) {
            final IllegalStateException e =
                    expectThrows(IllegalStateException.class, () -> indexShard.acquirePrimaryOperationPermit(null, ThreadPool.Names.INDEX));
            assertThat(e, hasToString(containsString("shard is not a primary")));
        }

        final long primaryTerm = indexShard.getPrimaryTerm();
        final long translogGen = engineClosed ? -1 : indexShard.getTranslog().getGeneration().translogFileGeneration;

        final Releasable operation1 = acquireReplicaOperationPermitBlockingly(indexShard, primaryTerm);
        assertEquals(1, indexShard.getActiveOperationsCount());
        final Releasable operation2 = acquireReplicaOperationPermitBlockingly(indexShard, primaryTerm);
        assertEquals(2, indexShard.getActiveOperationsCount());

        {
            final AtomicBoolean onResponse = new AtomicBoolean();
            final AtomicBoolean onFailure = new AtomicBoolean();
            final AtomicReference<Exception> onFailureException = new AtomicReference<>();
            ActionListener<Releasable> onLockAcquired = new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    onResponse.set(true);
                }

                @Override
                public void onFailure(Exception e) {
                    onFailure.set(true);
                    onFailureException.set(e);
                }
            };

            indexShard.acquireReplicaOperationPermit(primaryTerm - 1, onLockAcquired, ThreadPool.Names.INDEX);

            assertFalse(onResponse.get());
            assertTrue(onFailure.get());
            assertThat(onFailureException.get(), instanceOf(IllegalStateException.class));
            assertThat(
                    onFailureException.get(), hasToString(containsString("operation primary term [" + (primaryTerm - 1) + "] is too old")));
        }

        {
            final AtomicBoolean onResponse = new AtomicBoolean();
            final AtomicReference<Exception> onFailure = new AtomicReference<>();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final long newPrimaryTerm = primaryTerm + 1 + randomInt(20);
            // but you can not increment with a new primary term until the operations on the older primary term complete
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                indexShard.acquireReplicaOperationPermit(
                    newPrimaryTerm,
                        new ActionListener<Releasable>() {
                            @Override
                            public void onResponse(Releasable releasable) {
                                assertThat(indexShard.getPrimaryTerm(), equalTo(newPrimaryTerm));
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
                        },
                        ThreadPool.Names.SAME);
            });
            thread.start();
            barrier.await();
            // our operation should be blocked until the previous operations complete
            assertFalse(onResponse.get());
            assertNull(onFailure.get());
            assertThat(indexShard.getPrimaryTerm(), equalTo(primaryTerm));
            Releasables.close(operation1);
            // our operation should still be blocked
            assertFalse(onResponse.get());
            assertNull(onFailure.get());
            assertThat(indexShard.getPrimaryTerm(), equalTo(primaryTerm));
            Releasables.close(operation2);
            barrier.await();
            // now lock acquisition should have succeeded
            assertThat(indexShard.getPrimaryTerm(), equalTo(newPrimaryTerm));
            if (engineClosed) {
                assertFalse(onResponse.get());
                assertThat(onFailure.get(), instanceOf(AlreadyClosedException.class));
            } else {
                assertTrue(onResponse.get());
                assertNull(onFailure.get());
                assertThat(indexShard.getTranslog().getGeneration().translogFileGeneration, equalTo(translogGen + 1));
            }
            thread.join();
            assertEquals(0, indexShard.getActiveOperationsCount());
        }

        closeShards(indexShard);
    }

    public void testConcurrentTermIncreaseOnReplicaShard() throws BrokenBarrierException, InterruptedException, IOException {
        final IndexShard indexShard = newStartedShard(false);

        final CyclicBarrier barrier = new CyclicBarrier(3);
        final CountDownLatch latch = new CountDownLatch(2);

        final long primaryTerm = indexShard.getPrimaryTerm();
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
                    new ActionListener<Releasable>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            counter.incrementAndGet();
                            assertThat(indexShard.getPrimaryTerm(), equalTo(primaryTerm + increment));
                            latch.countDown();
                            releasable.close();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onFailure.set(e);
                            latch.countDown();
                        }
                    },
                    ThreadPool.Names.INDEX);
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

        assertThat(indexShard.getPrimaryTerm(), equalTo(primaryTerm + Math.max(firstIncrement, secondIncrement)));

        closeShards(indexShard);
    }

    public void testAcquireIndexCommit() throws IOException {
        final IndexShard shard = newStartedShard();
        int numDocs = randomInt(20);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "type", "id_" + i);
        }
        final boolean flushFirst = randomBoolean();
        Engine.IndexCommitRef commit = shard.acquireIndexCommit(flushFirst);
        int moreDocs = randomInt(20);
        for (int i = 0; i < moreDocs; i++) {
            indexDoc(shard, "type", "id_" + numDocs + i);
        }
        flushShard(shard);
        // check that we can still read the commit that we captured
        try (IndexReader reader = DirectoryReader.open(commit.getIndexCommit())) {
            assertThat(reader.numDocs(), equalTo(flushFirst ? numDocs : 0));
        }
        commit.close();
        flushShard(shard, true);

        // check it's clean up
        assertThat(DirectoryReader.listCommits(shard.store().directory()), hasSize(1));

        closeShards(shard);
    }

    /***
     * test one can snapshot the store at various lifecycle stages
     */
    public void testSnapshotStore() throws IOException {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "test", "0");
        flushShard(shard);

        final IndexShard newShard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

        Store.MetadataSnapshot snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

        assertTrue(newShard.recoverFromStore());

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

        newShard.updateRoutingEntry(newShard.routingEntry().moveToStarted());

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

        newShard.close("test", false);

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name(), equalTo("segments_2"));

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
                            shard.sync(TranslogTests.randomTranslogLocation(), (ex) -> semaphore.release());
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

    public void testMinimumCompatVersion() throws IOException {
        Version versionCreated = VersionUtils.randomVersion(random());
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, versionCreated.id)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData metaData = IndexMetaData.builder("test")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard test = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoveryShardFromStore(test);

        indexDoc(test, "test", "test");
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        indexDoc(test, "test", "test");
        assertEquals(versionCreated.luceneVersion, test.minimumCompatibleVersion());
        test.getEngine().flush();
        assertEquals(Version.CURRENT.luceneVersion, test.minimumCompatibleVersion());

        closeShards(test);
    }

    public void testShardStats() throws IOException {

        IndexShard shard = newStartedShard();
        ShardStats stats = new ShardStats(shard.routingEntry(), shard.shardPath(),
            new CommonStats(new IndicesQueryCache(Settings.EMPTY), shard, new CommonStatsFlags()), shard.commitStats(), shard.seqNoStats());
        assertEquals(shard.shardPath().getRootDataPath().toString(), stats.getDataPath());
        assertEquals(shard.shardPath().getRootStatePath().toString(), stats.getStatePath());
        assertEquals(shard.shardPath().isCustomDataPath(), stats.isCustomDataPath());

        if (randomBoolean() || true) { // try to serialize it to ensure values survive the serialization
            BytesStreamOutput out = new BytesStreamOutput();
            stats.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            stats = ShardStats.readShardStats(in);
        }
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        String xContent = builder.string();
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

    private ParsedDocument testParsedDocument(String id, String type, String routing,
                                              ParseContext.Document document, BytesReference source, Mapping mappingUpdate) {
        Field idField = new Field("_id", id, IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(idField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        return new ParsedDocument(versionField, seqID, id, type, routing, Arrays.asList(document), source, XContentType.JSON,
            mappingUpdate);
    }

    public void testIndexingOperationsListeners() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "test", "0", "{\"foo\" : \"bar\"}");
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
                if (result.hasFailure() == false) {
                    if (result.isCreated()) {
                        postIndexCreate.incrementAndGet();
                    } else {
                        postIndexUpdate.incrementAndGet();
                    }
                } else {
                    postIndex(shardId, index, result.getFailure());
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
                if (result.hasFailure() == false) {
                    postDelete.incrementAndGet();
                } else {
                    postDelete(shardId, delete, result.getFailure());
                }
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
                postDeleteException.incrementAndGet();

            }
        });
        recoveryShardFromStore(shard);

        ParsedDocument doc = testParsedDocument("1", "test", null, new ParseContext.Document(),
            new BytesArray(new byte[]{1}), null);
        Engine.Index index = new Engine.Index(new Term("_id", doc.id()), doc);
        shard.index(index);
        assertEquals(1, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(0, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(0, postDelete.get());
        assertEquals(0, postDeleteException.get());

        shard.index(index);
        assertEquals(2, preIndex.get());
        assertEquals(1, postIndexCreate.get());
        assertEquals(1, postIndexUpdate.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(0, postDelete.get());
        assertEquals(0, postDeleteException.get());

        Engine.Delete delete = new Engine.Delete("test", "1", new Term("_id", doc.id()));
        shard.delete(delete);

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
            shard.index(index);
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
            shard.delete(delete);
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
        shard.updateRoutingEntry(ShardRoutingHelper.relocate(shard.routingEntry(), "other_node"));
        CountDownLatch latch = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            latch.countDown();
            try {
                shard.relocated("simulated recovery");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try (Releasable ignored = acquirePrimaryOperationPermitBlockingly(shard)) {
            // start finalization of recovery
            recoveryThread.start();
            latch.await();
            // recovery can only be finalized after we release the current primaryOperationLock
            assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        }
        // recovery can be now finalized
        recoveryThread.join();
        assertThat(shard.state(), equalTo(IndexShardState.RELOCATED));
        try (Releasable ignored = acquirePrimaryOperationPermitBlockingly(shard)) {
            // lock can again be acquired
            assertThat(shard.state(), equalTo(IndexShardState.RELOCATED));
        }

        closeShards(shard);
    }

    public void testDelayedOperationsBeforeAndAfterRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true);
        shard.updateRoutingEntry(ShardRoutingHelper.relocate(shard.routingEntry(), "other_node"));
        Thread recoveryThread = new Thread(() -> {
            try {
                shard.relocated("simulated recovery");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        recoveryThread.start();
        List<PlainActionFuture<Releasable>> onLockAcquiredActions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            PlainActionFuture<Releasable> onLockAcquired = new PlainActionFuture<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    super.onResponse(releasable);
                }
            };
            shard.acquirePrimaryOperationPermit(onLockAcquired, ThreadPool.Names.INDEX);
            onLockAcquiredActions.add(onLockAcquired);
        }

        for (PlainActionFuture<Releasable> onLockAcquired : onLockAcquiredActions) {
            assertNotNull(onLockAcquired.get(30, TimeUnit.SECONDS));
        }

        recoveryThread.join();

        closeShards(shard);
    }

    public void testStressRelocated() throws Exception {
        final IndexShard shard = newStartedShard(true);
        shard.updateRoutingEntry(ShardRoutingHelper.relocate(shard.routingEntry(), "other_node"));
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
            try {
                shard.relocated("simulated recovery");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            relocated.set(true);
        });
        // ensure we wait for all primary operation locks to be acquired
        allPrimaryOperationLocksAcquired.await();
        // start recovery thread
        recoveryThread.start();
        assertThat(relocated.get(), equalTo(false));
        assertThat(shard.getActiveOperationsCount(), greaterThan(0));
        // ensure we only transition to RELOCATED state after pending operations completed
        assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        // complete pending operations
        barrier.await();
        // complete recovery/relocation
        recoveryThread.join();
        // ensure relocated successfully once pending operations are done
        assertThat(relocated.get(), equalTo(true));
        assertThat(shard.state(), equalTo(IndexShardState.RELOCATED));
        assertThat(shard.getActiveOperationsCount(), equalTo(0));

        for (Thread indexThread : indexThreads) {
            indexThread.join();
        }

        closeShards(shard);
    }

    public void testRelocatedShardCanNotBeRevived() throws IOException, InterruptedException {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting originalRouting = shard.routingEntry();
        shard.updateRoutingEntry(ShardRoutingHelper.relocate(originalRouting, "other_node"));
        shard.relocated("test");
        expectThrows(IllegalIndexShardStateException.class, () -> shard.updateRoutingEntry(originalRouting));
        closeShards(shard);
    }

    public void testShardCanNotBeMarkedAsRelocatedIfRelocationCancelled() throws IOException, InterruptedException {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting originalRouting = shard.routingEntry();
        shard.updateRoutingEntry(ShardRoutingHelper.relocate(originalRouting, "other_node"));
        shard.updateRoutingEntry(originalRouting);
        expectThrows(IllegalIndexShardStateException.class, () ->  shard.relocated("test"));
        closeShards(shard);
    }

    public void testRelocatedShardCanNotBeRevivedConcurrently() throws IOException, InterruptedException, BrokenBarrierException {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting originalRouting = shard.routingEntry();
        shard.updateRoutingEntry(ShardRoutingHelper.relocate(originalRouting, "other_node"));
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
                shard.relocated("test");
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
                shard.updateRoutingEntry(originalRouting);
            }
        });
        cancellingThread.start();
        cyclicBarrier.await();
        relocationThread.join();
        cancellingThread.join();
        if (shard.state() == IndexShardState.RELOCATED) {
            logger.debug("shard was relocated successfully");
            assertThat(cancellingException.get(), instanceOf(IllegalIndexShardStateException.class));
            assertThat("current routing:" + shard.routingEntry(), shard.routingEntry().relocating(), equalTo(true));
            assertThat(relocationException.get(), nullValue());
        } else {
            logger.debug("shard relocation was cancelled");
            assertThat(relocationException.get(), instanceOf(IllegalIndexShardStateException.class));
            assertThat("current routing:" + shard.routingEntry(), shard.routingEntry().relocating(), equalTo(false));
            assertThat(cancellingException.get(), nullValue());

        }
        closeShards(shard);
    }

    public void testRecoverFromStore() throws IOException {
        final IndexShard shard = newStartedShard(true);
        int translogOps = 1;
        indexDoc(shard, "test", "0");
        if (randomBoolean()) {
            flushShard(shard);
            translogOps = 0;
        }
        IndexShard newShard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(newShard.recoverFromStore());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(translogOps, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        assertEquals(100.0f, newShard.recoveryState().getTranslog().recoveredPercent(), 0.01f);
        newShard.updateRoutingEntry(newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 1);
        closeShards(newShard);
    }

    /* This test just verifies that we fill up local checkpoint up to max seen seqID on primary recovery */
    public void testRecoverFromStoreWithNoOps() throws IOException {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "test", "0");
        Engine.Index test = indexDoc(shard, "test", "1");
        // start a replica shard and index the second doc
        final IndexShard otherShard = newStartedShard(false);
        test = otherShard.prepareIndexOnReplica(
            SourceToParse.source(shard.shardId().getIndexName(), test.type(), test.id(), test.source(),
                XContentType.JSON),
            1, 1, 1, EXTERNAL, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
        otherShard.index(test);

        final ShardRouting primaryShardRouting = shard.routingEntry();
        IndexShard newShard = reinitShard(otherShard, ShardRoutingHelper.initWithSameId(primaryShardRouting,
            RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE));
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(newShard.recoverFromStore());
        assertEquals(1, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(1, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(1, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        assertEquals(100.0f, newShard.recoveryState().getTranslog().recoveredPercent(), 0.01f);
        Translog.Snapshot snapshot = newShard.getTranslog().newSnapshot();
        Translog.Operation operation;
        int numNoops = 0;
        while((operation = snapshot.next()) != null) {
            if (operation.opType() == Translog.Operation.Type.NO_OP) {
                numNoops++;
                assertEquals(1, operation.primaryTerm());
                assertEquals(0, operation.seqNo());
            }
        }
        assertEquals(1, numNoops);
        newShard.updateRoutingEntry(newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 1);
        assertDocCount(shard, 2);
        closeShards(newShard, shard);
    }

    public void testRecoverFromCleanStore() throws IOException {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "test", "0");
        if (randomBoolean()) {
            flushShard(shard);
        }
        final ShardRouting shardRouting = shard.routingEntry();
        IndexShard newShard = reinitShard(shard,
            ShardRoutingHelper.initWithSameId(shardRouting, RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE)
        );

        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue(newShard.recoverFromStore());
        assertEquals(0, newShard.recoveryState().getTranslog().recoveredOperations());
        assertEquals(0, newShard.recoveryState().getTranslog().totalOperations());
        assertEquals(0, newShard.recoveryState().getTranslog().totalOperationsOnStart());
        assertEquals(100.0f, newShard.recoveryState().getTranslog().recoveredPercent(), 0.01f);
        newShard.updateRoutingEntry(newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 0);
        closeShards(newShard);
    }

    public void testFailIfIndexNotPresentInRecoverFromStore() throws Exception {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "test", "0");
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
            newShard.recoverFromStore();
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

        newShard = reinitShard(newShard,
            ShardRoutingHelper.initWithSameId(routing, RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE));
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertTrue("recover even if there is nothing to recover", newShard.recoverFromStore());

        newShard.updateRoutingEntry(newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 0);
        // we can't issue this request through a client because of the inconsistencies we created with the cluster state
        // doing it directly instead
        indexDoc(newShard, "test", "0");
        newShard.refresh("test");
        assertDocCount(newShard, 1);

        closeShards(newShard);
    }

    public void testRecoveryFailsAfterMovingToRelocatedState() throws InterruptedException, IOException {
        final IndexShard shard = newStartedShard(true);
        ShardRouting origRouting = shard.routingEntry();
        assertThat(shard.state(), equalTo(IndexShardState.STARTED));
        ShardRouting inRecoveryRouting = ShardRoutingHelper.relocate(origRouting, "some_node");
        shard.updateRoutingEntry(inRecoveryRouting);
        shard.relocated("simulate mark as relocated");
        assertThat(shard.state(), equalTo(IndexShardState.RELOCATED));
        try {
            shard.updateRoutingEntry(origRouting);
            fail("Expected IndexShardRelocatedException");
        } catch (IndexShardRelocatedException expected) {
        }

        closeShards(shard);
    }

    public void testRestoreShard() throws IOException {
        final IndexShard source = newStartedShard(true);
        IndexShard target = newStartedShard(true);

        indexDoc(source, "test", "0");
        if (randomBoolean()) {
            source.refresh("test");
        }
        indexDoc(target, "test", "1");
        target.refresh("test");
        assertDocs(target, "1");
        flushShard(source); // only flush source
        final ShardRouting origRouting = target.routingEntry();
        ShardRouting routing = ShardRoutingHelper.reinitPrimary(origRouting);
        final Snapshot snapshot = new Snapshot("foo", new SnapshotId("bar", UUIDs.randomBase64UUID()));
        routing = ShardRoutingHelper.newWithRestoreSource(routing,
            new RecoverySource.SnapshotRecoverySource(snapshot, Version.CURRENT, "test"));
        target = reinitShard(target, routing);
        Store sourceStore = source.store();
        Store targetStore = target.store();

        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        target.markAsRecovering("store", new RecoveryState(routing, localNode, null));
        assertTrue(target.restoreFromRepository(new RestoreOnlyRepository("test") {
            @Override
            public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId,
                                     RecoveryState recoveryState) {
                try {
                    cleanLuceneIndex(targetStore.directory());
                    for (String file : sourceStore.directory().listAll()) {
                        if (file.equals("write.lock") || file.startsWith("extra")) {
                            continue;
                        }
                        targetStore.directory().copyFrom(sourceStore.directory(), file, file, IOContext.DEFAULT);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }));

        target.updateRoutingEntry(routing.moveToStarted());
        assertDocs(target, "0");

        closeShards(source, target);
    }

    public void testSearcherWrapperIsUsed() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "test", "0", "{\"foo\" : \"bar\"}");
        indexDoc(shard, "test", "1", "{\"foobar\" : \"bar\"}");
        shard.refresh("test");

        Engine.GetResult getResult = shard.get(new Engine.Get(false, "test", "1", new Term(IdFieldMapper.NAME, "1")));
        assertTrue(getResult.exists());
        assertNotNull(getResult.searcher());
        getResult.release();
        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            TopDocs search = searcher.searcher().search(new TermQuery(new Term("foo", "bar")), 10);
            assertEquals(search.totalHits, 1);
            search = searcher.searcher().search(new TermQuery(new Term("foobar", "bar")), 10);
            assertEquals(search.totalHits, 1);
        }
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new FieldMaskingReader("foo", reader);
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };
        closeShards(shard);
        IndexShard newShard = newShard(ShardRoutingHelper.reinitPrimary(shard.routingEntry()),
            shard.shardPath(), shard.indexSettings().getIndexMetaData(), wrapper, null);

        recoveryShardFromStore(newShard);

        try (Engine.Searcher searcher = newShard.acquireSearcher("test")) {
            TopDocs search = searcher.searcher().search(new TermQuery(new Term("foo", "bar")), 10);
            assertEquals(search.totalHits, 0);
            search = searcher.searcher().search(new TermQuery(new Term("foobar", "bar")), 10);
            assertEquals(search.totalHits, 1);
        }
        getResult = newShard.get(new Engine.Get(false, "test", "1", new Term(IdFieldMapper.NAME, "1")));
        assertTrue(getResult.exists());
        assertNotNull(getResult.searcher()); // make sure get uses the wrapped reader
        assertTrue(getResult.searcher().reader() instanceof FieldMaskingReader);
        getResult.release();

        closeShards(newShard);
    }

    public void testSearcherWrapperWorksWithGlobalOrdinals() throws IOException {
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                return new FieldMaskingReader("foo", reader);
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };

        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData metaData = IndexMetaData.builder("test")
            .putMapping("test", "{ \"properties\": { \"foo\":  { \"type\": \"text\", \"fielddata\": true }}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, wrapper);
        recoveryShardFromStore(shard);
        indexDoc(shard, "test", "0", "{\"foo\" : \"bar\"}");
        shard.refresh("created segment 1");
        indexDoc(shard, "test", "1", "{\"foobar\" : \"bar\"}");
        shard.refresh("created segment 2");

        // test global ordinals are evicted
        MappedFieldType foo = shard.mapperService().fullName("foo");
        IndexFieldData.Global ifd = shard.indexFieldDataService().getForField(foo);
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
        indexDoc(shard, "test", "0", "{\"foo\" : \"bar\"}");
        deleteDoc(shard, "test", "0");
        indexDoc(shard, "test", "1", "{\"foo\" : \"bar\"}");
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
        recoveryShardFromStore(newShard);
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
        indexDoc(shard, "test", "0", "{\"foo\" : \"bar\"}");
        shard.refresh("test");
        IndexSearcherWrapper wrapper = new IndexSearcherWrapper() {
            @Override
            public DirectoryReader wrap(DirectoryReader reader) throws IOException {
                throw new RuntimeException("boom");
            }

            @Override
            public IndexSearcher wrap(IndexSearcher searcher) throws EngineException {
                return searcher;
            }
        };

        closeShards(shard);
        IndexShard newShard = newShard(ShardRoutingHelper.reinitPrimary(shard.routingEntry()),
            shard.shardPath(), shard.indexSettings().getIndexMetaData(), wrapper, null);

        recoveryShardFromStore(newShard);

        try {
            newShard.acquireSearcher("test");
            fail("exception expected");
        } catch (RuntimeException ex) {
            //
        }
        closeShards(newShard);
    }

    public void testTranslogRecoverySyncsTranslog() throws IOException {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData metaData = IndexMetaData.builder("test")
            .putMapping("test", "{ \"properties\": { \"foo\":  { \"type\": \"text\"}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoveryShardFromStore(primary);

        indexDoc(primary, "test", "0", "{\"foo\" : \"bar\"}");
        IndexShard replica = newShard(primary.shardId(), false, "n2", metaData, null);
        recoverReplica(replica, primary, (shard, discoveryNode) ->
            new RecoveryTarget(shard, discoveryNode, recoveryListener, aLong -> {
            }) {
                @Override
                public long indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) {
                    final long localCheckpoint = super.indexTranslogOperations(operations, totalTranslogOps);
                    assertFalse(replica.getTranslog().syncNeeded());
                    return localCheckpoint;
                }
            }, true);

        closeShards(primary, replica);
    }

    public void testShardActiveDuringInternalRecovery() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "type", "0");
        shard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        shard.markAsRecovering("for testing", new RecoveryState(shard.routingEntry(), localNode, null));
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(shard.isActive());
        shard.prepareForIndexRecovery();
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(shard.isActive());
        shard.performTranslogRecovery(true);
        // Shard should now be active since we did recover:
        assertTrue(shard.isActive());
        closeShards(shard);
    }

    public void testShardActiveDuringPeerRecovery() throws IOException {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData metaData = IndexMetaData.builder("test")
            .putMapping("test", "{ \"properties\": { \"foo\":  { \"type\": \"text\"}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoveryShardFromStore(primary);

        indexDoc(primary, "test", "0", "{\"foo\" : \"bar\"}");
        IndexShard replica = newShard(primary.shardId(), false, "n2", metaData, null);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        // Shard is still inactive since we haven't started recovering yet
        assertFalse(replica.isActive());
        recoverReplica(replica, primary, (shard, discoveryNode) ->
            new RecoveryTarget(shard, discoveryNode, recoveryListener, aLong -> {
            }) {
                @Override
                public void prepareForTranslogOperations(int totalTranslogOps) throws IOException {
                    super.prepareForTranslogOperations(totalTranslogOps);
                    // Shard is still inactive since we haven't started recovering yet
                    assertFalse(replica.isActive());

                }

                @Override
                public long indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps) {
                    final long localCheckpoint = super.indexTranslogOperations(operations, totalTranslogOps);
                    // Shard should now be active since we did recover:
                    assertTrue(replica.isActive());
                    return localCheckpoint;
                }
            }, false);

        closeShards(primary, replica);
    }

    public void testRecoverFromLocalShard() throws IOException {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetaData metaData = IndexMetaData.builder("source")
            .putMapping("test", "{ \"properties\": { \"foo\":  { \"type\": \"text\"}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();

        IndexShard sourceShard = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoveryShardFromStore(sourceShard);

        indexDoc(sourceShard, "test", "0", "{\"foo\" : \"bar\"}");
        indexDoc(sourceShard, "test", "1", "{\"foo\" : \"bar\"}");
        sourceShard.refresh("test");


        ShardRouting targetRouting = TestShardRouting.newShardRouting(new ShardId("index_1", "index_1", 0), "n1", true,
            ShardRoutingState.INITIALIZING, RecoverySource.LocalShardsRecoverySource.INSTANCE);

        final IndexShard targetShard;
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Map<String, MappingMetaData> requestedMappingUpdates = ConcurrentCollections.newConcurrentMap();
        {
            targetShard = newShard(targetRouting);
            targetShard.markAsRecovering("store", new RecoveryState(targetShard.routingEntry(), localNode, null));

            BiConsumer<String, MappingMetaData> mappingConsumer = (type, mapping) -> {
                assertNull(requestedMappingUpdates.put(type, mapping));
            };

            final IndexShard differentIndex = newShard(new ShardId("index_2", "index_2", 0), true);
            recoveryShardFromStore(differentIndex);
            expectThrows(IllegalArgumentException.class, () -> {
                targetShard.recoverFromLocalShards(mappingConsumer, Arrays.asList(sourceShard, differentIndex));
            });
            closeShards(differentIndex);

            assertTrue(targetShard.recoverFromLocalShards(mappingConsumer, Arrays.asList(sourceShard)));
            RecoveryState recoveryState = targetShard.recoveryState();
            assertEquals(RecoveryState.Stage.DONE, recoveryState.getStage());
            assertTrue(recoveryState.getIndex().fileDetails().size() > 0);
            for (RecoveryState.File file : recoveryState.getIndex().fileDetails()) {
                if (file.reused()) {
                    assertEquals(file.recovered(), 0);
                } else {
                    assertEquals(file.recovered(), file.length());
                }
            }
            targetShard.updateRoutingEntry(ShardRoutingHelper.moveToStarted(targetShard.routingEntry()));
            assertDocCount(targetShard, 2);
        }
        // now check that it's persistent ie. that the added shards are committed
        {
            final IndexShard newShard = reinitShard(targetShard);
            recoveryShardFromStore(newShard);
            assertDocCount(newShard, 2);
            closeShards(newShard);
        }

        assertThat(requestedMappingUpdates, hasKey("test"));
        assertThat(requestedMappingUpdates.get("test").get().source().string(), equalTo("{\"properties\":{\"foo\":{\"type\":\"text\"}}}"));

        closeShards(sourceShard, targetShard);
    }

    public void testDocStats() throws IOException {
        IndexShard indexShard = null;
        try {
            indexShard = newStartedShard();
            final long numDocs = randomIntBetween(2, 32); // at least two documents so we have docs to delete
            // Delete at least numDocs/10 documents otherwise the number of deleted docs will be below 10%
            // and forceMerge will refuse to expunge deletes
            final long numDocsToDelete = randomIntBetween((int) Math.ceil(Math.nextUp(numDocs / 10.0)), Math.toIntExact(numDocs));
            for (int i = 0; i < numDocs; i++) {
                final String id = Integer.toString(i);
                final ParsedDocument doc =
                    testParsedDocument(id, "test", null, new ParseContext.Document(), new BytesArray("{}"), null);
                final Engine.Index index =
                    new Engine.Index(
                        new Term("_id", doc.id()),
                        doc,
                        SequenceNumbersService.UNASSIGNED_SEQ_NO,
                        0,
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        PRIMARY,
                        System.nanoTime(),
                        -1,
                        false);
                final Engine.IndexResult result = indexShard.index(index);
                assertThat(result.getVersion(), equalTo(1L));
            }

            indexShard.refresh("test");
            {
                final DocsStats docsStats = indexShard.docStats();
                assertThat(docsStats.getCount(), equalTo(numDocs));
                assertThat(docsStats.getDeleted(), equalTo(0L));
            }

            final List<Integer> ids = randomSubsetOf(
                Math.toIntExact(numDocsToDelete),
                IntStream.range(0, Math.toIntExact(numDocs)).boxed().collect(Collectors.toList()));
            for (final Integer i : ids) {
                final String id = Integer.toString(i);
                final ParsedDocument doc =
                    testParsedDocument(id, "test", null, new ParseContext.Document(), new BytesArray("{}"), null);
                final Engine.Index index =
                    new Engine.Index(
                        new Term("_id", doc.id()),
                        doc,
                        SequenceNumbersService.UNASSIGNED_SEQ_NO,
                        0,
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        PRIMARY,
                        System.nanoTime(),
                        -1,
                        false);
                final Engine.IndexResult result = indexShard.index(index);
                assertThat(result.getVersion(), equalTo(2L));
            }

            // flush the buffered deletes
            final FlushRequest flushRequest = new FlushRequest();
            flushRequest.force(false);
            flushRequest.waitIfOngoing(false);
            indexShard.flush(flushRequest);

            indexShard.refresh("test");
            {
                final DocsStats docStats = indexShard.docStats();
                assertThat(docStats.getCount(), equalTo(numDocs));
                // Lucene will delete a segment if all docs are deleted from it; this means that we lose the deletes when deleting all docs
                assertThat(docStats.getDeleted(), equalTo(numDocsToDelete == numDocs ? 0 : numDocsToDelete));
            }

            // merge them away
            final ForceMergeRequest forceMergeRequest = new ForceMergeRequest();
            forceMergeRequest.onlyExpungeDeletes(randomBoolean());
            forceMergeRequest.maxNumSegments(1);
            indexShard.forceMerge(forceMergeRequest);

            indexShard.refresh("test");
            {
                final DocsStats docStats = indexShard.docStats();
                assertThat(docStats.getCount(), equalTo(numDocs));
                assertThat(docStats.getDeleted(), equalTo(0L));
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
        indexDoc(indexShard, "doc", "0", "{\"foo\" : \"bar\"}");
        if (randomBoolean()) {
            indexShard.refresh("test");
        }
        indexDoc(indexShard, "doc", "1", "{\"foo\" : \"bar\"}");
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final IndexShard newShard = reinitShard(indexShard);
        Store.MetadataSnapshot storeFileMetaDatas = newShard.snapshotStoreMetadata();
        assertTrue("at least 2 files, commit and data: " +storeFileMetaDatas.toString(), storeFileMetaDatas.size() > 1);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        expectThrows(AlreadyClosedException.class, () -> newShard.getEngine()); // no engine
        Thread thread = new Thread(() -> {
            latch.countDown();
            while(stop.get() == false){
                try {
                    Store.MetadataSnapshot readMeta = newShard.snapshotStoreMetadata();
                    assertEquals(0, storeFileMetaDatas.recoveryDiff(readMeta).different.size());
                    assertEquals(0, storeFileMetaDatas.recoveryDiff(readMeta).missing.size());
                    assertEquals(storeFileMetaDatas.size(), storeFileMetaDatas.recoveryDiff(readMeta).identical.size());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        });
        thread.start();
        latch.await();

        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            newShard.store().cleanupAndVerify("test", storeFileMetaDatas);
        }
        assertTrue(stop.compareAndSet(false, true));
        thread.join();
        closeShards(newShard);
    }

    /** A dummy repository for testing which just needs restore overridden */
    private abstract static class RestoreOnlyRepository extends AbstractLifecycleComponent implements Repository {
        private final String indexName;

        RestoreOnlyRepository(String indexName) {
            super(Settings.EMPTY);
            this.indexName = indexName;
        }

        @Override
        protected void doStart() {
        }

        @Override
        protected void doStop() {
        }

        @Override
        protected void doClose() {
        }

        @Override
        public RepositoryMetaData getMetadata() {
            return null;
        }

        @Override
        public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
            return null;
        }

        @Override
        public MetaData getSnapshotMetaData(SnapshotInfo snapshot, List<IndexId> indices) throws IOException {
            return null;
        }

        @Override
        public RepositoryData getRepositoryData() {
            Map<IndexId, Set<SnapshotId>> map = new HashMap<>();
            map.put(new IndexId(indexName, "blah"), emptySet());
            return new RepositoryData(EMPTY_REPO_GEN, Collections.emptyMap(), Collections.emptyMap(), map, Collections.emptyList());
        }

        @Override
        public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData) {
        }

        @Override
        public SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards,
                                             List<SnapshotShardFailure> shardFailures, long repositoryStateId) {
            return null;
        }

        @Override
        public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId) {
        }

        @Override
        public long getSnapshotThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public long getRestoreThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public String startVerification() {
            return null;
        }

        @Override
        public void endVerification(String verificationToken) {
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public void snapshotShard(IndexShard shard, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus) {
        }

        @Override
        public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
            return null;
        }

        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {
        }
    }
}
