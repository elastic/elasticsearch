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

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexShardRetentionLeaseTests extends IndexShardTestCase {

    private final AtomicLong currentTimeMillis = new AtomicLong();

    @Override
    protected ThreadPool setUpThreadPool() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        doAnswer(invocationOnMock -> currentTimeMillis.get()).when(threadPool).absoluteTimeInMillis();
        when(threadPool.executor(anyString())).thenReturn(mock(ExecutorService.class));
        when(threadPool.scheduler()).thenReturn(mock(ScheduledExecutorService.class));
        return threadPool;
    }

    @Override
    protected void tearDownThreadPool() {

    }

    public void testAddOrRenewRetentionLease() throws IOException {
        final IndexShard indexShard = newStartedShard(true);
        final long primaryTerm = indexShard.getOperationPrimaryTerm();
        try {
            final int length = randomIntBetween(0, 8);
            final long[] minimumRetainingSequenceNumbers = new long[length];
            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                indexShard.addRetentionLease(
                        Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i, ActionListener.wrap(() -> {}));
                assertRetentionLeases(
                        indexShard, i + 1, minimumRetainingSequenceNumbers, () -> 0L, primaryTerm, 1 + i, true);
            }

            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(minimumRetainingSequenceNumbers[i], Long.MAX_VALUE);
                indexShard.renewRetentionLease(Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i);
                assertRetentionLeases(indexShard, length, minimumRetainingSequenceNumbers, () -> 0L, primaryTerm, 1 + length + i, true);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testExpirationOnPrimary() throws IOException {
        runExpirationTest(true);
    }

    public void testExpirationOnReplica() throws IOException {
        runExpirationTest(false);
    }

    private void runExpirationTest(final boolean primary) throws IOException {
        final long retentionLeaseMillis = randomLongBetween(1, TimeValue.timeValueHours(12).millis());
        final Settings settings = Settings
                .builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(), TimeValue.timeValueMillis(retentionLeaseMillis))
                .build();
        // current time is mocked through the thread pool
        final IndexShard indexShard = newStartedShard(primary, settings, new InternalEngineFactory());
        final long primaryTerm = indexShard.getOperationPrimaryTerm();
        try {
            final long[] retainingSequenceNumbers = new long[1];
            retainingSequenceNumbers[0] = randomLongBetween(0, Long.MAX_VALUE);
            if (primary) {
                indexShard.addRetentionLease("0", retainingSequenceNumbers[0], "test-0", ActionListener.wrap(() -> {
                }));
            } else {
                final RetentionLeases retentionLeases = new RetentionLeases(
                        primaryTerm,
                        1,
                        Collections.singleton(new RetentionLease("0", retainingSequenceNumbers[0], currentTimeMillis.get(), "test-0")));
                indexShard.updateRetentionLeasesOnReplica(retentionLeases);
            }

            {
                final RetentionLeases retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
                assertThat(retentionLeases.version(), equalTo(1L));
                assertThat(retentionLeases.leases(), hasSize(1));
                final RetentionLease retentionLease = retentionLeases.leases().iterator().next();
                assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, currentTimeMillis::get, primaryTerm, 1, primary);
            }

            // renew the lease
            currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(0, 1024));
            retainingSequenceNumbers[0] = randomLongBetween(retainingSequenceNumbers[0], Long.MAX_VALUE);
            if (primary) {
                indexShard.renewRetentionLease("0", retainingSequenceNumbers[0], "test-0");
            } else {
                final RetentionLeases retentionLeases = new RetentionLeases(
                        primaryTerm,
                        2,
                        Collections.singleton(new RetentionLease("0", retainingSequenceNumbers[0], currentTimeMillis.get(), "test-0")));
                indexShard.updateRetentionLeasesOnReplica(retentionLeases);
            }

            {
                final RetentionLeases retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
                assertThat(retentionLeases.version(), equalTo(2L));
                assertThat(retentionLeases.leases(), hasSize(1));
                final RetentionLease retentionLease = retentionLeases.leases().iterator().next();
                assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, currentTimeMillis::get, primaryTerm, 2, primary);
            }

            // now force the lease to expire
            currentTimeMillis.set(
                    currentTimeMillis.get() + randomLongBetween(retentionLeaseMillis, Long.MAX_VALUE - currentTimeMillis.get()));
            if (primary) {
                assertRetentionLeases(indexShard, 0, retainingSequenceNumbers, currentTimeMillis::get, primaryTerm, 3, true);
            } else {
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, currentTimeMillis::get, primaryTerm, 2, false);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testCommit() throws IOException {
        final Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(), Long.MAX_VALUE, TimeUnit.NANOSECONDS)
                .build();
        final IndexShard indexShard = newStartedShard(
                true,
                settings,
                new InternalEngineFactory());
        try {
            final int length = randomIntBetween(0, 8);
            final long[] minimumRetainingSequenceNumbers = new long[length];
            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                currentTimeMillis.set(TimeUnit.NANOSECONDS.toMillis(randomNonNegativeLong()));
                indexShard.addRetentionLease(
                        Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i, ActionListener.wrap(() -> {
                        }));
            }

            currentTimeMillis.set(TimeUnit.NANOSECONDS.toMillis(Long.MAX_VALUE));

            // force a commit
            indexShard.flush(new FlushRequest().force(true));

            // the committed retention leases should equal our current retention leases
            final SegmentInfos segmentCommitInfos = indexShard.store().readLastCommittedSegmentsInfo();
            assertTrue(segmentCommitInfos.getUserData().containsKey(Engine.RETENTION_LEASES));
            final RetentionLeases retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
            final RetentionLeases committedRetentionLeases = IndexShard.getRetentionLeases(segmentCommitInfos);
            if (retentionLeases.leases().isEmpty()) {
                assertThat(committedRetentionLeases.version(), equalTo(0L));
                assertThat(committedRetentionLeases.leases(), empty());
            } else {
                assertThat(committedRetentionLeases.version(), equalTo((long) length));
                assertThat(retentionLeases.leases(), contains(retentionLeases.leases().toArray(new RetentionLease[0])));
            }

            // when we recover, we should recover the retention leases
            final IndexShard recoveredShard = reinitShard(
                    indexShard,
                    ShardRoutingHelper.initWithSameId(indexShard.routingEntry(), RecoverySource.ExistingStoreRecoverySource.INSTANCE));
            try {
                recoverShardFromStore(recoveredShard);
                final RetentionLeases recoveredRetentionLeases = recoveredShard.getEngine().config().retentionLeasesSupplier().get();
                if (retentionLeases.leases().isEmpty()) {
                    assertThat(recoveredRetentionLeases.version(), equalTo(0L));
                    assertThat(recoveredRetentionLeases.leases(), empty());
                } else {
                    assertThat(recoveredRetentionLeases.version(), equalTo((long) length));
                    assertThat(
                            recoveredRetentionLeases.leases(),
                            contains(retentionLeases.leases().toArray(new RetentionLease[0])));
                }
            } finally {
                closeShards(recoveredShard);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testRetentionLeaseStats() throws IOException {
        final IndexShard indexShard = newStartedShard(true);
        try {
            final int length = randomIntBetween(0, 8);
            final long[] minimumRetainingSequenceNumbers = new long[length];
            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                indexShard.addRetentionLease(
                        Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i, ActionListener.wrap(() -> {}));
            }
            final RetentionLeaseStats stats = indexShard.getRetentionLeaseStats();
            assertRetentionLeases(
                    stats.retentionLeases(),
                    indexShard.indexSettings().getRetentionLeaseMillis(),
                    length,
                    minimumRetainingSequenceNumbers,
                    () -> 0L,
                    length == 0 ? RetentionLeases.EMPTY.primaryTerm() : indexShard.getOperationPrimaryTerm(),
                    length,
                    true);
        } finally {
            closeShards(indexShard);
        }
    }

    public void testRecoverFromStoreReserveRetentionLeases() throws Exception {
        final AtomicBoolean throwDuringRecoverFromTranslog = new AtomicBoolean();
        final IndexShard shard = newStartedShard(false, Settings.builder().put("index.soft_deletes.enabled", true).build(),
            config -> new InternalEngine(config) {
                @Override
                public InternalEngine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner,
                                                          long recoverUpToSeqNo) throws IOException {
                    if (throwDuringRecoverFromTranslog.get()) {
                        throw new RuntimeException("crashed before recover from translog is completed");
                    }
                    return super.recoverFromTranslog(translogRecoveryRunner, recoverUpToSeqNo);
                }
            });
        final List<RetentionLease> leases = new ArrayList<>();
        long version = randomLongBetween(0, 100);
        long primaryTerm = randomLongBetween(1, 100);
        final int iterations = randomIntBetween(1, 10);
        for (int i = 0; i < iterations; i++) {
            if (randomBoolean()) {
                indexDoc(shard, "_doc", Integer.toString(i));
            } else {
                leases.add(new RetentionLease(Integer.toString(i), randomNonNegativeLong(),
                    randomLongBetween(Integer.MAX_VALUE, Long.MAX_VALUE), "test"));
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    version += randomLongBetween(1, 100);
                    primaryTerm += randomLongBetween(0, 100);
                    shard.updateRetentionLeasesOnReplica(new RetentionLeases(primaryTerm, version, leases));
                    shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
                }
            }
            if (randomBoolean()) {
                shard.updateGlobalCheckpointOnReplica(randomLongBetween(shard.getGlobalCheckpoint(), shard.getLocalCheckpoint()), "test");
                flushShard(shard);
            }
        }
        version += randomLongBetween(1, 100);
        primaryTerm += randomLongBetween(0, 100);
        shard.updateRetentionLeasesOnReplica(new RetentionLeases(primaryTerm, version, leases));
        shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
        closeShard(shard, false);

        final IndexShard failedShard = reinitShard(shard, newShardRouting(shard.routingEntry().shardId(),
            shard.routingEntry().currentNodeId(), true, ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE));
        final DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        failedShard.markAsRecovering("store", new RecoveryState(failedShard.routingEntry(), localNode, null));
        throwDuringRecoverFromTranslog.set(true);
        expectThrows(IndexShardRecoveryException.class, failedShard::recoverFromStore);
        closeShards(failedShard);

        final IndexShard newShard = reinitShard(shard, newShardRouting(shard.routingEntry().shardId(),
            shard.routingEntry().currentNodeId(), true, ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE));
        newShard.markAsRecovering("store", new RecoveryState(failedShard.routingEntry(), localNode, null));
        throwDuringRecoverFromTranslog.set(false);
        assertTrue(newShard.recoverFromStore());
        final RetentionLeases retentionLeases = newShard.getRetentionLeases();
        assertThat(retentionLeases.version(), equalTo(version));
        assertThat(retentionLeases.primaryTerm(), equalTo(primaryTerm));
        if (leases.isEmpty()) {
            assertThat(retentionLeases.leases(), empty());
        } else {
            assertThat(retentionLeases.leases(), containsInAnyOrder(leases.toArray(new RetentionLease[0])));
        }
        closeShards(newShard);
    }

    private void assertRetentionLeases(
            final IndexShard indexShard,
            final int size,
            final long[] minimumRetainingSequenceNumbers,
            final LongSupplier currentTimeMillisSupplier,
            final long primaryTerm,
            final long version,
            final boolean primary) {
        assertRetentionLeases(
                indexShard.getEngine().config().retentionLeasesSupplier().get(),
                indexShard.indexSettings().getRetentionLeaseMillis(),
                size,
                minimumRetainingSequenceNumbers,
                currentTimeMillisSupplier,
                primaryTerm,
                version,
                primary);
    }

    private void assertRetentionLeases(
            final RetentionLeases retentionLeases,
            final long retentionLeaseMillis,
            final int size,
            final long[] minimumRetainingSequenceNumbers,
            final LongSupplier currentTimeMillisSupplier,
            final long primaryTerm,
            final long version,
            final boolean primary) {
        assertThat(retentionLeases.primaryTerm(), equalTo(primaryTerm));
        assertThat(retentionLeases.version(), equalTo(version));
        final Map<String, RetentionLease> idToRetentionLease = new HashMap<>();
        for (final RetentionLease retentionLease : retentionLeases.leases()) {
            idToRetentionLease.put(retentionLease.id(), retentionLease);
        }

        assertThat(idToRetentionLease.entrySet(), hasSize(size));
        for (int i = 0; i < size; i++) {
            assertThat(idToRetentionLease.keySet(), hasItem(Integer.toString(i)));
            final RetentionLease retentionLease = idToRetentionLease.get(Integer.toString(i));
            assertThat(retentionLease.retainingSequenceNumber(), equalTo(minimumRetainingSequenceNumbers[i]));
            if (primary) {
                // retention leases can be expired on replicas, so we can only assert on primaries here
                assertThat(currentTimeMillisSupplier.getAsLong() - retentionLease.timestamp(), lessThanOrEqualTo(retentionLeaseMillis));
            }
            assertThat(retentionLease.source(), equalTo("test-" + i));
        }
    }

}
