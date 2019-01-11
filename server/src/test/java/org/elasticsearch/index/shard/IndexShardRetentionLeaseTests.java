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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

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

    public void testAddOrUpdateRetentionLease() throws IOException {
        final IndexShard indexShard = newStartedShard(true);
        try {
            final int length = randomIntBetween(0, 8);
            final long[] minimumRetainingSequenceNumbers = new long[length];
            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                indexShard.addOrUpdateRetentionLease(Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i);
                assertRetentionLeases(indexShard, i + 1, minimumRetainingSequenceNumbers, () -> 0L);
            }

            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(minimumRetainingSequenceNumbers[i], Long.MAX_VALUE);
                indexShard.addOrUpdateRetentionLease(Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i);
                assertRetentionLeases(indexShard, length, minimumRetainingSequenceNumbers, () -> 0L);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testExpiration() throws IOException {
        final long retentionLeaseMillis = randomLongBetween(1, TimeValue.timeValueHours(12).millis());
        final Settings settings = Settings
                .builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(), TimeValue.timeValueMillis(retentionLeaseMillis))
                .build();
        // current time is mocked through the thread pool
        final IndexShard indexShard = newStartedShard(true, settings, new InternalEngineFactory());
        try {
            final long[] retainingSequenceNumbers = new long[1];
            retainingSequenceNumbers[0] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            indexShard.addOrUpdateRetentionLease("0", retainingSequenceNumbers[0], "test-0");

            {
                final Collection<RetentionLease> retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
                assertThat(retentionLeases, hasSize(1));
                final RetentionLease retentionLease = retentionLeases.iterator().next();
                assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, currentTimeMillis::get);
            }

            // renew the lease
            currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(0, 1024));
            retainingSequenceNumbers[0] = randomLongBetween(retainingSequenceNumbers[0], Long.MAX_VALUE);
            indexShard.addOrUpdateRetentionLease("0", retainingSequenceNumbers[0], "test-0");

            {
                final Collection<RetentionLease> retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
                assertThat(retentionLeases, hasSize(1));
                final RetentionLease retentionLease = retentionLeases.iterator().next();
                assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, currentTimeMillis::get);
            }

            // now force the lease to expire
            currentTimeMillis.set(
                    currentTimeMillis.get() + randomLongBetween(retentionLeaseMillis, Long.MAX_VALUE - currentTimeMillis.get()));
            assertRetentionLeases(indexShard, 0, retainingSequenceNumbers, currentTimeMillis::get);
        } finally {
            closeShards(indexShard);
        }
    }

    private void assertRetentionLeases(
            final IndexShard indexShard,
            final int size,
            final long[] minimumRetainingSequenceNumbers,
            final LongSupplier currentTimeMillisSupplier) {
        final Collection<RetentionLease> retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
        final Map<String, RetentionLease> idToRetentionLease = new HashMap<>();
        for (final RetentionLease retentionLease : retentionLeases) {
            idToRetentionLease.put(retentionLease.id(), retentionLease);
        }

        assertThat(idToRetentionLease.entrySet(), hasSize(size));
        for (int i = 0; i < size; i++) {
            assertThat(idToRetentionLease.keySet(), hasItem(Integer.toString(i)));
            final RetentionLease retentionLease = idToRetentionLease.get(Integer.toString(i));
            assertThat(retentionLease.retainingSequenceNumber(), equalTo(minimumRetainingSequenceNumbers[i]));
            assertThat(
                    currentTimeMillisSupplier.getAsLong() - retentionLease.timestamp(),
                    lessThanOrEqualTo(indexShard.indexSettings().getRetentionLeaseMillis()));
            assertThat(retentionLease.source(), equalTo("test-" + i));
        }
    }

}
