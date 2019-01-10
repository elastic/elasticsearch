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

package org.elasticsearch.index.seqno;

import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.IndexSettingsModule;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ReplicationTrackerRetentionLeaseTests extends ReplicationTrackerTestCase {

    public void testAddOrUpdateRetentionLease() {
        final AllocationId id = AllocationId.newInitializing();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
                new ShardId("test", "_na", 0),
                id.getId(),
                IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
                UNASSIGNED_SEQ_NO,
                value -> {},
                () -> 0L);
        replicationTracker.updateFromMaster(
                randomNonNegativeLong(),
                Collections.singleton(id.getId()),
                routingTable(Collections.emptySet(), id),
                Collections.emptySet());
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final int length = randomIntBetween(0, 8);
        final long[] minimumRetainingSequenceNumbers = new long[length];
        for (int i = 0; i < length; i++) {
            minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            replicationTracker.addOrUpdateRetentionLease(Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i);
            assertRetentionLeases(replicationTracker, i + 1, minimumRetainingSequenceNumbers, () -> 0L);
        }

        for (int i = 0; i < length; i++) {
            minimumRetainingSequenceNumbers[i] = randomLongBetween(minimumRetainingSequenceNumbers[i], Long.MAX_VALUE);
            replicationTracker.addOrUpdateRetentionLease(Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i);
            assertRetentionLeases(replicationTracker, length, minimumRetainingSequenceNumbers, () -> 0L);
        }

    }

    public void testExpiration() {
        final AllocationId id = AllocationId.newInitializing();
        final AtomicLong currentTimeMillis = new AtomicLong(randomLongBetween(0, 1024));
        final long retentionLeaseMillis = randomLongBetween(1, TimeValue.timeValueHours(12).millis());
        final Settings settings = Settings
                .builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(), TimeValue.timeValueMillis(retentionLeaseMillis))
                .build();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
                new ShardId("test", "_na", 0),
                id.getId(),
                IndexSettingsModule.newIndexSettings("test", settings),
                UNASSIGNED_SEQ_NO,
                value -> {},
                currentTimeMillis::get);
        replicationTracker.updateFromMaster(
                randomNonNegativeLong(),
                Collections.singleton(id.getId()),
                routingTable(Collections.emptySet(), id),
                Collections.emptySet());
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final long[] retainingSequenceNumbers = new long[1];
        retainingSequenceNumbers[0] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        replicationTracker.addOrUpdateRetentionLease("0", retainingSequenceNumbers[0], "test-0");

        {
            final Collection<RetentionLease> retentionLeases = replicationTracker.getRetentionLeases();
            assertThat(retentionLeases, hasSize(1));
            final RetentionLease retentionLease = retentionLeases.iterator().next();
            assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, currentTimeMillis::get);
        }

        // renew the lease
        currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(0, 1024));
        retainingSequenceNumbers[0] = randomLongBetween(retainingSequenceNumbers[0], Long.MAX_VALUE);
        replicationTracker.addOrUpdateRetentionLease("0", retainingSequenceNumbers[0], "test-0");

        {
            final Collection<RetentionLease> retentionLeases = replicationTracker.getRetentionLeases();
            assertThat(retentionLeases, hasSize(1));
            final RetentionLease retentionLease = retentionLeases.iterator().next();
            assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, currentTimeMillis::get);
        }

        // now force the lease to expire
        currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(retentionLeaseMillis, Long.MAX_VALUE - currentTimeMillis.get()));
        assertRetentionLeases(replicationTracker, 0, retainingSequenceNumbers, currentTimeMillis::get);
    }

    private void assertRetentionLeases(
            final ReplicationTracker replicationTracker,
            final int size,
            final long[] minimumRetainingSequenceNumbers,
            final LongSupplier currentTimeMillisSupplier) {
        final Collection<RetentionLease> retentionLeases = replicationTracker.getRetentionLeases();
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
                    lessThanOrEqualTo(replicationTracker.indexSettings().getRetentionLeaseMillis()));
            assertThat(retentionLease.source(), equalTo("test-" + i));
        }
    }

}
