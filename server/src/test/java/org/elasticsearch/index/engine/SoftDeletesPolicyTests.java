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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SoftDeletesPolicyTests extends ESTestCase  {

    /**
     * Makes sure we won't advance the retained seq# if the retention lock is held
     */
    public void testSoftDeletesRetentionLock() {
        long retainedOps = between(0, 10000);
        AtomicLong globalCheckpoint = new AtomicLong(NO_OPS_PERFORMED);
        final AtomicLong[] retainingSequenceNumbers = new AtomicLong[randomIntBetween(0, 8)];
        for (int i = 0; i < retainingSequenceNumbers.length; i++) {
            retainingSequenceNumbers[i] = new AtomicLong();
        }
        final Supplier<RetentionLeases> retentionLeasesSupplier =
                () -> {
                    final List<RetentionLease> leases = new ArrayList<>(retainingSequenceNumbers.length);
                    for (int i = 0; i < retainingSequenceNumbers.length; i++) {
                        leases.add(new RetentionLease(Integer.toString(i), retainingSequenceNumbers[i].get(), 0L, "test"));
                    }
                    return new RetentionLeases(1, 1, leases);
                };
        long safeCommitCheckpoint = globalCheckpoint.get();
        SoftDeletesPolicy policy = new SoftDeletesPolicy(globalCheckpoint::get, between(1, 10000), retainedOps, retentionLeasesSupplier);
        long minRetainedSeqNo = policy.getMinRetainedSeqNo();
        List<Releasable> locks = new ArrayList<>();
        int iters = scaledRandomIntBetween(10, 1000);
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                locks.add(policy.acquireRetentionLock());
            }
            // Advances the global checkpoint and the local checkpoint of a safe commit
            globalCheckpoint.addAndGet(between(0, 1000));
            for (final AtomicLong retainingSequenceNumber : retainingSequenceNumbers) {
                retainingSequenceNumber.set(randomLongBetween(retainingSequenceNumber.get(), globalCheckpoint.get()));
            }
            safeCommitCheckpoint = randomLongBetween(safeCommitCheckpoint, globalCheckpoint.get());
            policy.setLocalCheckpointOfSafeCommit(safeCommitCheckpoint);
            if (rarely()) {
                retainedOps = between(0, 10000);
                policy.setRetentionOperations(retainedOps);
            }
            // Release some locks
            List<Releasable> releasingLocks = randomSubsetOf(locks);
            locks.removeAll(releasingLocks);
            releasingLocks.forEach(Releasable::close);

            // getting the query has side effects, updating the internal state of the policy
            final Query query = policy.getRetentionQuery();
            assertThat(query, instanceOf(PointRangeQuery.class));
            final PointRangeQuery retentionQuery = (PointRangeQuery) query;

            // we only expose the minimum sequence number to the merge policy if the retention lock is not held
            if (locks.isEmpty()) {
                final long minimumRetainingSequenceNumber = Arrays.stream(retainingSequenceNumbers)
                        .mapToLong(AtomicLong::get)
                        .min()
                        .orElse(Long.MAX_VALUE);
                long retainedSeqNo =
                        Math.min(safeCommitCheckpoint, Math.min(minimumRetainingSequenceNumber, globalCheckpoint.get() - retainedOps)) + 1;
                minRetainedSeqNo = Math.max(minRetainedSeqNo, retainedSeqNo);
            }
            assertThat(retentionQuery.getNumDims(), equalTo(1));
            assertThat(LongPoint.decodeDimension(retentionQuery.getLowerPoint(), 0), equalTo(minRetainedSeqNo));
            assertThat(LongPoint.decodeDimension(retentionQuery.getUpperPoint(), 0), equalTo(Long.MAX_VALUE));
            assertThat(policy.getMinRetainedSeqNo(), equalTo(minRetainedSeqNo));
        }

        locks.forEach(Releasable::close);
        final long minimumRetainingSequenceNumber = Arrays.stream(retainingSequenceNumbers)
                .mapToLong(AtomicLong::get)
                .min()
                .orElse(Long.MAX_VALUE);
        long retainedSeqNo =
                Math.min(safeCommitCheckpoint, Math.min(minimumRetainingSequenceNumber, globalCheckpoint.get() - retainedOps)) + 1;
        minRetainedSeqNo = Math.max(minRetainedSeqNo, retainedSeqNo);
        assertThat(policy.getMinRetainedSeqNo(), equalTo(minRetainedSeqNo));
    }

    public void testAlwaysFetchLatestRetentionLeases() {
        final AtomicLong globalCheckpoint = new AtomicLong(NO_OPS_PERFORMED);
        final Collection<RetentionLease> leases = new ArrayList<>();
        final int numLeases = randomIntBetween(0, 10);
        for (int i = 0; i < numLeases; i++) {
            leases.add(new RetentionLease(Integer.toString(i), randomLongBetween(0, 1000), randomNonNegativeLong(), "test"));
        }
        final Supplier<RetentionLeases> leasesSupplier =
                () -> new RetentionLeases(
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        Collections.unmodifiableCollection(new ArrayList<>(leases)));
        final SoftDeletesPolicy policy =
                new SoftDeletesPolicy(globalCheckpoint::get, randomIntBetween(1, 1000), randomIntBetween(0, 1000), leasesSupplier);
        if (randomBoolean()) {
            policy.acquireRetentionLock();
        }
        if (numLeases == 0) {
            assertThat(policy.getRetentionPolicy().v2().leases(), empty());
        } else {
            assertThat(policy.getRetentionPolicy().v2().leases(), contains(leases.toArray(new RetentionLease[0])));
        }
    }
}
