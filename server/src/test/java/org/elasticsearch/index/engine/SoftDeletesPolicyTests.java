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

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class SoftDeletesPolicyTests extends ESTestCase  {
    /**
     * Makes sure we won't advance the retained seq# if the retention lock is held
     */
    public void testSoftDeletesRetentionLock() {
        long retainedOps = between(0, 10000);
        AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        long safeCommitCheckpoint = globalCheckpoint.get();
        SoftDeletesPolicy policy = new SoftDeletesPolicy(globalCheckpoint::get, between(1, 10000), retainedOps);
        long minRetainedSeqNo = policy.getMinRetainedSeqNo();
        List<Releasable> locks = new ArrayList<>();
        int iters = scaledRandomIntBetween(10, 1000);
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                locks.add(policy.acquireRetentionLock());
            }
            // Advances the global checkpoint and the local checkpoint of a safe commit
            globalCheckpoint.addAndGet(between(0, 1000));
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

            // We only expose the seqno to the merge policy if the retention lock is not held.
            policy.getRetentionQuery();
            if (locks.isEmpty()) {
                long retainedSeqNo = Math.min(safeCommitCheckpoint, globalCheckpoint.get() - retainedOps) + 1;
                minRetainedSeqNo = Math.max(minRetainedSeqNo, retainedSeqNo);
            }
            assertThat(policy.getMinRetainedSeqNo(), equalTo(minRetainedSeqNo));
        }

        locks.forEach(Releasable::close);
        long retainedSeqNo = Math.min(safeCommitCheckpoint, globalCheckpoint.get() - retainedOps) + 1;
        minRetainedSeqNo = Math.max(minRetainedSeqNo, retainedSeqNo);
        assertThat(policy.getMinRetainedSeqNo(), equalTo(minRetainedSeqNo));
    }
}
