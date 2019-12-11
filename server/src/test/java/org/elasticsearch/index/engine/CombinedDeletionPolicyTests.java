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

import com.carrotsearch.hppc.LongArrayList;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CombinedDeletionPolicyTests extends ESTestCase {

    public void testKeepCommitsAfterGlobalCheckpoint() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        final int extraRetainedOps = between(0, 100);
        final SoftDeletesPolicy softDeletesPolicy =
                new SoftDeletesPolicy(globalCheckpoint::get, NO_OPS_PERFORMED, extraRetainedOps, () -> RetentionLeases.EMPTY);
        TranslogDeletionPolicy translogPolicy = createTranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = newCombinedDeletionPolicy(translogPolicy, softDeletesPolicy, globalCheckpoint);

        final LongArrayList maxSeqNoList = new LongArrayList();
        final LongArrayList translogGenList = new LongArrayList();
        final List<IndexCommit> commitList = new ArrayList<>();
        int totalCommits = between(2, 20);
        long lastMaxSeqNo = 0;
        long lastCheckpoint = lastMaxSeqNo;
        long lastTranslogGen = 0;
        final UUID translogUUID = UUID.randomUUID();
        for (int i = 0; i < totalCommits; i++) {
            lastMaxSeqNo += between(1, 10000);
            lastCheckpoint = randomLongBetween(lastCheckpoint, lastMaxSeqNo);
            lastTranslogGen += between(1, 100);
            commitList.add(mockIndexCommit(lastCheckpoint, lastMaxSeqNo, translogUUID, lastTranslogGen));
            maxSeqNoList.add(lastMaxSeqNo);
            translogGenList.add(lastTranslogGen);
        }

        int keptIndex = randomInt(commitList.size() - 1);
        final long lower = maxSeqNoList.get(keptIndex);
        final long upper = keptIndex == commitList.size() - 1 ?
            Long.MAX_VALUE : Math.max(maxSeqNoList.get(keptIndex), maxSeqNoList.get(keptIndex + 1) - 1);
        globalCheckpoint.set(randomLongBetween(lower, upper));
        indexPolicy.onCommit(commitList);

        for (int i = 0; i < commitList.size(); i++) {
            if (i < keptIndex) {
                verify(commitList.get(i), times(1)).delete();
            } else {
                verify(commitList.get(i), never()).delete();
            }
        }
        assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(translogGenList.get(keptIndex)));
        assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(lastTranslogGen));
        assertThat(softDeletesPolicy.getMinRetainedSeqNo(),
            equalTo(Math.max(NO_OPS_PERFORMED,
                Math.min(getLocalCheckpoint(commitList.get(keptIndex)) + 1, globalCheckpoint.get() + 1 - extraRetainedOps))));
    }

    public void testAcquireIndexCommit() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        final int extraRetainedOps = between(0, 100);
        final SoftDeletesPolicy softDeletesPolicy =
                new SoftDeletesPolicy(globalCheckpoint::get, -1, extraRetainedOps, () -> RetentionLeases.EMPTY);
        final UUID translogUUID = UUID.randomUUID();
        TranslogDeletionPolicy translogPolicy = createTranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = newCombinedDeletionPolicy(translogPolicy, softDeletesPolicy, globalCheckpoint);
        long lastMaxSeqNo = between(1, 1000);
        long lastCheckpoint = randomLongBetween(-1, lastMaxSeqNo);
        long lastTranslogGen = between(1, 20);
        int safeIndex = 0;
        List<IndexCommit> commitList = new ArrayList<>();
        List<IndexCommit> snapshottingCommits = new ArrayList<>();
        final int iters = between(10, 100);
        for (int i = 0; i < iters; i++) {
            int newCommits = between(1, 10);
            for (int n = 0; n < newCommits; n++) {
                lastMaxSeqNo += between(1, 1000);
                lastCheckpoint = randomLongBetween(lastCheckpoint, lastMaxSeqNo);
                lastTranslogGen += between(1, 20);
                commitList.add(mockIndexCommit(lastCheckpoint, lastMaxSeqNo, translogUUID, lastTranslogGen));
            }
            // Advance the global checkpoint to between [safeIndex, safeIndex + 1)
            safeIndex = randomIntBetween(safeIndex, commitList.size() - 1);
            long lower = Math.max(globalCheckpoint.get(),
                Long.parseLong(commitList.get(safeIndex).getUserData().get(SequenceNumbers.MAX_SEQ_NO)));
            long upper = safeIndex == commitList.size() - 1 ? lastMaxSeqNo :
                Long.parseLong(commitList.get(safeIndex + 1).getUserData().get(SequenceNumbers.MAX_SEQ_NO)) - 1;
            globalCheckpoint.set(randomLongBetween(lower, upper));
            commitList.forEach(this::resetDeletion);
            indexPolicy.onCommit(commitList);
            IndexCommit safeCommit = CombinedDeletionPolicy.findSafeCommitPoint(commitList, globalCheckpoint.get());
            assertThat(softDeletesPolicy.getMinRetainedSeqNo(), equalTo(
                Math.max(NO_OPS_PERFORMED, Math.min(getLocalCheckpoint(safeCommit) + 1, globalCheckpoint.get() + 1 - extraRetainedOps))));
            // Captures and releases some commits
            int captures = between(0, 5);
            for (int n = 0; n < captures; n++) {
                boolean safe = randomBoolean();
                final IndexCommit snapshot = indexPolicy.acquireIndexCommit(safe);
                expectThrows(UnsupportedOperationException.class, snapshot::delete);
                snapshottingCommits.add(snapshot);
                if (safe) {
                    assertThat(snapshot.getUserData(), equalTo(commitList.get(safeIndex).getUserData()));
                } else {
                    assertThat(snapshot.getUserData(), equalTo(commitList.get(commitList.size() - 1).getUserData()));
                }
            }
            final List<IndexCommit> releasingSnapshots = randomSubsetOf(snapshottingCommits);
            for (IndexCommit snapshot : releasingSnapshots) {
                snapshottingCommits.remove(snapshot);
                final long pendingSnapshots = snapshottingCommits.stream().filter(snapshot::equals).count();
                final IndexCommit lastCommit = commitList.get(commitList.size() - 1);
                safeCommit = CombinedDeletionPolicy.findSafeCommitPoint(commitList, globalCheckpoint.get());
                assertThat(indexPolicy.releaseCommit(snapshot),
                    equalTo(pendingSnapshots == 0 && snapshot.equals(lastCommit) == false && snapshot.equals(safeCommit) == false));
            }
            // Snapshotting commits must not be deleted.
            snapshottingCommits.forEach(snapshot -> assertThat(snapshot.isDeleted(), equalTo(false)));
            // We don't need to retain translog for snapshotting commits.
            assertThat(translogPolicy.getMinTranslogGenerationForRecovery(),
                equalTo(Long.parseLong(commitList.get(safeIndex).getUserData().get(Translog.TRANSLOG_GENERATION_KEY))));
            assertThat(translogPolicy.getTranslogGenerationOfLastCommit(),
                equalTo(Long.parseLong(commitList.get(commitList.size() - 1).getUserData().get(Translog.TRANSLOG_GENERATION_KEY))));
            assertThat(softDeletesPolicy.getMinRetainedSeqNo(), equalTo(
                Math.max(NO_OPS_PERFORMED,
                    Math.min(getLocalCheckpoint(commitList.get(safeIndex)) + 1, globalCheckpoint.get() + 1 - extraRetainedOps))));
        }
        snapshottingCommits.forEach(indexPolicy::releaseCommit);
        globalCheckpoint.set(randomLongBetween(lastMaxSeqNo, Long.MAX_VALUE));
        commitList.forEach(this::resetDeletion);
        indexPolicy.onCommit(commitList);
        for (int i = 0; i < commitList.size() - 1; i++) {
            assertThat(commitList.get(i).isDeleted(), equalTo(true));
        }
        assertThat(commitList.get(commitList.size() - 1).isDeleted(), equalTo(false));
        assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(lastTranslogGen));
        assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(lastTranslogGen));
        IndexCommit safeCommit = CombinedDeletionPolicy.findSafeCommitPoint(commitList, globalCheckpoint.get());
        assertThat(softDeletesPolicy.getMinRetainedSeqNo(), equalTo(
            Math.max(NO_OPS_PERFORMED, Math.min(getLocalCheckpoint(safeCommit) + 1, globalCheckpoint.get() + 1 - extraRetainedOps))));
    }

    public void testDeleteInvalidCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(randomNonNegativeLong());
        final SoftDeletesPolicy softDeletesPolicy = new SoftDeletesPolicy(globalCheckpoint::get, -1, 0, () -> RetentionLeases.EMPTY);
        TranslogDeletionPolicy translogPolicy = createTranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = newCombinedDeletionPolicy(translogPolicy, softDeletesPolicy, globalCheckpoint);

        final int invalidCommits = between(1, 10);
        final List<IndexCommit> commitList = new ArrayList<>();
        for (int i = 0; i < invalidCommits; i++) {
            long maxSeqNo = randomNonNegativeLong();
            commitList.add(mockIndexCommit(randomLongBetween(-1, maxSeqNo), maxSeqNo, UUID.randomUUID(), randomNonNegativeLong()));
        }

        final UUID expectedTranslogUUID = UUID.randomUUID();
        long lastTranslogGen = 0;
        final int validCommits = between(1, 10);
        long lastMaxSeqNo = between(1, 1000);
        long lastCheckpoint = randomLongBetween(-1, lastMaxSeqNo);
        for (int i = 0; i < validCommits; i++) {
            lastTranslogGen += between(1, 1000);
            lastMaxSeqNo += between(1, 1000);
            lastCheckpoint = randomLongBetween(lastCheckpoint, lastMaxSeqNo);
            commitList.add(mockIndexCommit(lastCheckpoint, lastMaxSeqNo, expectedTranslogUUID, lastTranslogGen));
        }

        // We should never keep invalid commits regardless of the value of the global checkpoint.
        indexPolicy.onCommit(commitList);
        for (int i = 0; i < invalidCommits - 1; i++) {
            verify(commitList.get(i), times(1)).delete();
        }
        assertThat(softDeletesPolicy.getMinRetainedSeqNo(),
            equalTo(getLocalCheckpoint(CombinedDeletionPolicy.findSafeCommitPoint(commitList, globalCheckpoint.get())) + 1));
    }

    public void testCheckUnreferencedCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final SoftDeletesPolicy softDeletesPolicy = new SoftDeletesPolicy(globalCheckpoint::get, -1, 0, () -> RetentionLeases.EMPTY);
        final UUID translogUUID = UUID.randomUUID();
        final TranslogDeletionPolicy translogPolicy = createTranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = newCombinedDeletionPolicy(translogPolicy, softDeletesPolicy, globalCheckpoint);
        final List<IndexCommit> commitList = new ArrayList<>();
        int totalCommits = between(2, 20);
        long lastMaxSeqNo = between(1, 1000);
        long lastCheckpoint = randomLongBetween(-1, lastMaxSeqNo);
        long lastTranslogGen = between(1, 50);
        for (int i = 0; i < totalCommits; i++) {
            lastMaxSeqNo += between(1, 10000);
            lastTranslogGen += between(1, 100);
            lastCheckpoint = randomLongBetween(lastCheckpoint, lastMaxSeqNo);
            commitList.add(mockIndexCommit(lastCheckpoint, lastMaxSeqNo, translogUUID, lastTranslogGen));
        }
        int safeCommitIndex = randomIntBetween(0, commitList.size() - 1);
        globalCheckpoint.set(Long.parseLong(commitList.get(safeCommitIndex).getUserData().get(SequenceNumbers.MAX_SEQ_NO)));
        commitList.forEach(this::resetDeletion);
        indexPolicy.onCommit(commitList);

        if (safeCommitIndex == commitList.size() - 1) {
            // Safe commit is the last commit - no need to clean up
            assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(lastTranslogGen));
            assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(lastTranslogGen));
            assertThat(indexPolicy.hasUnreferencedCommits(), equalTo(false));
        } else {
            // Advanced but not enough for any commit after the safe commit becomes safe
            IndexCommit nextSafeCommit = commitList.get(safeCommitIndex + 1);
            globalCheckpoint.set(randomLongBetween(globalCheckpoint.get(),
                Long.parseLong(nextSafeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)) - 1));
            assertFalse(indexPolicy.hasUnreferencedCommits());
            // Advanced enough for some index commit becomes safe
            globalCheckpoint.set(randomLongBetween(
                Long.parseLong(nextSafeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)), lastMaxSeqNo));
            assertTrue(indexPolicy.hasUnreferencedCommits());
            // Advanced enough for the last commit becomes safe
            globalCheckpoint.set(randomLongBetween(lastMaxSeqNo, Long.MAX_VALUE));
            commitList.forEach(this::resetDeletion);
            indexPolicy.onCommit(commitList);
            // Safe commit is the last commit - no need to clean up
            assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(lastTranslogGen));
            assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(lastTranslogGen));
            assertThat(indexPolicy.hasUnreferencedCommits(), equalTo(false));
        }
    }

    private CombinedDeletionPolicy newCombinedDeletionPolicy(TranslogDeletionPolicy translogPolicy, SoftDeletesPolicy softDeletesPolicy,
                                                             AtomicLong globalCheckpoint) {
        return new CombinedDeletionPolicy(logger, translogPolicy, softDeletesPolicy, globalCheckpoint::get)
        {
            @Override
            protected int getDocCountOfCommit(IndexCommit indexCommit) {
                return between(0, 1000);
            }
        };
    }

    IndexCommit mockIndexCommit(long localCheckpoint, long maxSeqNo, UUID translogUUID, long translogGen) throws IOException {
        final Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(Translog.TRANSLOG_UUID_KEY, translogUUID.toString());
        userData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(translogGen));
        final IndexCommit commit = mock(IndexCommit.class);
        final Directory directory = mock(Directory.class);
        when(commit.getUserData()).thenReturn(userData);
        when(commit.getDirectory()).thenReturn(directory);
        resetDeletion(commit);
        return commit;
    }

    void resetDeletion(IndexCommit commit) {
        final AtomicBoolean deleted = new AtomicBoolean();
        when(commit.isDeleted()).thenAnswer(args -> deleted.get());
        doAnswer(arg -> {
            deleted.set(true);
            return null;
        }).when(commit).delete();
    }

    private long getLocalCheckpoint(IndexCommit commit) throws IOException {
        return Long.parseLong(commit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
    }
}
