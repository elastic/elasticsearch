/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
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
        final SoftDeletesPolicy softDeletesPolicy = new SoftDeletesPolicy(
            globalCheckpoint::get,
            NO_OPS_PERFORMED,
            extraRetainedOps,
            () -> RetentionLeases.EMPTY
        );
        TranslogDeletionPolicy translogPolicy = new TranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = newCombinedDeletionPolicy(translogPolicy, softDeletesPolicy, globalCheckpoint);

        final List<Long> maxSeqNoList = new ArrayList<>();
        final List<IndexCommit> commitList = new ArrayList<>();
        int totalCommits = between(2, 20);
        long lastMaxSeqNo = 0;
        long lastCheckpoint = lastMaxSeqNo;
        final UUID translogUUID = UUID.randomUUID();
        for (int i = 0; i < totalCommits; i++) {
            lastMaxSeqNo += between(1, 10000);
            lastCheckpoint = randomLongBetween(lastCheckpoint, lastMaxSeqNo);
            commitList.add(mockIndexCommit(lastCheckpoint, lastMaxSeqNo, translogUUID));
            maxSeqNoList.add(lastMaxSeqNo);
        }

        int keptIndex = randomInt(commitList.size() - 1);
        final long lower = maxSeqNoList.get(keptIndex);
        final long upper = keptIndex == commitList.size() - 1
            ? Long.MAX_VALUE
            : Math.max(maxSeqNoList.get(keptIndex), maxSeqNoList.get(keptIndex + 1) - 1);
        globalCheckpoint.set(randomLongBetween(lower, upper));
        indexPolicy.onCommit(commitList);

        for (int i = 0; i < commitList.size(); i++) {
            if (i < keptIndex) {
                verify(commitList.get(i), times(1)).delete();
            } else {
                verify(commitList.get(i), never()).delete();
            }
        }
        assertThat(translogPolicy.getLocalCheckpointOfSafeCommit(), equalTo(getLocalCheckpoint(commitList.get(keptIndex))));
        assertThat(
            softDeletesPolicy.getMinRetainedSeqNo(),
            equalTo(
                Math.max(
                    NO_OPS_PERFORMED,
                    Math.min(getLocalCheckpoint(commitList.get(keptIndex)) + 1, globalCheckpoint.get() + 1 - extraRetainedOps)
                )
            )
        );
    }

    public void testAcquireIndexCommit() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        final int extraRetainedOps = between(0, 100);
        final SoftDeletesPolicy softDeletesPolicy = new SoftDeletesPolicy(
            globalCheckpoint::get,
            -1,
            extraRetainedOps,
            () -> RetentionLeases.EMPTY
        );
        final UUID translogUUID = UUID.randomUUID();
        TranslogDeletionPolicy translogPolicy = new TranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = newCombinedDeletionPolicy(translogPolicy, softDeletesPolicy, globalCheckpoint);
        long lastMaxSeqNo = between(1, 1000);
        long lastCheckpoint = randomLongBetween(-1, lastMaxSeqNo);
        int safeIndex = 0;
        List<IndexCommit> commitList = new ArrayList<>();
        List<IndexCommit> snapshottingCommits = new ArrayList<>();
        final int iters = between(10, 100);
        for (int i = 0; i < iters; i++) {
            int newCommits = between(1, 10);
            for (int n = 0; n < newCommits; n++) {
                lastMaxSeqNo += between(1, 1000);
                lastCheckpoint = randomLongBetween(lastCheckpoint, lastMaxSeqNo);
                commitList.add(mockIndexCommit(lastCheckpoint, lastMaxSeqNo, translogUUID));
            }
            // Advance the global checkpoint to between [safeIndex, safeIndex + 1)
            safeIndex = randomIntBetween(safeIndex, commitList.size() - 1);
            long lower = Math.max(
                globalCheckpoint.get(),
                Long.parseLong(commitList.get(safeIndex).getUserData().get(SequenceNumbers.MAX_SEQ_NO))
            );
            long upper = safeIndex == commitList.size() - 1
                ? lastMaxSeqNo
                : Long.parseLong(commitList.get(safeIndex + 1).getUserData().get(SequenceNumbers.MAX_SEQ_NO)) - 1;
            globalCheckpoint.set(randomLongBetween(lower, upper));
            commitList.forEach(this::resetDeletion);
            indexPolicy.onCommit(commitList);
            IndexCommit safeCommit = CombinedDeletionPolicy.findSafeCommitPoint(commitList, globalCheckpoint.get());
            assertThat(
                softDeletesPolicy.getMinRetainedSeqNo(),
                equalTo(
                    Math.max(NO_OPS_PERFORMED, Math.min(getLocalCheckpoint(safeCommit) + 1, globalCheckpoint.get() + 1 - extraRetainedOps))
                )
            );
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
                assertThat(
                    indexPolicy.releaseCommit(snapshot),
                    equalTo(pendingSnapshots == 0 && snapshot.equals(lastCommit) == false && snapshot.equals(safeCommit) == false)
                );
            }
            // Snapshotting commits must not be deleted.
            snapshottingCommits.forEach(snapshot -> assertThat(snapshot.isDeleted(), equalTo(false)));
            // We don't need to retain translog for snapshotting commits.
            assertThat(translogPolicy.getLocalCheckpointOfSafeCommit(), equalTo(getLocalCheckpoint(commitList.get(safeIndex))));
            assertThat(
                softDeletesPolicy.getMinRetainedSeqNo(),
                equalTo(
                    Math.max(
                        NO_OPS_PERFORMED,
                        Math.min(getLocalCheckpoint(commitList.get(safeIndex)) + 1, globalCheckpoint.get() + 1 - extraRetainedOps)
                    )
                )
            );
        }
        snapshottingCommits.forEach(indexPolicy::releaseCommit);
        globalCheckpoint.set(randomLongBetween(lastMaxSeqNo, Long.MAX_VALUE));
        commitList.forEach(this::resetDeletion);
        indexPolicy.onCommit(commitList);
        for (int i = 0; i < commitList.size() - 1; i++) {
            assertThat(commitList.get(i).isDeleted(), equalTo(true));
        }
        assertThat(commitList.get(commitList.size() - 1).isDeleted(), equalTo(false));
        assertThat(translogPolicy.getLocalCheckpointOfSafeCommit(), equalTo(getLocalCheckpoint(commitList.get(commitList.size() - 1))));
        IndexCommit safeCommit = CombinedDeletionPolicy.findSafeCommitPoint(commitList, globalCheckpoint.get());
        assertThat(
            softDeletesPolicy.getMinRetainedSeqNo(),
            equalTo(Math.max(NO_OPS_PERFORMED, Math.min(getLocalCheckpoint(safeCommit) + 1, globalCheckpoint.get() + 1 - extraRetainedOps)))
        );
    }

    public void testDeleteInvalidCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(randomNonNegativeLong());
        final SoftDeletesPolicy softDeletesPolicy = new SoftDeletesPolicy(globalCheckpoint::get, -1, 0, () -> RetentionLeases.EMPTY);
        TranslogDeletionPolicy translogPolicy = new TranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = newCombinedDeletionPolicy(translogPolicy, softDeletesPolicy, globalCheckpoint);

        final int invalidCommits = between(1, 10);
        final List<IndexCommit> commitList = new ArrayList<>();
        for (int i = 0; i < invalidCommits; i++) {
            long maxSeqNo = randomNonNegativeLong();
            commitList.add(mockIndexCommit(randomLongBetween(-1, maxSeqNo), maxSeqNo, UUID.randomUUID()));
        }

        final UUID expectedTranslogUUID = UUID.randomUUID();
        final int validCommits = between(1, 10);
        long lastMaxSeqNo = between(1, 1000);
        long lastCheckpoint = randomLongBetween(-1, lastMaxSeqNo);
        for (int i = 0; i < validCommits; i++) {
            lastMaxSeqNo += between(1, 1000);
            lastCheckpoint = randomLongBetween(lastCheckpoint, lastMaxSeqNo);
            commitList.add(mockIndexCommit(lastCheckpoint, lastMaxSeqNo, expectedTranslogUUID));
        }

        // We should never keep invalid commits regardless of the value of the global checkpoint.
        indexPolicy.onCommit(commitList);
        for (int i = 0; i < invalidCommits - 1; i++) {
            verify(commitList.get(i), times(1)).delete();
        }
        assertThat(
            softDeletesPolicy.getMinRetainedSeqNo(),
            equalTo(getLocalCheckpoint(CombinedDeletionPolicy.findSafeCommitPoint(commitList, globalCheckpoint.get())) + 1)
        );
    }

    public void testCheckUnreferencedCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final SoftDeletesPolicy softDeletesPolicy = new SoftDeletesPolicy(globalCheckpoint::get, -1, 0, () -> RetentionLeases.EMPTY);
        final UUID translogUUID = UUID.randomUUID();
        final TranslogDeletionPolicy translogPolicy = new TranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = newCombinedDeletionPolicy(translogPolicy, softDeletesPolicy, globalCheckpoint);
        final List<IndexCommit> commitList = new ArrayList<>();
        int totalCommits = between(2, 20);
        long lastMaxSeqNo = between(1, 1000);
        long lastCheckpoint = randomLongBetween(-1, lastMaxSeqNo);
        for (int i = 0; i < totalCommits; i++) {
            lastMaxSeqNo += between(1, 10000);
            lastCheckpoint = randomLongBetween(lastCheckpoint, lastMaxSeqNo);
            commitList.add(mockIndexCommit(lastCheckpoint, lastMaxSeqNo, translogUUID));
        }
        int safeCommitIndex = randomIntBetween(0, commitList.size() - 1);
        globalCheckpoint.set(Long.parseLong(commitList.get(safeCommitIndex).getUserData().get(SequenceNumbers.MAX_SEQ_NO)));
        commitList.forEach(this::resetDeletion);
        indexPolicy.onCommit(commitList);

        if (safeCommitIndex == commitList.size() - 1) {
            // Safe commit is the last commit - no need to clean up
            assertThat(translogPolicy.getLocalCheckpointOfSafeCommit(), equalTo(getLocalCheckpoint(commitList.get(commitList.size() - 1))));
            assertThat(indexPolicy.hasUnreferencedCommits(), equalTo(false));
        } else {
            // Advanced but not enough for any commit after the safe commit becomes safe
            IndexCommit nextSafeCommit = commitList.get(safeCommitIndex + 1);
            globalCheckpoint.set(
                randomLongBetween(globalCheckpoint.get(), Long.parseLong(nextSafeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)) - 1)
            );
            assertFalse(indexPolicy.hasUnreferencedCommits());
            // Advanced enough for some index commit becomes safe
            globalCheckpoint.set(
                randomLongBetween(Long.parseLong(nextSafeCommit.getUserData().get(SequenceNumbers.MAX_SEQ_NO)), lastMaxSeqNo)
            );
            assertTrue(indexPolicy.hasUnreferencedCommits());
            // Advanced enough for the last commit becomes safe
            globalCheckpoint.set(randomLongBetween(lastMaxSeqNo, Long.MAX_VALUE));
            commitList.forEach(this::resetDeletion);
            indexPolicy.onCommit(commitList);
            // Safe commit is the last commit - no need to clean up
            assertThat(translogPolicy.getLocalCheckpointOfSafeCommit(), equalTo(getLocalCheckpoint(commitList.get(commitList.size() - 1))));
            assertThat(indexPolicy.hasUnreferencedCommits(), equalTo(false));
        }
    }

    public void testCommitsListener() throws Exception {
        final List<IndexCommit> acquiredCommits = new ArrayList<>();
        final List<IndexCommit> deletedCommits = new ArrayList<>();
        final Set<String> newCommitFiles = new HashSet<>();
        final CombinedDeletionPolicy.CommitsListener commitsListener = new CombinedDeletionPolicy.CommitsListener() {
            @Override
            public void onNewAcquiredCommit(IndexCommit commit, Set<String> additionalFiles) {
                assertThat(commit, instanceOf(FilterIndexCommit.class));
                assertThat(acquiredCommits.add(((FilterIndexCommit) commit).getIndexCommit()), equalTo(true));
                assertThat(additionalFiles, not(empty()));
                newCommitFiles.clear();
                newCommitFiles.addAll(additionalFiles);
            }

            @Override
            public void onDeletedCommit(IndexCommit commit) {
                assertThat(acquiredCommits.remove(commit), equalTo(true));
                assertThat(deletedCommits.add(commit), equalTo(true));
                assertThat(commit.isDeleted(), equalTo(true));
            }
        };

        final AtomicLong globalCheckpoint = new AtomicLong(0L);
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy();
        final SoftDeletesPolicy softDeletesPolicy = new SoftDeletesPolicy(
            globalCheckpoint::get,
            NO_OPS_PERFORMED,
            0L,
            () -> RetentionLeases.EMPTY
        );
        final CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(
            logger,
            translogDeletionPolicy,
            softDeletesPolicy,
            globalCheckpoint::get,
            commitsListener
        ) {
            @Override
            protected int getDocCountOfCommit(IndexCommit indexCommit) {
                return 10;
            }

            @Override
            synchronized boolean releaseCommit(IndexCommit indexCommit) {
                return super.releaseCommit(wrapCommit(indexCommit));
            }
        };

        final UUID translogUUID = UUID.randomUUID();
        final IndexCommit commit0 = mockIndexCommit(NO_OPS_PERFORMED, NO_OPS_PERFORMED, translogUUID, Set.of("segments_0"));
        combinedDeletionPolicy.onInit(List.of(commit0));

        assertThat(acquiredCommits, contains(commit0));
        assertThat(deletedCommits, hasSize(0));
        assertThat(newCommitFiles, contains("segments_0"));

        final IndexCommit commit1 = mockIndexCommit(10L, 10L, translogUUID, Set.of("_0.cfe", "_0.si", "_0.cfs", "segments_1"));
        combinedDeletionPolicy.onCommit(List.of(commit0, commit1));

        assertThat(acquiredCommits, contains(commit0, commit1));
        assertThat(deletedCommits, hasSize(0));
        assertThat(newCommitFiles, containsInAnyOrder(equalTo("_0.cfe"), equalTo("_0.si"), equalTo("_0.cfs"), equalTo("segments_1")));

        globalCheckpoint.set(10L);
        final IndexCommit commit2 = mockIndexCommit(
            20L,
            20L,
            translogUUID,
            Set.of("_1.cfs", "_0.cfe", "_0.si", "_1.cfe", "_1.si", "_0.cfs", "segments_2")
        );
        combinedDeletionPolicy.onCommit(List.of(commit0, commit1, commit2));

        assertThat(acquiredCommits, contains(commit0, commit1, commit2));
        assertThat(deletedCommits, hasSize(0));
        assertThat(newCommitFiles, containsInAnyOrder(equalTo("_1.cfe"), equalTo("_1.si"), equalTo("_1.cfs"), equalTo("segments_2")));

        boolean maybeCleanUpCommits = combinedDeletionPolicy.releaseCommit(commit0);
        assertThat(maybeCleanUpCommits, equalTo(true));

        globalCheckpoint.set(20L);
        final IndexCommit commit3 = mockIndexCommit(
            30L,
            30L,
            translogUUID,
            Set.of(
                "_3.fdx",
                "_3_Lucene90_0.tip",
                "_3_Lucene90_0.dvm",
                "_3.si",
                "_3_0.tmd",
                "_3_0.tim",
                "_3_ES85BloomFilter_0.bfi",
                "_3_0.pos",
                "_3_ES85BloomFilter_0.bfm",
                "_3_0.tip",
                "_3_Lucene90_0.doc",
                "_3.nvd",
                "_3.nvm",
                "_3.fnm",
                "_3_0.doc",
                "segments_3",
                "_3.kdd",
                "_3_Lucene90_0.tmd",
                "_3.fdm",
                "_3.kdi",
                "_3_Lucene90_0.dvd",
                "_3_Lucene90_0.pos",
                "_3.kdm",
                "_3.fdt",
                "_3_Lucene90_0.tim"
            )
        );
        combinedDeletionPolicy.onCommit(List.of(commit0, commit1, commit2, commit3));

        assertThat(acquiredCommits, contains(commit1, commit2, commit3));
        assertThat(deletedCommits, contains(commit0));
        assertThat(
            newCommitFiles,
            containsInAnyOrder(
                equalTo("_3.fdx"),
                equalTo("_3_Lucene90_0.tip"),
                equalTo("_3_Lucene90_0.dvm"),
                equalTo("_3.si"),
                equalTo("_3_0.tmd"),
                equalTo("_3_0.tim"),
                equalTo("_3_ES85BloomFilter_0.bfi"),
                equalTo("_3_0.pos"),
                equalTo("_3_ES85BloomFilter_0.bfm"),
                equalTo("_3_0.tip"),
                equalTo("_3_Lucene90_0.doc"),
                equalTo("_3.nvd"),
                equalTo("_3.nvm"),
                equalTo("_3.fnm"),
                equalTo("_3_0.doc"),
                equalTo("segments_3"),
                equalTo("_3.kdd"),
                equalTo("_3_Lucene90_0.tmd"),
                equalTo("_3.fdm"),
                equalTo("_3.kdi"),
                equalTo("_3_Lucene90_0.dvd"),
                equalTo("_3_Lucene90_0.pos"),
                equalTo("_3.kdm"),
                equalTo("_3.fdt"),
                equalTo("_3_Lucene90_0.tim")
            )
        );

        maybeCleanUpCommits = combinedDeletionPolicy.releaseCommit(commit2);
        assertThat("No commits to clean up (commit #2 is the safe commit)", maybeCleanUpCommits, equalTo(false));

        globalCheckpoint.set(30L);
        final IndexCommit commit4 = mockIndexCommit(
            40L,
            40L,
            translogUUID,
            Set.of(
                "_3.fdx",
                "_3_Lucene90_0.tip",
                "_3_Lucene90_0.dvm",
                "_3.si",
                "_3_0.tmd",
                "_3_0.tim",
                "_3_ES85BloomFilter_0.bfi",
                "_3_0.pos",
                "_3_ES85BloomFilter_0.bfm",
                "_3_0.tip",
                "_3_Lucene90_0.doc",
                "_3.nvd",
                "_4.cfe",
                "_3.nvm",
                "_3.fnm",
                "_3_0.doc",
                "segments_4",
                "_3.kdd",
                "_3_Lucene90_0.tmd",
                "_3.fdm",
                "_3.kdi",
                "_4.cfs",
                "_3_Lucene90_0.dvd",
                "_3_Lucene90_0.pos",
                "_3.kdm",
                "_4.si",
                "_3.fdt",
                "_3_Lucene90_0.tim"
            )
        );
        combinedDeletionPolicy.onCommit(List.of(commit1, commit2, commit3, commit4));

        assertThat(acquiredCommits, contains(commit1, commit3, commit4));
        assertThat(deletedCommits, contains(commit0, commit2));
        assertThat(newCommitFiles, containsInAnyOrder(equalTo("_4.cfe"), equalTo("_4.si"), equalTo("_4.cfs"), equalTo("segments_4")));

        maybeCleanUpCommits = combinedDeletionPolicy.releaseCommit(commit3);
        assertThat("No commits to clean up (commit #3 is the safe commit)", maybeCleanUpCommits, equalTo(false));

        maybeCleanUpCommits = combinedDeletionPolicy.releaseCommit(commit4);
        assertThat("No commits to clean up (commit #4 is the last commit)", maybeCleanUpCommits, equalTo(false));

        maybeCleanUpCommits = combinedDeletionPolicy.releaseCommit(commit1);
        assertThat(maybeCleanUpCommits, equalTo(true));

        final boolean globalCheckpointCatchUp = randomBoolean();
        globalCheckpoint.set(globalCheckpointCatchUp ? 50L : 40L);

        final IndexCommit commit5 = mockIndexCommit(
            50L,
            50L,
            translogUUID,
            Set.of(
                "_3.fdx",
                "_3_Lucene90_0.tip",
                "_3_Lucene90_0.dvm",
                "_3.si",
                "_3_0.tmd",
                "_3_0.tim",
                "_3_ES85BloomFilter_0.bfi",
                "_3_0.pos",
                "_3_ES85BloomFilter_0.bfm",
                "_3_0.tip",
                "_5.si",
                "_3_Lucene90_0.doc",
                "segments_5",
                "_3.nvd",
                "_5.cfs",
                "_4.cfe",
                "_3.nvm",
                "_3.fnm",
                "_3_0.doc",
                "_3.kdd",
                "_3_Lucene90_0.tmd",
                "_3.fdm",
                "_3.kdi",
                "_5.cfe",
                "_4.cfs",
                "_3_Lucene90_0.dvd",
                "_3_Lucene90_0.pos",
                "_3.kdm",
                "_4.si",
                "_3.fdt",
                "_3_Lucene90_0.tim"
            )
        );
        combinedDeletionPolicy.onCommit(List.of(commit1, commit3, commit4, commit5));

        if (globalCheckpointCatchUp) {
            assertThat(acquiredCommits, contains(commit5));
            assertThat(deletedCommits, contains(commit0, commit2, commit1, commit3, commit4));
        } else {
            assertThat(acquiredCommits, contains(commit4, commit5));
            assertThat(deletedCommits, contains(commit0, commit2, commit1, commit3));
        }
        assertThat(newCommitFiles, containsInAnyOrder(equalTo("_5.cfe"), equalTo("_5.si"), equalTo("_5.cfs"), equalTo("segments_5")));

        maybeCleanUpCommits = combinedDeletionPolicy.releaseCommit(commit5);
        assertThat("No commits to clean up (commit #5 is the last commit)", maybeCleanUpCommits, equalTo(false));
    }

    private CombinedDeletionPolicy newCombinedDeletionPolicy(
        TranslogDeletionPolicy translogPolicy,
        SoftDeletesPolicy softDeletesPolicy,
        AtomicLong globalCheckpoint
    ) {
        return new CombinedDeletionPolicy(logger, translogPolicy, softDeletesPolicy, globalCheckpoint::get, null) {
            @Override
            protected int getDocCountOfCommit(IndexCommit indexCommit) throws IOException {
                if (randomBoolean()) {
                    throw new IOException("Simulated IO");
                } else {
                    return between(0, 1000);
                }
            }
        };
    }

    IndexCommit mockIndexCommit(long localCheckpoint, long maxSeqNo, UUID translogUUID) throws IOException {
        return mockIndexCommit(localCheckpoint, maxSeqNo, translogUUID, Set.of());
    }

    IndexCommit mockIndexCommit(long localCheckpoint, long maxSeqNo, UUID translogUUID, Set<String> fileNames) throws IOException {
        final Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCheckpoint));
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(Translog.TRANSLOG_UUID_KEY, translogUUID.toString());
        final IndexCommit commit = mock(IndexCommit.class);
        final Directory directory = mock(Directory.class);
        when(commit.getUserData()).thenReturn(userData);
        when(commit.getDirectory()).thenReturn(directory);
        when(commit.getFileNames()).thenReturn(fileNames);
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
