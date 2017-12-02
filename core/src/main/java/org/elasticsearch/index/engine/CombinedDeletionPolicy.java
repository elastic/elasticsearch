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

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * An {@link IndexDeletionPolicy} that coordinates between Lucene's commits and the retention of translog generation files,
 * making sure that all translog files that are needed to recover from the Lucene commit are not deleted.
 * <p>
 * In particular, this policy will delete index commits whose max sequence number is at most
 * the current global checkpoint except the index commit which has the highest max sequence number among those.
 */
final class CombinedDeletionPolicy extends IndexDeletionPolicy {
    private final TranslogDeletionPolicy translogDeletionPolicy;
    private final EngineConfig.OpenMode openMode;
    private final LongSupplier globalCheckpointSupplier;

    CombinedDeletionPolicy(EngineConfig.OpenMode openMode, TranslogDeletionPolicy translogDeletionPolicy,
                           LongSupplier globalCheckpointSupplier) {
        this.openMode = openMode;
        this.translogDeletionPolicy = translogDeletionPolicy;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        final List<IndexCommit> keptCommits = deleteOldIndexCommits(commits);
        switch (openMode) {
            case CREATE_INDEX_AND_TRANSLOG:
                break;
            case OPEN_INDEX_CREATE_TRANSLOG:
                assert commits.isEmpty() == false : "index is opened, but we have no commits";
                break;
            case OPEN_INDEX_AND_TRANSLOG:
                assert commits.isEmpty() == false : "index is opened, but we have no commits";
                setLastCommittedTranslogGeneration(keptCommits);
                break;
            default:
                throw new IllegalArgumentException("unknown openMode [" + openMode + "]");
        }
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        final List<IndexCommit> keptCommits = deleteOldIndexCommits(commits);
        setLastCommittedTranslogGeneration(keptCommits);
    }

    private void setLastCommittedTranslogGeneration(List<IndexCommit> keptCommits) throws IOException {
        assert keptCommits.isEmpty() == false : "All index commits were deleted";
        assert keptCommits.stream().allMatch(c -> c.isDeleted() == false) : "All kept commits must not be deleted";

        final IndexCommit lastCommit = keptCommits.get(keptCommits.size() - 1);
        final long lastGen = Long.parseLong(lastCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        long minRequiredGen = lastGen;
        for (IndexCommit indexCommit : keptCommits) {
            long translogGen = Long.parseLong(indexCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
            minRequiredGen = Math.min(minRequiredGen, translogGen);
        }
        assert minRequiredGen <= lastGen;
        translogDeletionPolicy.setTranslogGenerationOfLastCommit(lastGen);
        translogDeletionPolicy.setMinTranslogGenerationForRecovery(minRequiredGen);
    }

    /**
     * Deletes old index commits which are not required for operation based recovery.
     */
    private List<IndexCommit> deleteOldIndexCommits(List<? extends IndexCommit> commits) throws IOException {
        if (commits.isEmpty()) {
            return Collections.emptyList();
        }

        final List<IndexCommit> keptCommits = new ArrayList<>();
        final int keptPosition = indexOfKeptCommits(commits);
        final IntSet duplicateIndexes = indexesOfDuplicateCommits(commits);

        for (int i = 0; i < commits.size() - 1; i++) {
            final IndexCommit commit = commits.get(i);
            if (i < keptPosition || duplicateIndexes.contains(i)) {
                commit.delete();
            } else {
                keptCommits.add(commit);
            }
        }
        keptCommits.add(commits.get(commits.size() - 1)); // Always keep the last commit.

        return keptCommits;
    }

    /**
     * Find the index position of a safe index commit whose max sequence number is not greater than the global checkpoint.
     */
    private int indexOfKeptCommits(List<? extends IndexCommit> commits) throws IOException {
        final long currentGlobalCheckpoint = globalCheckpointSupplier.getAsLong();

        // Commits are sorted by age (the 0th one is the oldest commit).
        for (int i = commits.size() - 1; i >= 0; i--) {
            final Map<String, String> commitUserData = commits.get(i).getUserData();
            // 5.x commits do not contain MAX_SEQ_NO.
            if (commitUserData.containsKey(SequenceNumbers.MAX_SEQ_NO) == false) {
                return i;
            }
            final long maxSeqNoFromCommit = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO));
            if (maxSeqNoFromCommit <= currentGlobalCheckpoint) {
                return i;
            }
        }
        /*
         * We may reach to this point in these cases:
         * 1. In the previous 6.x, we keep only the last commit - which is likely not a safe commit if writes are in progress.
         * Thus, after upgrading, we may not find a safe commit until we can reserve one.
         * 2. In peer-recovery, if the file-based happens, a replica will be received the latest commit from a primary.
         * However, that commit may not be a safe commit if writes are in progress in the primary.
         */
        return -1;
    }

    /**
     * In some cases, we may have more than one index commits with the same max sequence number.
     * We better scan and delete these duplicate index commits as soon as possible.
     *
     * @return index positions of duplicate commits.
     */
    private IntSet indexesOfDuplicateCommits(List<? extends IndexCommit> commits) throws IOException {
        final LongSet seenMaxSeqNo = new LongHashSet();
        final IntSet duplicateIndexes = new IntHashSet();

        for (int i = commits.size() - 1; i >= 0; i--) {
            final Map<String, String> commitUserData = commits.get(i).getUserData();
            // 5.x commits do not contain MAX_SEQ_NO.
            if (commitUserData.containsKey(SequenceNumbers.MAX_SEQ_NO)) {
                final long maxSeqNoFromCommit = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO));
                if (seenMaxSeqNo.add(maxSeqNoFromCommit) == false) {
                    duplicateIndexes.add(i);
                }
            }
        }
        return duplicateIndexes;
    }
}
