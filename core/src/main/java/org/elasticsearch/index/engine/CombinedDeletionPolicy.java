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

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
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
        switch (openMode) {
            case CREATE_INDEX_AND_TRANSLOG:
                assert commits.isEmpty() : "index is created, but we have commits";
                break;
            case OPEN_INDEX_CREATE_TRANSLOG:
                assert commits.isEmpty() == false : "index is opened, but we have no commits";
                // When an engine starts with OPEN_INDEX_CREATE_TRANSLOG, a new fresh index commit will be created immediately.
                // We therefore can simply skip processing here as `onCommit` will be called right after with a new commit.
                break;
            case OPEN_INDEX_AND_TRANSLOG:
                assert commits.isEmpty() == false : "index is opened, but we have no commits";
                onCommit(commits);
                break;
            default:
                throw new IllegalArgumentException("unknown openMode [" + openMode + "]");
        }
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        final int keptPosition = indexOfKeptCommits(commits);
        for (int i = 0; i < keptPosition; i++) {
            commits.get(i).delete();
        }
        updateTranslogDeletionPolicy(commits.get(keptPosition), commits.get(commits.size() - 1));
    }

    private void updateTranslogDeletionPolicy(final IndexCommit minRequiredCommit, final IndexCommit lastCommit) throws IOException {
        assert minRequiredCommit.isDeleted() == false : "The minimum required commit must not be deleted";
        final long minRequiredGen = Long.parseLong(minRequiredCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));

        assert lastCommit.isDeleted() == false : "The last commit must not be deleted";
        final long lastGen = Long.parseLong(lastCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));

        assert minRequiredGen <= lastGen : "minRequiredGen must not be greater than lastGen";
        translogDeletionPolicy.setTranslogGenerationOfLastCommit(lastGen);
        translogDeletionPolicy.setMinTranslogGenerationForRecovery(minRequiredGen);
    }

    /**
     * Find the highest index position of a safe index commit whose max sequence number is not greater than the global checkpoint.
     * Index commits with different translog UUID will be filtered out as they don't belong to this engine.
     */
    private int indexOfKeptCommits(List<? extends IndexCommit> commits) throws IOException {
        final long currentGlobalCheckpoint = globalCheckpointSupplier.getAsLong();
        final String expectedTranslogUUID = commits.get(commits.size() - 1).getUserData().get(Translog.TRANSLOG_UUID_KEY);

        // Commits are sorted by age (the 0th one is the oldest commit).
        for (int i = commits.size() - 1; i >= 0; i--) {
            final Map<String, String> commitUserData = commits.get(i).getUserData();
            // Ignore index commits with different translog uuid.
            if (expectedTranslogUUID.equals(commitUserData.get(Translog.TRANSLOG_UUID_KEY)) == false) {
                return i + 1;
            }
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
        return 0;
    }
}
