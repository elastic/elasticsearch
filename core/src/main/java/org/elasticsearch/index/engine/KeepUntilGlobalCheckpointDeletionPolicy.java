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

import java.io.IOException;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * An {@link IndexDeletionPolicy} tries to keep the oldest commit whose max sequence number is not
 * greater than the current global checkpoint, and also keeps all subsequent commits. Once those
 * commits are kept, a {@link CombinedDeletionPolicy} will retain translog operations at least up to
 * the current global checkpoint.
 * <p>
 * This policy also tries to keep no more than {@link #maxNumberOfKeptCommits} index commits.
 */
public final class KeepUntilGlobalCheckpointDeletionPolicy extends IndexDeletionPolicy {
    private int maxNumberOfKeptCommits;
    private final LongSupplier globalCheckpointSupplier;

    public KeepUntilGlobalCheckpointDeletionPolicy(int maxNumberOfKeptCommits, LongSupplier globalCheckpointSupplier) {
        assert maxNumberOfKeptCommits >= 1;
        this.maxNumberOfKeptCommits = maxNumberOfKeptCommits;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
    }

    @Override
    public synchronized void onInit(List<? extends IndexCommit> commits) throws IOException {
        if (commits.isEmpty() == false) {
            onCommit(commits);
        }
    }

    @Override
    public synchronized void onCommit(List<? extends IndexCommit> commits) throws IOException {
        final int keptIndex = Math.max(indexOfKeptCommits(commits), commits.size() - maxNumberOfKeptCommits);
        for (int i = 0; i < keptIndex; i++) {
            commits.get(i).delete();
        }
        assert commits.get(commits.size() - 1).isDeleted() == false : "Last commit should not be deleted";
    }

    private int indexOfKeptCommits(List<? extends IndexCommit> commits) throws IOException {
        final long currentGlobalCheckpoint = globalCheckpointSupplier.getAsLong();
        // Commits are sorted by age (the 0th one is the oldest commit).
        for (int i = commits.size() - 1; i >= 0; i--) {
            final IndexCommit commit = commits.get(i);
            long maxSeqNoFromCommit = Long.parseLong(commit.getUserData().get(SequenceNumbers.MAX_SEQ_NO));
            if (maxSeqNoFromCommit <= currentGlobalCheckpoint) {
                return i;
            }
        }
        return -1;
    }

    public synchronized void setMaxNumberOfKeptCommits(int maxNumberOfKeptCommits) {
        assert maxNumberOfKeptCommits >= 1;
        this.maxNumberOfKeptCommits = maxNumberOfKeptCommits;
    }
}
