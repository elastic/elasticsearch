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
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * An {@link IndexDeletionPolicy} keeps the oldest commit whose max sequence number is not
 * greater than the current global checkpoint, and also keeps all subsequent commits. Once those
 * commits are kept, a {@link CombinedDeletionPolicy} will retain translog operations at least up to
 * the current global checkpoint.
 */
public final class KeepUntilGlobalCheckpointDeletionPolicy extends IndexDeletionPolicy {
    private final LongSupplier globalCheckpointSupplier;

    public KeepUntilGlobalCheckpointDeletionPolicy(LongSupplier globalCheckpointSupplier) {
        this.globalCheckpointSupplier = globalCheckpointSupplier;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        if (commits.isEmpty() == false) {
            onCommit(commits);
        }
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        assert commits.isEmpty() == false;
        final int keptIndex = indexOfKeptCommits(commits);
        for (int i = 0; i < keptIndex; i++) {
            commits.get(i).delete();
        }
        assert commits.get(commits.size() - 1).isDeleted() == false : "The last commit must not be deleted";
    }

    private int indexOfKeptCommits(List<? extends IndexCommit> commits) throws IOException {
        final long currentGlobalCheckpoint = globalCheckpointSupplier.getAsLong();
        // Commits are sorted by age (the 0th one is the oldest commit).
        for (int i = commits.size() - 1; i >= 0; i--) {
            final Map<String, String> commitUserData = commits.get(i).getUserData();
            // Index from 5.x does not contain MAX_SEQ_NO.
            if (commitUserData.containsKey(SequenceNumbers.MAX_SEQ_NO) == false) {
                return i;
            }
            final long maxSeqNoFromCommit = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO));
            if (maxSeqNoFromCommit <= currentGlobalCheckpoint) {
                return i;
            }
        }
        return -1;
    }

}
