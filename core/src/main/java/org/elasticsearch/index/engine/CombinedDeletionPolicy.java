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
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.util.List;

/**
 * An {@link IndexDeletionPolicy} that coordinates between Lucene's commits and the retention of translog generation files,
 * making sure that all translog files that are needed to recover from the Lucene commit are not deleted.
 */
class CombinedDeletionPolicy extends IndexDeletionPolicy {

    private final TranslogDeletionPolicy translogDeletionPolicy;
    private final EngineConfig.OpenMode openMode;

    private final SnapshotDeletionPolicy indexDeletionPolicy;

    CombinedDeletionPolicy(SnapshotDeletionPolicy indexDeletionPolicy, TranslogDeletionPolicy translogDeletionPolicy,
                           EngineConfig.OpenMode openMode) {
        this.indexDeletionPolicy = indexDeletionPolicy;
        this.translogDeletionPolicy = translogDeletionPolicy;
        this.openMode = openMode;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        indexDeletionPolicy.onInit(commits);
        switch (openMode) {
            case CREATE_INDEX_AND_TRANSLOG:
                break;
            case OPEN_INDEX_CREATE_TRANSLOG:
                assert commits.isEmpty() == false : "index is opened, but we have no commits";
                break;
            case OPEN_INDEX_AND_TRANSLOG:
                assert commits.isEmpty() == false : "index is opened, but we have no commits";
                setLastCommittedTranslogGeneration(commits);
                break;
            default:
                throw new IllegalArgumentException("unknown openMode [" + openMode + "]");
        }
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        indexDeletionPolicy.onCommit(commits);
        setLastCommittedTranslogGeneration(commits);
    }

    private void setLastCommittedTranslogGeneration(List<? extends IndexCommit> commits) throws IOException {
        // We need to keep translog since the smallest translog generation of un-deleted commits.
        // However, there are commits that are not deleted just because they are being snapshotted (rather than being kept by the policy).
        // TODO: We need to distinguish those commits and skip them in calculating the minimum required translog generation.
        long minRequiredGen = Long.MAX_VALUE;
        for (IndexCommit indexCommit : commits) {
            if (indexCommit.isDeleted() == false) {
                long translogGen = Long.parseLong(indexCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
                minRequiredGen = Math.min(translogGen, minRequiredGen);
            }
        }
        assert minRequiredGen != Long.MAX_VALUE : "All commits are deleted";
        translogDeletionPolicy.setMinTranslogGenerationForRecovery(minRequiredGen);
    }

    public SnapshotDeletionPolicy getIndexDeletionPolicy() {
        return indexDeletionPolicy;
    }

    public TranslogDeletionPolicy getTranslogDeletionPolicy() {
        return translogDeletionPolicy;
    }
}
