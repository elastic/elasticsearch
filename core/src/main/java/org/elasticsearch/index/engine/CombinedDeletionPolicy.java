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
import org.elasticsearch.common.lucene.index.ESIndexDeletionPolicy;
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
    private final ESIndexDeletionPolicy indexDeletionPolicy;

    CombinedDeletionPolicy(ESIndexDeletionPolicy indexDeletionPolicy, TranslogDeletionPolicy translogDeletionPolicy,
                           EngineConfig.OpenMode openMode) {
        this.indexDeletionPolicy = indexDeletionPolicy;
        this.translogDeletionPolicy = translogDeletionPolicy;
        this.openMode = openMode;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        final List<IndexCommit> keptCommits = indexDeletionPolicy.onInit(commits);
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
        assert commits.isEmpty() == false;
        final List<IndexCommit> keptCommits = indexDeletionPolicy.onCommit(commits);
        setLastCommittedTranslogGeneration(keptCommits);
    }

    private void setLastCommittedTranslogGeneration(List<IndexCommit> keptCommits) throws IOException {
        assert keptCommits.isEmpty() == false : "All index commits were deleted";
        assert keptCommits.stream().allMatch(commit -> commit.isDeleted() == false) : "Kept commits must not be deleted";

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
}
