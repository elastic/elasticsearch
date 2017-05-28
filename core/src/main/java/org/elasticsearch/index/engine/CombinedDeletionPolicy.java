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

public class CombinedDeletionPolicy extends IndexDeletionPolicy {

    private final TranslogDeletionPolicy translogDeletionPolicy;

    private final SnapshotDeletionPolicy indexDeletionPolicy;

    CombinedDeletionPolicy(SnapshotDeletionPolicy indexDeletionPolicy, TranslogDeletionPolicy translogDeletionPolicy) {
        this.indexDeletionPolicy = indexDeletionPolicy;
        this.translogDeletionPolicy = translogDeletionPolicy;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        indexDeletionPolicy.onInit(commits);
        setLastCommittedTranslogGeneration(commits);
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        indexDeletionPolicy.onInit(commits);
        setLastCommittedTranslogGeneration(commits);
    }

    private void setLastCommittedTranslogGeneration(List<? extends IndexCommit> commits) throws IOException {
        final long minGen;
        if (commits.isEmpty()) {
            minGen = 1; // new index, keep the first generation onwards
        } else {
            final IndexCommit indexCommit = commits.get(commits.size() - 1);
            assert indexCommit.isDeleted() == false : "last commit is deleted";
            minGen = Long.parseLong(indexCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        }
        translogDeletionPolicy.setMinTranslogGenerationForRecovery(minGen);
    }

    public SnapshotDeletionPolicy getIndexDeletionPolicy() {
        return indexDeletionPolicy;
    }
}
