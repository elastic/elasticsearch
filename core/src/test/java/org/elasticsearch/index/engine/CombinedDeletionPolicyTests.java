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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CombinedDeletionPolicyTests extends ESTestCase {

    public void testPassThrough() throws IOException {
        final List<Long> onInitList = new ArrayList<>();
        final List<Long> onCommitList = new ArrayList<>();

        final IndexDeletionPolicy primaryPolicy = new IndexDeletionPolicy() {
            @Override
            public void onInit(List<? extends IndexCommit> commits) throws IOException {
                commits.forEach(c -> onInitList.add(c.getGeneration()));
            }

            @Override
            public void onCommit(List<? extends IndexCommit> commits) throws IOException {
                commits.forEach(c -> onCommitList.add(c.getGeneration()));
            }
        };

        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(primaryPolicy, createTranslogDeletionPolicy(),
            EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
        List<IndexCommit> commitList = new ArrayList<>();
        long count = randomIntBetween(1, 3);
        for (long i = 0; i < count; i++) {
            commitList.add(mockIndexCommitWithTranslogGen(i, randomNonNegativeLong()));
        }
        final Long[] commitGenerations = commitList.stream()
            .map(IndexCommit::getGeneration)
            .toArray(Long[]::new);

        combinedDeletionPolicy.onInit(commitList);
        assertThat(onInitList, contains(commitGenerations));
        assertThat(onCommitList, empty());
        onCommitList.clear();
        onInitList.clear();

        combinedDeletionPolicy.onCommit(commitList);
        assertThat(onInitList, empty());
        assertThat(onCommitList, contains(commitGenerations));
    }

    public void testSettingMinTranslogGen() throws IOException {
        IndexDeletionPolicy indexDeletionPolicy = mock(IndexDeletionPolicy.class);
        final TranslogDeletionPolicy translogDeletionPolicy = mock(TranslogDeletionPolicy.class);
        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(indexDeletionPolicy, translogDeletionPolicy,
            EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
        List<IndexCommit> commitList = new ArrayList<>();
        long count = randomIntBetween(10, 20);
        long lastGen = 0;
        for (int i = 0; i < count; i++) {
            lastGen += randomIntBetween(10, 20000);
            commitList.add(mockIndexCommitWithTranslogGen(i, lastGen));
        }
        combinedDeletionPolicy.onInit(commitList);
        verify(translogDeletionPolicy, times(1)).setMinTranslogGenerationForRecovery(lastGen);
        commitList.clear();
        for (int i = 0; i < count; i++) {
            lastGen += randomIntBetween(10, 20000);
            commitList.add(mockIndexCommitWithTranslogGen(i, lastGen));
        }
        combinedDeletionPolicy.onCommit(commitList);
        verify(translogDeletionPolicy, times(1)).setMinTranslogGenerationForRecovery(lastGen);
    }

    public void testIgnoreSnapshottingCommits() throws Exception {
        final AtomicBoolean deleteAll = new AtomicBoolean(false);
        IndexDeletionPolicy predicateBasedPolicy = new IndexDeletionPolicy() {
            @Override
            public void onInit(List<? extends IndexCommit> commits) throws IOException {
                onCommit(commits);
            }

            @Override
            public void onCommit(List<? extends IndexCommit> commits) throws IOException {
                if (deleteAll.get()) {
                    commits.forEach(IndexCommit::delete);
                }
            }
        };

        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(predicateBasedPolicy,
            createTranslogDeletionPolicy(), EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
        List<IndexCommit> commitList = new ArrayList<>();
        long count = randomIntBetween(10, 20);
        long lastGen = 0;
        for (int i = 0; i < count; i++) {
            lastGen += randomIntBetween(10, 20000);
            commitList.add(mockIndexCommitWithTranslogGen(i, lastGen));
        }
        deleteAll.set(false);
        SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(combinedDeletionPolicy);
        snapshotDeletionPolicy.onInit(commitList);
        // The last commit is kept by SnapshotDeletionPolicy, but the CombinedPolicy does not take into account.
        snapshotDeletionPolicy.snapshot();
        deleteAll.set(true);
        AssertionError assertionError = expectThrows(AssertionError.class, () -> combinedDeletionPolicy.onCommit(commitList));
        assertThat(assertionError.getMessage(), equalTo("last commit is deleted"));
    }

    IndexCommit mockIndexCommitWithTranslogGen(long commitGen, long translogGen) throws IOException {
        IndexCommit commit = mock(IndexCommit.class);
        when(commit.getGeneration()).thenReturn(commitGen);
        when(commit.getUserData()).thenReturn(Collections.singletonMap(Translog.TRANSLOG_GENERATION_KEY, Long.toString(translogGen)));
        return commit;
    }
}
