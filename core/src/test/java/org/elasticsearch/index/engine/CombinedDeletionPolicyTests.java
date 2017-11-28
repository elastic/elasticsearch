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
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.elasticsearch.common.lucene.index.ESIndexDeletionPolicy;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CombinedDeletionPolicyTests extends ESTestCase {

    public void testPassThrough() throws IOException {
        ESIndexDeletionPolicy indexDeletionPolicy = mock(ESIndexDeletionPolicy.class);
        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(indexDeletionPolicy, createTranslogDeletionPolicy(),
            EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
        List<IndexCommit> commitList = new ArrayList<>();
        long count = randomIntBetween(1, 3);
        for (int i = 0; i < count; i++) {
            commitList.add(mockIndexCommitWithTranslogGen(randomNonNegativeLong()));
        }
        when(indexDeletionPolicy.onInit(anyListOf(IndexCommit.class))).thenReturn(commitList);
        when(indexDeletionPolicy.onCommit(anyListOf(IndexCommit.class))).thenReturn(commitList);
        combinedDeletionPolicy.onInit(commitList);
        verify(indexDeletionPolicy, times(1)).onInit(commitList);
        combinedDeletionPolicy.onCommit(commitList);
        verify(indexDeletionPolicy, times(1)).onCommit(commitList);
    }

    public void testSettingTranslogGenerations() throws IOException {
        final int count = randomIntBetween(10, 20);
        final List<IndexCommit> commitList = new ArrayList<>();
        final LongArrayList translogGens = new LongArrayList();
        long lastGen = 0;
        for (int i = 0; i < count; i++) {
            lastGen += randomIntBetween(10, 20000);
            commitList.add(mockIndexCommitWithTranslogGen(lastGen));
            translogGens.add(lastGen);
        }

        final AtomicInteger keepN = new AtomicInteger(between(1, commitList.size()));
        final ESIndexDeletionPolicy keepLastNPolicy = new ESIndexDeletionPolicy() {
            @Override
            public List<IndexCommit> onInit(List<? extends IndexCommit> commits) throws IOException {
                return onCommit(commits);
            }

            @Override
            public List<IndexCommit> onCommit(List<? extends IndexCommit> commits) throws IOException {
                return new ArrayList<>(commits.subList(commits.size() - keepN.get(), commits.size()));
            }
        };
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(randomLong(), randomLong());
        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(keepLastNPolicy, translogDeletionPolicy,
            EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);

        combinedDeletionPolicy.onInit(commitList);
        assertThat(translogDeletionPolicy.getTranslogGenerationOfLastCommit(),
            equalTo(lastGen));
        assertThat(translogDeletionPolicy.getMinTranslogGenerationForRecovery(),
            equalTo(translogGens.get(translogGens.size() - keepN.get())));

        int moreCommits = randomIntBetween(0, 10);
        for (int i = 0; i < moreCommits; i++) {
            lastGen += randomIntBetween(10, 20000);
            commitList.add(mockIndexCommitWithTranslogGen(lastGen));
            translogGens.add(lastGen);
        }
        keepN.addAndGet(randomInt(moreCommits));

        combinedDeletionPolicy.onCommit(commitList);
        assertThat(translogDeletionPolicy.getTranslogGenerationOfLastCommit(),
            equalTo(lastGen));
        assertThat(translogDeletionPolicy.getMinTranslogGenerationForRecovery(),
            equalTo(translogGens.get(translogGens.size() - keepN.get())));
    }

    public void testIgnoreSnapshottingCommits() throws Exception {
        final ESIndexDeletionPolicy keepOnlyLastPolicy = new ESIndexDeletionPolicy() {
            @Override
            public List<IndexCommit> onInit(List<? extends IndexCommit> commits) throws IOException {
                return onCommit(commits);
            }
            @Override
            public List<IndexCommit> onCommit(List<? extends IndexCommit> commits) throws IOException {
                for (int i = 0; i < commits.size() - 1; i++) {
                    commits.get(i).delete();
                }
                return Arrays.asList(commits.get(commits.size() - 1));
            }
        };
        final TranslogDeletionPolicy translogDeletionPolicy = new TranslogDeletionPolicy(randomLong(), randomLong());
        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(keepOnlyLastPolicy, translogDeletionPolicy,
            EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
        SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(combinedDeletionPolicy);

        final int count = randomIntBetween(10, 20);
        final List<IndexCommit> commitList = new ArrayList<>();
        long lastGen = 0;
        for (int i = 0; i < count; i++) {
            lastGen += randomIntBetween(10, 20000);
            commitList.add(mockIndexCommitWithTranslogGen(lastGen));
        }
        snapshotDeletionPolicy.onInit(commitList);
        assertThat(translogDeletionPolicy.getTranslogGenerationOfLastCommit(), equalTo(lastGen));
        assertThat(translogDeletionPolicy.getMinTranslogGenerationForRecovery(), equalTo(lastGen));

        snapshotDeletionPolicy.snapshot();
        long newGen = lastGen + randomIntBetween(10, 20000);
        commitList.add(mockIndexCommitWithTranslogGen(newGen));
        snapshotDeletionPolicy.onCommit(commitList);
        assertThat(translogDeletionPolicy.getTranslogGenerationOfLastCommit(), equalTo(newGen));
        // The previous commit is being snapshotted but ignored by the CombinedPolicy.
        assertThat(translogDeletionPolicy.getMinTranslogGenerationForRecovery(), equalTo(newGen));
    }

    IndexCommit mockIndexCommitWithTranslogGen(long gen) throws IOException {
        IndexCommit commit = mock(IndexCommit.class);
        when(commit.getUserData()).thenReturn(Collections.singletonMap(Translog.TRANSLOG_GENERATION_KEY, Long.toString(gen)));
        return commit;
    }
}
