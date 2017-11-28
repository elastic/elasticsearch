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
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomLongBetween;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KeepUntilGlobalCheckpointDeletionPolicyTests extends ESTestCase {

    public void testUnassignedGlobalCheckpointKeepAllCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(SequenceNumbers.UNASSIGNED_SEQ_NO);
        final KeepUntilGlobalCheckpointDeletionPolicy deletionPolicy = new KeepUntilGlobalCheckpointDeletionPolicy(globalCheckpoint::get);
        List<IndexCommit> commitList = new ArrayList<>();
        int totalCommits = between(1, 20);
        for (int i = 0; i < totalCommits; i++) {
            commitList.add(mockIndexCommitWithMaxSeqNo(randomNonNegativeLong()));
        }
        final IndexCommit[] allCommits = commitList.toArray(new IndexCommit[commitList.size()]);
        assertThat("Unassigned global checkpoint keeps all commits", deletionPolicy.onInit(commitList), contains(allCommits));
        assertThat("Unassigned global checkpoint keeps all commits", deletionPolicy.onCommit(commitList), contains(allCommits));
        for (IndexCommit indexCommit : commitList) {
            verify(indexCommit, never()).delete();
        }
    }

    public void testKeepUpGlobalCheckpointKeepLastCommitOnly() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        final KeepUntilGlobalCheckpointDeletionPolicy deletionPolicy = new KeepUntilGlobalCheckpointDeletionPolicy(globalCheckpoint::get);
        List<IndexCommit> commitList = new ArrayList<>();
        int totalCommits = between(1, 20);
        long lastMaxSeqNo = 0;
        for (int i = 0; i < totalCommits; i++) {
            lastMaxSeqNo += between(1, 1000);
            commitList.add(mockIndexCommitWithMaxSeqNo(lastMaxSeqNo));
        }
        final IndexCommit lastCommit = commitList.get(commitList.size() - 1);
        globalCheckpoint.set(lastMaxSeqNo);
        assertThat("Keep up global checkpoint keeps only the last commit", deletionPolicy.onInit(commitList), contains(lastCommit));
        for (int i = 0; i < commitList.size() - 1; i++) {
            verify(commitList.get(i), times(1)).delete();
        }
        assertThat("Keep up global checkpoint keeps only the last commit", deletionPolicy.onCommit(commitList), contains(lastCommit));
        for (int i = 0; i < commitList.size() - 1; i++) {
            verify(commitList.get(i), times(2)).delete();
        }
        verify(lastCommit, never()).delete();
    }

    public void testLaggingGlobalCheckpoint() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        final KeepUntilGlobalCheckpointDeletionPolicy deletionPolicy = new KeepUntilGlobalCheckpointDeletionPolicy(globalCheckpoint::get);
        List<IndexCommit> commitList = new ArrayList<>();
        List<Long> maxSeqNoList = new ArrayList<>();
        int totalCommits = between(2, 20);
        long lastMaxSeqNo = 0;
        for (int i = 0; i < totalCommits; i++) {
            lastMaxSeqNo += between(1, 1000);
            commitList.add(mockIndexCommitWithMaxSeqNo(lastMaxSeqNo));
            maxSeqNoList.add(lastMaxSeqNo);
        }

        int pivot = randomInt(maxSeqNoList.size() - 2);
        long currentGCP = randomLongBetween(maxSeqNoList.get(pivot), Math.max(maxSeqNoList.get(pivot), maxSeqNoList.get(pivot + 1) - 1));
        globalCheckpoint.set(currentGCP);

        final IndexCommit[] keptCommits = commitList.subList(pivot, commitList.size()).toArray(new IndexCommit[0]);
        assertThat(deletionPolicy.onInit(commitList), contains(keptCommits));
        assertThat(deletionPolicy.onCommit(commitList), contains(keptCommits));
    }

    public void testLegacyIndex() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong(randomInt(1000));
        final KeepUntilGlobalCheckpointDeletionPolicy deletionPolicy = new KeepUntilGlobalCheckpointDeletionPolicy(globalCheckpoint::get);

        // Keep a single legacy commit
        {
            IndexCommit legacyCommit = mockLegacyIndexCommit();
            final List<IndexCommit> keptCommits = deletionPolicy.onInit(singletonList(legacyCommit));
            verify(legacyCommit, times(0)).delete();
            assertThat(keptCommits, contains(legacyCommit));
        }

        // Keep a safe commit, and delete a legacy commit.
        {
            IndexCommit legacyCommit = mockLegacyIndexCommit();
            IndexCommit safeCommit = mockIndexCommitWithMaxSeqNo(randomLongBetween(0, globalCheckpoint.get()));

            final List<IndexCommit> keptCommits = deletionPolicy.onCommit(Arrays.asList(legacyCommit, safeCommit));
            verify(legacyCommit, times(1)).delete();
            verify(safeCommit, times(0)).delete();
            assertThat(keptCommits, contains(safeCommit));
        }

        // Keep until the safe commit, and delete legacy commits
        {
            IndexCommit legacyCommit = mockLegacyIndexCommit();
            IndexCommit oldCommit = mockIndexCommitWithMaxSeqNo(randomLongBetween(0, globalCheckpoint.get()));
            IndexCommit safeCommit = mockIndexCommitWithMaxSeqNo(randomLongBetween(0, globalCheckpoint.get()));
            IndexCommit unsafeCommit = mockIndexCommitWithMaxSeqNo(globalCheckpoint.get() + between(1, 1000));

            List<IndexCommit> keptCommits = deletionPolicy.onCommit(Arrays.asList(legacyCommit, oldCommit, safeCommit, unsafeCommit));
            verify(legacyCommit, times(1)).delete();
            verify(oldCommit, times(1)).delete();
            verify(safeCommit, times(0)).delete();
            verify(unsafeCommit, times(0)).delete();
            assertThat(keptCommits, contains(safeCommit, unsafeCommit));
        }
    }

    public void testCleanupDuplicateCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        final KeepUntilGlobalCheckpointDeletionPolicy deletionPolicy = new KeepUntilGlobalCheckpointDeletionPolicy(globalCheckpoint::get);

        final long maxSeqNo = randomNonNegativeLong();
        IndexCommit commit1 = mockIndexCommitWithMaxSeqNo(maxSeqNo);
        IndexCommit commit2 = mockIndexCommitWithMaxSeqNo(maxSeqNo);

        assertThat(deletionPolicy.onInit(Arrays.asList(commit1, commit2)), contains(commit2));
        verify(commit2, never()).delete();
        verify(commit1, times(1)).delete();

        assertThat(deletionPolicy.onCommit(Arrays.asList(commit1, commit2)), contains(commit2));
        verify(commit2, never()).delete();
        verify(commit1, times(2)).delete();
    }

    IndexCommit mockIndexCommitWithMaxSeqNo(long maxSeqNo) throws IOException {
        final Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(randomNonNegativeLong()));
        final IndexCommit commit = mock(IndexCommit.class);
        when(commit.getUserData()).thenReturn(userData);
        return commit;
    }

    IndexCommit mockLegacyIndexCommit() throws IOException {
        final Map<String, String> userData = new HashMap<>();
        userData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(randomNonNegativeLong()));
        final IndexCommit commit = mock(IndexCommit.class);
        when(commit.getUserData()).thenReturn(userData);
        return commit;
    }

}
