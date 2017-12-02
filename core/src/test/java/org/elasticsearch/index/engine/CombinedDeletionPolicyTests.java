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
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonList;
import static org.elasticsearch.index.engine.EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG;
import static org.elasticsearch.index.engine.EngineConfig.OpenMode.OPEN_INDEX_CREATE_TRANSLOG;
import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CombinedDeletionPolicyTests extends ESTestCase {

    public void testCommitsKeepUpToGlobalCheckpoint() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        TranslogDeletionPolicy translogPolicy = createTranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = new CombinedDeletionPolicy(OPEN_INDEX_AND_TRANSLOG, translogPolicy, globalCheckpoint::get);

        final LongArrayList maxSeqNoList = new LongArrayList();
        final LongArrayList translogGenList = new LongArrayList();
        final List<IndexCommit> commitList = new ArrayList<>();
        int totalCommits = between(2, 20);
        long lastMaxSeqNo = 0;
        long lastTranslogGen = 0;

        for (int i = 0; i < totalCommits; i++) {
            lastMaxSeqNo += between(1, 10000);
            lastTranslogGen += between(1, 100);
            commitList.add(mockIndexCommit(lastMaxSeqNo, lastTranslogGen));
            maxSeqNoList.add(lastMaxSeqNo);
            translogGenList.add(lastTranslogGen);
        }

        int keptIndex = randomInt(commitList.size() - 2);
        globalCheckpoint.set(randomLongBetween(
            maxSeqNoList.get(keptIndex), Math.max(maxSeqNoList.get(keptIndex), maxSeqNoList.get(keptIndex + 1) - 1))
        );
        indexPolicy.onCommit(commitList);

        for (int i = 0; i < commitList.size(); i++) {
            if (i < keptIndex) {
                verify(commitList.get(i), times(1)).delete();
            } else {
                verify(commitList.get(i), never()).delete();
            }
        }
        assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(translogGenList.get(keptIndex)));
        assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(lastTranslogGen));
    }

    public void testIgnoreSnapshottingCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        TranslogDeletionPolicy translogPolicy = createTranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = new CombinedDeletionPolicy(OPEN_INDEX_CREATE_TRANSLOG, translogPolicy, globalCheckpoint::get);

        long firstMaxSeqNo = randomLongBetween(0, Long.MAX_VALUE - 1);
        long secondMaxSeqNo = randomLongBetween(firstMaxSeqNo + 1, Long.MAX_VALUE);

        long lastTranslogGen = randomNonNegativeLong();
        final IndexCommit firstCommit = mockIndexCommit(firstMaxSeqNo, randomNonNegativeLong());
        final IndexCommit secondCommit = mockIndexCommit(secondMaxSeqNo, lastTranslogGen);
        SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(indexPolicy);

        snapshotDeletionPolicy.onInit(Arrays.asList(firstCommit));
        snapshotDeletionPolicy.snapshot();
        assertThat(snapshotDeletionPolicy.getSnapshots(), contains(firstCommit));

        // SnapshotPolicy prevents the first commit from deleting, but CombinedPolicy does not retain its translog.
        globalCheckpoint.set(randomLongBetween(secondMaxSeqNo, Long.MAX_VALUE));
        snapshotDeletionPolicy.onCommit(Arrays.asList(firstCommit, secondCommit));
        verify(firstCommit, never()).delete();
        verify(secondCommit, never()).delete();
        assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(lastTranslogGen));
        assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(lastTranslogGen));
    }

    public void testLegacyIndex() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        TranslogDeletionPolicy translogPolicy = createTranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = new CombinedDeletionPolicy(OPEN_INDEX_AND_TRANSLOG, translogPolicy, globalCheckpoint::get);

        long legacyTranslogGen = randomNonNegativeLong();
        IndexCommit legacyCommit = mockLegacyIndexCommit(legacyTranslogGen);
        indexPolicy.onInit(singletonList(legacyCommit));
        verify(legacyCommit, never()).delete();
        assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(legacyTranslogGen));
        assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(legacyTranslogGen));

        globalCheckpoint.set(randomNonNegativeLong());
        long safeTranslogGen = randomLongBetween(legacyTranslogGen, Long.MAX_VALUE);
        IndexCommit safeCommit = mockIndexCommit(randomLongBetween(0, globalCheckpoint.get()), safeTranslogGen);

        indexPolicy.onCommit(Arrays.asList(legacyCommit, safeCommit));
        verify(legacyCommit, times(1)).delete();
        verify(safeCommit, times(0)).delete();
        assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(safeTranslogGen));
        assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(safeTranslogGen));
    }

    public void testCleanupDuplicateCommits() throws Exception {
        final AtomicLong globalCheckpoint = new AtomicLong();
        TranslogDeletionPolicy translogPolicy = createTranslogDeletionPolicy();
        CombinedDeletionPolicy indexPolicy = new CombinedDeletionPolicy(OPEN_INDEX_AND_TRANSLOG, translogPolicy, globalCheckpoint::get);

        final long maxSeqNo = randomNonNegativeLong();
        final long lastTranslogGen = randomNonNegativeLong();
        IndexCommit commit1 = mockIndexCommit(maxSeqNo, randomNonNegativeLong());
        IndexCommit commit2 = mockIndexCommit(maxSeqNo, lastTranslogGen);

        indexPolicy.onInit(Arrays.asList(commit1, commit2));
        verify(commit2, never()).delete();
        verify(commit1, times(1)).delete();

        assertThat(translogPolicy.getTranslogGenerationOfLastCommit(), equalTo(lastTranslogGen));
        assertThat(translogPolicy.getMinTranslogGenerationForRecovery(), equalTo(lastTranslogGen));
    }

    IndexCommit mockIndexCommit(long maxSeqNo, long translogGen) throws IOException {
        final Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(translogGen));
        final IndexCommit commit = mock(IndexCommit.class);
        when(commit.getUserData()).thenReturn(userData);
        return commit;
    }

    IndexCommit mockLegacyIndexCommit(long translogGen) throws IOException {
        final Map<String, String> userData = new HashMap<>();
        userData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(translogGen));
        final IndexCommit commit = mock(IndexCommit.class);
        when(commit.getUserData()).thenReturn(userData);
        return commit;
    }
}
