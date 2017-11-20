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
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CombinedDeletionPolicyTests extends ESTestCase {

    public void testPassThrough() throws IOException {
        SnapshotDeletionPolicy indexDeletionPolicy = mock(SnapshotDeletionPolicy.class);
        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(indexDeletionPolicy, createTranslogDeletionPolicy(),
            EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
        List<IndexCommit> commitList = new ArrayList<>();
        long count = randomIntBetween(1, 3);
        for (int i = 0; i < count; i++) {
            commitList.add(mockIndexCommitWithTranslogGen(randomNonNegativeLong()));
        }
        combinedDeletionPolicy.onInit(commitList);
        verify(indexDeletionPolicy, times(1)).onInit(commitList);
        combinedDeletionPolicy.onCommit(commitList);
        verify(indexDeletionPolicy, times(1)).onCommit(commitList);
    }

    public void testSettingMinTranslogGen() throws IOException {
        SnapshotDeletionPolicy indexDeletionPolicy = mock(SnapshotDeletionPolicy.class);
        final TranslogDeletionPolicy translogDeletionPolicy = mock(TranslogDeletionPolicy.class);
        CombinedDeletionPolicy combinedDeletionPolicy = new CombinedDeletionPolicy(indexDeletionPolicy, translogDeletionPolicy,
            EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG);
        List<IndexCommit> commitList = new ArrayList<>();
        long count = randomIntBetween(10, 20);
        long lastGen = 0;
        for (int i = 0; i < count; i++) {
            lastGen += randomIntBetween(10, 20000);
            commitList.add(mockIndexCommitWithTranslogGen(lastGen));
        }
        combinedDeletionPolicy.onInit(commitList);
        verify(translogDeletionPolicy, times(1)).setMinTranslogGenerationForRecovery(lastGen);
        commitList.clear();
        for (int i = 0; i < count; i++) {
            lastGen += randomIntBetween(10, 20000);
            commitList.add(mockIndexCommitWithTranslogGen(lastGen));
        }
        combinedDeletionPolicy.onCommit(commitList);
        verify(translogDeletionPolicy, times(1)).setMinTranslogGenerationForRecovery(lastGen);
    }

    IndexCommit mockIndexCommitWithTranslogGen(long gen) throws IOException {
        IndexCommit commit = mock(IndexCommit.class);
        when(commit.getUserData()).thenReturn(Collections.singletonMap(Translog.TRANSLOG_GENERATION_KEY, Long.toString(gen)));
        return commit;
    }
}
