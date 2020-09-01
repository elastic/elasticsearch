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
package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CloneSnapshotIT extends AbstractSnapshotIntegTestCase {

    public void testCloneSnapshotIndex() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "fs");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareDelete(indexName));
        }
        final String targetSnapshot = "target-snapshot";
        assertAcked(client().admin().cluster().prepareCloneSnapshot(repoName, sourceSnapshot, targetSnapshot).setIndices(indexName).get());

        final List<SnapshotStatus> status = client().admin().cluster().prepareSnapshotStatus(repoName)
                .setSnapshots(sourceSnapshot, targetSnapshot).get().getSnapshots();
        assertThat(status, hasSize(2));
        final SnapshotIndexStatus status1 = status.get(0).getIndices().get(indexName);
        final SnapshotIndexStatus status2 = status.get(1).getIndices().get(indexName);
        assertEquals(status1.getStats().getTotalFileCount(), status2.getStats().getTotalFileCount());
        assertEquals(status1.getStats().getTotalSize(), status2.getStats().getTotalSize());
    }

    @AwaitsFix(bugUrl = "TODO if we want it")
    public void testCloneSnapshotWithIndexSettingUpdates() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "repo-name";
        createRepository(repoName, "fs");

        final String indexName = "index-1";
        createIndexWithRandomDocs(indexName, randomIntBetween(5, 10));
        final String sourceSnapshot = "source-snapshot";
        createFullSnapshot(repoName, sourceSnapshot);

        indexRandomDocs(indexName, randomIntBetween(20, 100));

        final String targetSnapshot = "target-snapshot";
        assertAcked(client().admin().cluster().prepareCloneSnapshot(repoName, sourceSnapshot, targetSnapshot).setIndices(indexName)
                .setIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()).get());

        final RestoreInfo restoreInfo = client().admin().cluster()
                .prepareRestoreSnapshot(repoName, targetSnapshot).setIndices(indexName).setRenamePattern("(.+)")
                .setRenameReplacement("$1-copy").setWaitForCompletion(true).get().getRestoreInfo();
        assertEquals(restoreInfo.successfulShards(), restoreInfo.totalShards());

        final String restoredIndex = indexName + "-copy";
        final Settings settings =
                client().admin().indices().prepareGetIndex().setIndices(restoredIndex).get().getSettings().get(restoredIndex);
        assertEquals(settings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS), "1");
    }
}
