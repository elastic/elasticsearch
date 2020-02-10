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

import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BlobStoreIncrementalityIT extends AbstractSnapshotIntegTestCase {

    public void testIncrementalBehaviorOnPrimaryFailover() throws InterruptedException, ExecutionException, IOException {
        internalCluster().startMasterOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        final String indexName = "test-index";
        createIndex(indexName, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(indexName);
        internalCluster().startDataOnlyNode();
        ensureGreen(indexName);
        for (int j = 0; j < randomIntBetween(1, 10); ++j) {
            final BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < scaledRandomIntBetween(1, 100); ++i) {
                bulkRequest.add(new IndexRequest(indexName).source("foo" + j, "bar" + i));
            }
            client().bulk(bulkRequest).get();
        }
        final String snapshot1 = "snap-1";
        final String snapshot2 = "snap-2";
        final String repo = "test-repo";
        assertThat(client().admin().cluster().preparePutRepository(repo)
                .setType("fs").setSettings(Settings.builder().put("location", randomRepoPath())).execute().actionGet().isAcknowledged(),
            is(true));

        client().admin().cluster().prepareCreateSnapshot(repo, snapshot1).setIndices(indexName).setWaitForCompletion(true).get();
        stopNode(primaryNode);
        ensureYellow(indexName);
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot2).setIndices(indexName).setWaitForCompletion(true).get();
        final SnapshotsStatusResponse response =
            client().admin().cluster().prepareSnapshotStatus(repo).setSnapshots(snapshot1, snapshot2).get();
        assertThat(response.getSnapshots().get(1).getIndices().get(indexName)
            .getShards().get(0).getStats().getIncrementalFileCount(), is(0));
    }
}
