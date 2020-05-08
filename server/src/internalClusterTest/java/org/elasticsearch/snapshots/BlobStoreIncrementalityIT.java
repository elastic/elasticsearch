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

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BlobStoreIncrementalityIT extends AbstractSnapshotIntegTestCase {

    public void testIncrementalBehaviorOnPrimaryFailover() throws InterruptedException, ExecutionException, IOException {
        internalCluster().startMasterOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        final String indexName = "test-index";
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0).build());
        ensureYellow(indexName);
        final String newPrimary = internalCluster().startDataOnlyNode();
        final Collection<String> toDelete = new ArrayList<>();
        ensureGreen(indexName);

        logger.info("--> adding some documents to test index");
        for (int j = 0; j < randomIntBetween(1, 10); ++j) {
            final BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < scaledRandomIntBetween(1, 100); ++i) {
                bulkRequest.add(new IndexRequest(indexName).source("foo" + j, "bar" + i));
            }
            final BulkResponse bulkResponse = client().bulk(bulkRequest).get();
            for (BulkItemResponse item : bulkResponse.getItems()) {
                if (randomBoolean()) {
                    toDelete.add(item.getId());
                }
            }
        }
        refresh(indexName);

        final long documentCountOriginal = getCountForIndex(indexName);

        final String snapshot1 = "snap-1";
        final String repo = "test-repo";
        createRepository(repo, "fs", randomRepoPath());

        logger.info("--> creating snapshot 1");
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot1).setIndices(indexName).setWaitForCompletion(true).get();

        logger.info("--> Shutting down initial primary node [{}]", primaryNode);
        stopNode(primaryNode);

        ensureYellow(indexName);
        final String snapshot2 = "snap-2";
        logger.info("--> creating snapshot 2");
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot2).setIndices(indexName).setWaitForCompletion(true).get();

        assertTwoIdenticalShardSnapshots(repo, indexName, snapshot1, snapshot2);

        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot1, "-copy-1");
        assertCountInIndexThenDelete(indexName + "-copy-1", documentCountOriginal);

        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot2, "-copy-2");
        assertCountInIndexThenDelete(indexName + "-copy-2", documentCountOriginal);

        internalCluster().startDataOnlyNode();
        ensureGreen(indexName);

        logger.info("--> delete some documents from test index");
        for (final String id : toDelete) {
            assertThat(client().prepareDelete(indexName, id).get().getResult(), is(DocWriteResponse.Result.DELETED));
        }

        refresh(indexName);

        final String snapshot3 = "snap-3";
        logger.info("--> creating snapshot 3");
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot3).setIndices(indexName).setWaitForCompletion(true).get();

        logger.info("--> Shutting down new primary node [{}]", newPrimary);
        stopNode(newPrimary);
        ensureYellow(indexName);

        final String snapshot4 = "snap-4";
        logger.info("--> creating snapshot 4");
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot4).setIndices(indexName).setWaitForCompletion(true).get();

        assertTwoIdenticalShardSnapshots(repo, indexName, snapshot3, snapshot4);

        final long countAfterDelete = documentCountOriginal - toDelete.size();
        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot3, "-copy-3");
        assertCountInIndexThenDelete(indexName + "-copy-3", countAfterDelete);

        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot4, "-copy-4");
        assertCountInIndexThenDelete(indexName + "-copy-4", countAfterDelete);
    }

    public void testForceMergeCausesFullSnapshot() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String indexName = "test-index";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        ensureGreen(indexName);

        logger.info("--> adding some documents to test index and flush in between to get at least two segments");
        for (int j = 0; j < 2; j++) {
            final BulkRequest bulkRequest = new BulkRequest();
            for (int i = 0; i < scaledRandomIntBetween(1, 100); ++i) {
                bulkRequest.add(new IndexRequest(indexName).source("foo" + j, "bar" + i));
            }
            client().bulk(bulkRequest).get();
            flushAndRefresh(indexName);
        }
        final IndexStats indexStats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName);
        assertThat(indexStats.getIndexShards().get(0).getPrimary().getSegments().getCount(), greaterThan(1L));

        final String snapshot1 = "snap-1";
        final String repo = "test-repo";
        createRepository(repo, "fs", randomRepoPath());

        logger.info("--> creating snapshot 1");
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot1).setIndices(indexName).setWaitForCompletion(true).get();

        logger.info("--> force merging down to a single segment");
        final ForceMergeResponse forceMergeResponse =
            client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(true).get();
        assertThat(forceMergeResponse.getFailedShards(), is(0));

        final String snapshot2 = "snap-2";
        logger.info("--> creating snapshot 2");
        client().admin().cluster().prepareCreateSnapshot(repo, snapshot2).setIndices(indexName).setWaitForCompletion(true).get();

        logger.info("--> asserting that the two snapshots refer to different files in the repository");
        final SnapshotStats secondSnapshotShardStatus =
            getStats(repo, snapshot2).getIndices().get(indexName).getShards().get(0).getStats();
        assertThat(secondSnapshotShardStatus.getIncrementalFileCount(), greaterThan(0));
    }

    private void assertCountInIndexThenDelete(String index, long expectedCount) throws ExecutionException, InterruptedException {
        logger.info("--> asserting that index [{}] contains [{}] documents", index, expectedCount);
        assertThat(getCountForIndex(index), is(expectedCount));
        logger.info("--> deleting index [{}]", index);
        assertThat(client().admin().indices().prepareDelete(index).get().isAcknowledged(), is(true));
    }

    private void assertTwoIdenticalShardSnapshots(String repo, String indexName, String snapshot1, String snapshot2) {
        logger.info(
            "--> asserting that snapshots [{}] and [{}] are referring to the same files in the repository", snapshot1, snapshot2);
        final SnapshotStats firstSnapshotShardStatus = getStats(repo, snapshot1).getIndices().get(indexName).getShards().get(0).getStats();
        final int totalFilesInShard = firstSnapshotShardStatus.getTotalFileCount();
        assertThat(totalFilesInShard, greaterThan(0));
        final SnapshotStats secondSnapshotShardStatus =
            getStats(repo, snapshot2).getIndices().get(indexName).getShards().get(0).getStats();
        assertThat(secondSnapshotShardStatus.getTotalFileCount(), is(totalFilesInShard));
        assertThat(secondSnapshotShardStatus.getIncrementalFileCount(), is(0));
    }

    private SnapshotStatus getStats(String repository, String snapshot) {
        return client().admin().cluster().prepareSnapshotStatus(repository).setSnapshots(snapshot).get().getSnapshots().get(0);
    }

    private void ensureRestoreSingleShardSuccessfully(String repo, String indexName, String snapshot, String indexSuffix) {
        logger.info("--> restoring [{}]", snapshot);
        final RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot(repo, snapshot)
            .setIndices(indexName).setRenamePattern("(.+)").setRenameReplacement("$1" + indexSuffix).setWaitForCompletion(true).get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.totalShards(), is(1));
        assertThat(restoreInfo.failedShards(), is(0));
    }

    private long getCountForIndex(String indexName) throws ExecutionException, InterruptedException {
        return client().search(new SearchRequest(new SearchRequest(indexName).source(
            new SearchSourceBuilder().size(0).trackTotalHits(true)))).get().getHits().getTotalHits().value;
    }
}
