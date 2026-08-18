/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BlobStoreIncrementalityIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), InternalSettingsPlugin.class);
    }

    public void testIncrementalBehaviorOnPrimaryFailover() throws InterruptedException, ExecutionException, IOException {
        internalCluster().startMasterOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        final String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 1).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0).build());
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
        createRepository(repo, "fs");

        createSnapshot(repo, snapshot1, Collections.singletonList(indexName));

        logger.info("--> Shutting down initial primary node [{}]", primaryNode);
        stopNode(primaryNode);

        ensureYellow(indexName);
        final String snapshot2 = "snap-2";
        logger.info("--> creating snapshot 2");
        createSnapshot(repo, snapshot2, Collections.singletonList(indexName));

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
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repo, snapshot3).setIndices(indexName).setWaitForCompletion(true).get();

        logger.info("--> Shutting down new primary node [{}]", newPrimary);
        stopNode(newPrimary);
        ensureYellow(indexName);

        final String snapshot4 = "snap-4";
        logger.info("--> creating snapshot 4");
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repo, snapshot4).setIndices(indexName).setWaitForCompletion(true).get();

        assertTwoIdenticalShardSnapshots(repo, indexName, snapshot3, snapshot4);

        final long countAfterDelete = documentCountOriginal - toDelete.size();
        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot3, "-copy-3");
        assertCountInIndexThenDelete(indexName + "-copy-3", countAfterDelete);

        ensureRestoreSingleShardSuccessfully(repo, indexName, snapshot4, "-copy-4");
        assertCountInIndexThenDelete(indexName + "-copy-4", countAfterDelete);
    }

    public void testForceMergeCausesFullSnapshot() throws Exception {
        // Covers both merge flavors: a merge down to a single segment and an expunge-deletes
        // merge must both leave a fresh force merge UUID in the commit so that the snapshot
        // after the merge uploads the rewritten files (https://github.com/elastic/elasticsearch/issues/102395)
        final boolean onlyExpungeDeletes = randomBoolean();
        logger.info("--> testing force merge with only_expunge_deletes: [{}]", onlyExpungeDeletes);

        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String indexName = "test-index";
        createIndex(
            indexName,
            indexSettings(1, 0)
                // disable automatic refresh so the docs indexed below land in exactly two segments
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                // retention leases protect recent deletes from being merged away; sync them
                // frequently so the merges below can reclaim the deletes promptly
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                .build()
        );
        ensureGreen(indexName);

        logger.info("--> indexing docs and flushing in between to get two segments");
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 15; i++) {
                indexDoc(indexName, j + "-" + i, "field", "value-" + i);
            }
            flushAndRefresh(indexName);
        }
        final IndexStats indexStats = indicesAdmin().prepareStats(indexName).get().getIndex(indexName);
        assertThat(indexStats.getIndexShards().get(0).getPrimary().getSegments().getCount(), greaterThan(1L));

        // Delete 2 of the 15 docs in the first segment (above index.merge.policy.expunge_deletes_allowed's
        // default of 10%) so an expunge merge rewrites that segment once retention releases the deletes.
        // The deletes must happen BEFORE the first snapshot: deletes are operations and advance max_seq_no,
        // and the bug only manifests when both snapshots see identical seq_no history around a merge that
        // changed the files.
        logger.info("--> deleting 2 docs before the first snapshot");
        client().prepareDelete(indexName, "0-0").get();
        client().prepareDelete(indexName, "0-1").get();
        flushAndRefresh(indexName);

        // guard: the setup really produced tombstones for the merge to reclaim
        final IndexStats statsAfterDelete = indicesAdmin().prepareStats(indexName).get().getIndex(indexName);
        assertThat(statsAfterDelete.getIndexShards().get(0).getPrimary().getDocs().getDeleted(), greaterThan(0L));

        final String repo = "test-repo";
        createRepository(repo, "fs");
        createSnapshot(repo, "snap-1", Collections.singletonList(indexName));

        // Retention leases advance on the sync interval; retry until the merge has actually
        // reclaimed the deleted docs (a no-op merge writes no new commit, which would make
        // this test pass or fail for the wrong reasons).
        logger.info("--> force merging until the deleted docs are reclaimed");
        assertBusy(() -> {
            final ForceMergeRequestBuilder forceMerge = indicesAdmin().prepareForceMerge(indexName).setFlush(true);
            if (onlyExpungeDeletes) {
                forceMerge.setOnlyExpungeDeletes(true);
            } else {
                forceMerge.setMaxNumSegments(1);
            }
            assertThat(forceMerge.get().getFailedShards(), is(0));
            final IndexStats stats = indicesAdmin().prepareStats(indexName).get().getIndex(indexName);
            assertThat(stats.getIndexShards().get(0).getPrimary().getDocs().getDeleted(), is(0L));
        });

        final String snapshot2 = "snap-2";
        createSnapshot(repo, snapshot2, Collections.singletonList(indexName));

        // snapshot must upload something rather than reuse snap-1's file list
        final SnapshotStats secondSnapshotShardStatus = getStats(repo, snapshot2).getIndices().get(indexName).getShards().get(0).getStats();
        assertThat(secondSnapshotShardStatus.getIncrementalFileCount(), greaterThan(0));
    }

    public void testRecordCorrectSegmentCountsWithBackgroundMerges() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        final String indexName = "test";
        // disable merges
        assertAcked(prepareCreate(indexName).setSettings(indexSettingsNoReplicas(1).put(MergePolicyConfig.INDEX_MERGE_ENABLED, "false")));

        // create an empty snapshot so that later snapshots run as quickly as possible
        createFullSnapshot(repoName, "empty");

        // create a situation where we temporarily have a bunch of segments until the merges can catch up
        long id = 0;
        final int rounds = scaledRandomIntBetween(5, 9);
        for (int i = 0; i < rounds; ++i) {
            final int numDocs = scaledRandomIntBetween(100, 1000);
            BulkRequestBuilder request = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < numDocs; ++j) {
                request.add(
                    new IndexRequest(indexName).id(Long.toString(id++))
                        .source(jsonBuilder().startObject().field("l", randomLong()).endObject())
                );
            }
            assertNoFailures(request.get());
        }

        // snapshot with a bunch of unmerged segments
        final SnapshotInfo before = createFullSnapshot(repoName, "snapshot-before");
        final SnapshotInfo.IndexSnapshotDetails beforeIndexDetails = before.indexSnapshotDetails().get(indexName);
        final long beforeSegmentCount = beforeIndexDetails.getMaxSegmentsPerShard();

        // reactivate merges
        assertAcked(indicesAdmin().prepareClose(indexName).get());
        updateIndexSettings(
            Settings.builder()
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), "2")
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, "true"),
            indexName
        );
        assertAcked(indicesAdmin().prepareOpen(indexName).get());
        assertEquals(0, indicesAdmin().prepareForceMerge(indexName).setFlush(true).get().getFailedShards());

        // wait for merges to reduce segment count
        assertBusy(() -> {
            IndicesStatsResponse stats = indicesAdmin().prepareStats(indexName).setSegments(true).get();
            assertThat(stats.getIndex(indexName).getPrimaries().getSegments().getCount(), lessThan(beforeSegmentCount));
        }, 30L, TimeUnit.SECONDS);

        final SnapshotInfo after = createFullSnapshot(repoName, "snapshot-after");
        final int incrementalFileCount = clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT)
            .setRepository(repoName)
            .setSnapshots(after.snapshotId().getName())
            .get()
            .getSnapshots()
            .get(0)
            .getStats()
            .getIncrementalFileCount();
        assertEquals(0, incrementalFileCount);
        logger.info("--> no files have changed between snapshots, asserting that segment counts are constant as well");
        final SnapshotInfo.IndexSnapshotDetails afterIndexDetails = after.indexSnapshotDetails().get(indexName);
        assertEquals(beforeSegmentCount, afterIndexDetails.getMaxSegmentsPerShard());
    }

    private void assertCountInIndexThenDelete(String index, long expectedCount) {
        logger.info("--> asserting that index [{}] contains [{}] documents", index, expectedCount);
        assertDocCount(index, expectedCount);
        logger.info("--> deleting index [{}]", index);
        assertThat(indicesAdmin().prepareDelete(index).get().isAcknowledged(), is(true));
    }

    private void assertTwoIdenticalShardSnapshots(String repo, String indexName, String snapshot1, String snapshot2) {
        logger.info("--> asserting that snapshots [{}] and [{}] are referring to the same files in the repository", snapshot1, snapshot2);
        final SnapshotStats firstSnapshotShardStatus = getStats(repo, snapshot1).getIndices().get(indexName).getShards().get(0).getStats();
        final int totalFilesInShard = firstSnapshotShardStatus.getTotalFileCount();
        assertThat(totalFilesInShard, greaterThan(0));
        final SnapshotStats secondSnapshotShardStatus = getStats(repo, snapshot2).getIndices().get(indexName).getShards().get(0).getStats();
        assertThat(secondSnapshotShardStatus.getTotalFileCount(), is(totalFilesInShard));
        assertThat(secondSnapshotShardStatus.getIncrementalFileCount(), is(0));
    }

    private SnapshotStatus getStats(String repository, String snapshot) {
        return clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, repository).setSnapshots(snapshot).get().getSnapshots().get(0);
    }

    private void ensureRestoreSingleShardSuccessfully(String repo, String indexName, String snapshot, String indexSuffix) {
        logger.info("--> restoring [{}]", snapshot);
        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repo, snapshot)
            .setIndices(indexName)
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1" + indexSuffix)
            .setWaitForCompletion(true)
            .get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.totalShards(), is(1));
        assertThat(restoreInfo.failedShards(), is(0));
    }
}
