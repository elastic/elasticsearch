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

import org.elasticsearch.Version;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.client;
import static org.elasticsearch.test.ESIntegTestCase.cluster;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

/**
 * Test cases shared between SharedClusterSnapshotRestoreIT and SnapshotBackwardsCompatibilityIT.
 */
public class SnapshotSharedTest {
    /**
     * Callback used after snapshots have been created.
     */
    public static interface AfterSnapshotAction {
        public static AfterSnapshotAction NOOP = new AfterSnapshotAction() {
            @Override
            public void run(String[] indices, int numDocs) {
            }
        };
        /**
         * Called after the snapshots have been created.
         * @param indices the indices that the test is using
         * @param numDocs the number of docs in open indices on the cluster
         */
        public void run(String[] indices, int numDocs);
    }

    public static void testBasicWorkflow(ESLogger logger, ESIntegTestCase testCase, AfterSnapshotAction afterSnapshotAction, Matcher<Version> snapshotVersion) {
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", testCase.randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        testCase.createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        testCase.ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            testCase.index("test-idx-1", "doc", Integer.toString(i), "foo", "bar" + i);
            testCase.index("test-idx-2", "doc", Integer.toString(i), "foo", "baz" + i);
            testCase.index("test-idx-3", "doc", Integer.toString(i), "foo", "baz" + i);
        }
        testCase.refresh();
        assertHitCount(client().prepareSearch("test-idx-1").get(), 100L);
        assertHitCount(client().prepareSearch("test-idx-2").get(), 100L);
        assertHitCount(client().prepareSearch("test-idx-3").get(), 100L);

        ListenableActionFuture<FlushResponse> flushResponseFuture = null;
        if (randomBoolean()) {
            ArrayList<String> indicesToFlush = new ArrayList<>();
            for (int i = 1; i < 4; i++) {
                if (randomBoolean()) {
                    indicesToFlush.add("test-idx-" + i);
                }
            }
            if (!indicesToFlush.isEmpty()) {
                String[] indices = indicesToFlush.toArray(new String[indicesToFlush.size()]);
                logger.info("--> starting asynchronous flush for indices {}", Arrays.toString(indices));
                flushResponseFuture = client().admin().indices().prepareFlush(indices).execute();
            }
        }
        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-3").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        String snapshotName;
        if (ESIntegTestCase.getMinimumVersionInCluster().onOrAfter(Version.V_2_2_0)) {
            snapshotName = randomFrom("test-snap", "_all", "*", "*-snap", "test*");
        } else {
            snapshotName = "test-snap";
        }
        SnapshotInfo snapshotInfo = client().admin().cluster().prepareGetSnapshots("test-repo")
            .setSnapshots(snapshotName).get().getSnapshots().get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), snapshotVersion);

        logger.info("--> delete some data");
        for (int i = 0; i < 50; i++) {
            client().prepareDelete("test-idx-1", "doc", Integer.toString(i)).get();
        }
        for (int i = 50; i < 100; i++) {
            client().prepareDelete("test-idx-2", "doc", Integer.toString(i)).get();
        }
        for (int i = 0; i < 100; i += 2) {
            client().prepareDelete("test-idx-3", "doc", Integer.toString(i)).get();
        }
        assertAllSuccessful(testCase.refresh());
        assertHitCount(client().prepareSearch("test-idx-1").get(), 50L);
        assertHitCount(client().prepareSearch("test-idx-2").get(), 50L);
        assertHitCount(client().prepareSearch("test-idx-3").get(), 50L);

        logger.info("--> close indices");
        client().admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        afterSnapshotAction.run(new String[] {"test-idx-1", "test-idx-2", "test-idx-3"}, 50);

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        testCase.ensureGreen();
        for (int i=0; i<5; i++) {
            assertHitCount(client().prepareSearch("test-idx-1").get(), 100L);
            assertHitCount(client().prepareSearch("test-idx-2").get(), 100L);
            assertHitCount(client().prepareSearch("test-idx-3").get(), 50L);
        }

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2");
        logger.info("--> restore one index after deletion");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx-*", "-test-idx-2").execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        testCase.ensureGreen();
        for (int i=0; i<5; i++) {
            assertHitCount(client().prepareSearch("test-idx-1").get(), 100L);
        }
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.getMetaData().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetaData().hasIndex("test-idx-2"), equalTo(false));

        if (flushResponseFuture != null) {
            // Finish flush
            flushResponseFuture.actionGet();
        }
    }

    /**
     * Take multiple snapshots of the same index and assert things along the
     * way.
     *
     * @param actionBetweenSnapshots action run after the first snapshot is created
     */
    public static void testSnapshotMoreThanOnce(ESLogger logger, ESIntegTestCase testCase, AfterSnapshotAction actionBetweenSnapshots, int numberOfReplicas) throws Exception {
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.settingsBuilder()
                        .put("location", testCase.randomRepoPath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)));

        // only one shard
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(testCase.indexSettings()).setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)));
        testCase.ensureGreen();
        logger.info("-->  indexing");

        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "doc", Integer.toString(i)).setSource("foo", "bar" + i);
        }
        testCase.indexRandom(true, builders);
        testCase.flushAndRefresh();
        assertNoFailures(client().admin().indices().prepareForceMerge("test").setFlush(true).setMaxNumSegments(1).get());

        CreateSnapshotResponse createSnapshotResponseFirst = client().admin().cluster().prepareCreateSnapshot("test-repo", "test").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseFirst.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseFirst.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseFirst.getSnapshotInfo().totalShards()));
        assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFiles(), greaterThan(1));
            }
        }

        actionBetweenSnapshots.run(new String[] {"test"}, numdocs);

        CreateSnapshotResponse createSnapshotResponseSecond = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-1").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseSecond.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseSecond.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseSecond.getSnapshotInfo().totalShards()));
        assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-1").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-1").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFiles(), equalTo(0));
            }
        }

        client().prepareDelete("test", "doc", "1").get();
        CreateSnapshotResponse createSnapshotResponseThird = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-2").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseThird.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseThird.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseThird.getSnapshotInfo().totalShards()));
        assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-2").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-2").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFiles(), equalTo(2)); // we flush before the snapshot such that we have to process the segments_N files plus the .del file
            }
        }
    }
}
