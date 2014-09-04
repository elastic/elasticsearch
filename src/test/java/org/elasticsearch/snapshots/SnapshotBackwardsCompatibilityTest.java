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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class SnapshotBackwardsCompatibilityTest extends ElasticsearchBackwardsCompatIntegrationTest {

    @Test
    public void testSnapshotAndRestore() throws ExecutionException, InterruptedException, IOException {
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", newTempDir(LifecycleScope.SUITE).getAbsolutePath())
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000))));
        String[] indicesBefore = new String[randomIntBetween(2,5)];
        String[] indicesAfter = new String[randomIntBetween(2,5)];
        for (int i = 0; i < indicesBefore.length; i++) {
            indicesBefore[i] = "index_before_" + i;
            createIndex(indicesBefore[i]);
        }
        for (int i = 0; i < indicesAfter.length; i++) {
            indicesAfter[i] = "index_after_" + i;
            createIndex(indicesAfter[i]);
        }
        String[] indices = new String[indicesBefore.length + indicesAfter.length];
        System.arraycopy(indicesBefore, 0, indices, 0, indicesBefore.length);
        System.arraycopy(indicesAfter, 0, indices, indicesBefore.length, indicesAfter.length);
        ensureYellow();
        logger.info("--> indexing some data");
        IndexRequestBuilder[] buildersBefore = new IndexRequestBuilder[randomIntBetween(10, 200)];
        for (int i = 0; i < buildersBefore.length; i++) {
            buildersBefore[i] = client().prepareIndex(RandomPicks.randomFrom(getRandom(), indicesBefore), "foo", Integer.toString(i)).setSource("{ \"foo\" : \"bar\" } ");
        }
        IndexRequestBuilder[] buildersAfter = new IndexRequestBuilder[randomIntBetween(10, 200)];
        for (int i = 0; i < buildersAfter.length; i++) {
            buildersAfter[i] = client().prepareIndex(RandomPicks.randomFrom(getRandom(), indicesBefore), "bar", Integer.toString(i)).setSource("{ \"foo\" : \"bar\" } ");
        }
        indexRandom(true, buildersBefore);
        indexRandom(true, buildersAfter);
        assertThat(client().prepareCount(indices).get().getCount(), equalTo((long) (buildersBefore.length + buildersAfter.length)));
        long[] counts = new long[indices.length];
        for (int i = 0; i < indices.length; i++) {
            counts[i] = client().prepareCount(indices[i]).get().getCount();
        }

        logger.info("--> snapshot subset of indices before upgrage");
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("index_before_*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap-1").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete some data from indices that were already snapshotted");
        int howMany = randomIntBetween(1, buildersBefore.length);

        for (int i = 0; i < howMany; i++) {
            IndexRequestBuilder indexRequestBuilder = RandomPicks.randomFrom(getRandom(), buildersBefore);
            IndexRequest request = indexRequestBuilder.request();
            client().prepareDelete(request.index(), request.type(), request.id()).get();
        }
        refresh();
        final long numDocs = client().prepareCount(indices).get().getCount();
        assertThat(client().prepareCount(indices).get().getCount(), lessThan((long) (buildersBefore.length + buildersAfter.length)));


        client().admin().indices().prepareUpdateSettings(indices).setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "none")).get();
        backwardsCluster().allowOnAllNodes(indices);
        logClusterState();
        boolean upgraded;
        do {
            logClusterState();
            CountResponse countResponse = client().prepareCount().get();
            assertHitCount(countResponse, numDocs);
            upgraded = backwardsCluster().upgradeOneNode();
            ensureYellow();
            countResponse = client().prepareCount().get();
            assertHitCount(countResponse, numDocs);
        } while (upgraded);
        client().admin().indices().prepareUpdateSettings(indices).setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "all")).get();

        logger.info("--> close indices");

        client().admin().indices().prepareClose("index_before_*").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureYellow();
        assertThat(client().prepareCount(indices).get().getCount(), equalTo((long) (buildersBefore.length + buildersAfter.length)));
        for (int i = 0; i < indices.length; i++) {
            assertThat(counts[i], equalTo(client().prepareCount(indices[i]).get().getCount()));
        }

        logger.info("--> snapshot subset of indices after upgrade");
        createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap-2").setWaitForCompletion(true).setIndices("index_*").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));

        // Test restore after index deletion
        logger.info("--> delete indices");
        String index = RandomPicks.randomFrom(getRandom(), indices);
        cluster().wipeIndices(index);
        logger.info("--> restore one index after deletion");
        restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap-2").setWaitForCompletion(true).setIndices(index).execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureYellow();
        assertThat(client().prepareCount(indices).get().getCount(), equalTo((long) (buildersBefore.length + buildersAfter.length)));
        for (int i = 0; i < indices.length; i++) {
            assertThat(counts[i], equalTo(client().prepareCount(indices[i]).get().getCount()));
        }
    }

    public void testSnapshotMoreThanOnce() throws ExecutionException, InterruptedException, IOException {
        Client client = client();
        final File tempDir = newTempDir(LifecycleScope.SUITE).getAbsoluteFile();
        logger.info("-->  creating repository");
        assertAcked(client.admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder()
                        .put("location", tempDir)
                        .put("compress", randomBoolean())
                        .put("chunk_size", randomIntBetween(100, 1000))));

        // only one shard
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
        ));
        ensureYellow();
        logger.info("-->  indexing");

        final int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "doc", Integer.toString(i)).setSource("foo", "bar" + i);
        }
        indexRandom(true, builders);
        flushAndRefresh();
        assertNoFailures(client().admin().indices().prepareOptimize("test").setForce(true).setFlush(true).setWaitForMerge(true).setMaxNumSegments(1).get());

        CreateSnapshotResponse createSnapshotResponseFirst = client.admin().cluster().prepareCreateSnapshot("test-repo", "test").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseFirst.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseFirst.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseFirst.getSnapshotInfo().totalShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFiles(), greaterThan(1));
            }
        }
        if (frequently()) {
            logger.info("-->  upgrade");
            client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "none")).get();
            backwardsCluster().allowOnAllNodes("test");
            logClusterState();
            boolean upgraded;
            do {
                logClusterState();
                CountResponse countResponse = client().prepareCount().get();
                assertHitCount(countResponse, numDocs);
                upgraded = backwardsCluster().upgradeOneNode();
                ensureYellow();
                countResponse = client().prepareCount().get();
                assertHitCount(countResponse, numDocs);
            } while (upgraded);
            client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "all")).get();
        }
        if (cluster().numDataNodes() > 1 && randomBoolean()) { // only bump the replicas if we have enough nodes
            logger.info("--> move from 0 to 1 replica");
            client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)).get();
        }
        logger.debug("---> repo exists: " + new File(tempDir, "indices/test/0").exists() + " files: " + Arrays.toString(new File(tempDir, "indices/test/0").list())); // it's only one shard!
        CreateSnapshotResponse createSnapshotResponseSecond = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-1").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseSecond.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseSecond.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseSecond.getSnapshotInfo().totalShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-1").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-1").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {

                assertThat(status.getStats().getProcessedFiles(), equalTo(1)); // we flush before the snapshot such that we have to process the segments_N files
            }
        }

        client().prepareDelete("test", "doc", "1").get();
        CreateSnapshotResponse createSnapshotResponseThird = client.admin().cluster().prepareCreateSnapshot("test-repo", "test-2").setWaitForCompletion(true).setIndices("test").get();
        assertThat(createSnapshotResponseThird.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponseThird.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponseThird.getSnapshotInfo().totalShards()));
        assertThat(client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-2").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-2").get().getSnapshots().get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFiles(), equalTo(2)); // we flush before the snapshot such that we have to process the segments_N files plus the .del file
            }
        }
    }
}
