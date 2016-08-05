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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ClusterScope(scope = Scope.TEST)
public class CreateIndexIT extends ESIntegTestCase {
    public void testCreationDateGivenFails() {
        try {
            prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, 4L)).get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("unknown setting [index.creation_date] please check that any required plugins are installed, or check the " +
                "breaking changes documentation for removed settings", ex.getMessage());
        }
    }

    public void testCreationDateGenerated() {
        long timeBeforeRequest = System.currentTimeMillis();
        prepareCreate("test").get();
        long timeAfterRequest = System.currentTimeMillis();
        ClusterStateResponse response = client().admin().cluster().prepareState().get();
        ClusterState state = response.getState();
        assertThat(state, notNullValue());
        MetaData metadata = state.getMetaData();
        assertThat(metadata, notNullValue());
        ImmutableOpenMap<String, IndexMetaData> indices = metadata.getIndices();
        assertThat(indices, notNullValue());
        assertThat(indices.size(), equalTo(1));
        IndexMetaData index = indices.get("test");
        assertThat(index, notNullValue());
        assertThat(index.getCreationDate(), allOf(lessThanOrEqualTo(timeAfterRequest), greaterThanOrEqualTo(timeBeforeRequest)));
    }

    public void testDoubleAddMapping() throws Exception {
        try {
            prepareCreate("test")
                    .addMapping("type1", "date", "type=date")
                    .addMapping("type1", "num", "type=integer");
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        try {
            prepareCreate("test")
                    .addMapping("type1", new HashMap<String,Object>())
                    .addMapping("type1", new HashMap<String,Object>());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        try {
            prepareCreate("test")
                    .addMapping("type1", jsonBuilder())
                    .addMapping("type1", jsonBuilder());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    public void testInvalidShardCountSettings() throws Exception {
        int value = randomIntBetween(-10, 0);
        try {
            prepareCreate("test").setSettings(Settings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, value)
                    .build())
            .get();
            fail("should have thrown an exception about the primary shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_shards] must be >= 1", e.getMessage());
        }
        value = randomIntBetween(-10, -1);
        try {
            prepareCreate("test").setSettings(Settings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, value)
                    .build())
                    .get();
            fail("should have thrown an exception about the replica shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
        }

    }

    public void testCreateIndexWithBlocks() {
        try {
            setClusterReadOnly(true);
            assertBlocked(prepareCreate("test"));
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testCreateIndexWithMetadataBlocks() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_BLOCKS_METADATA, true)));
        assertBlocked(client().admin().indices().prepareGetSettings("test"), IndexMetaData.INDEX_METADATA_BLOCK);
        disableIndexBlock("test", IndexMetaData.SETTING_BLOCKS_METADATA);
    }

    public void testUnknownSettingFails() {
        try {
            prepareCreate("test").setSettings(Settings.builder()
                .put("index.unknown.value", "this must fail")
                .build())
                .get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("unknown setting [index.unknown.value] please check that any required plugins are installed, or check the" +
                " breaking changes documentation for removed settings", e.getMessage());
        }
    }

    public void testInvalidShardCountSettingsWithoutPrefix() throws Exception {
        int value = randomIntBetween(-10, 0);
        try {
            prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS.substring(IndexMetaData.INDEX_SETTING_PREFIX.length()), value)
                .build())
                .get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_shards] must be >= 1", e.getMessage());
        }
        value = randomIntBetween(-10, -1);
        try {
            prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS.substring(IndexMetaData.INDEX_SETTING_PREFIX.length()), value)
                .build())
                .get();
            fail("should have thrown an exception about the shard count");
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to parse value [" + value + "] for setting [index.number_of_replicas] must be >= 0", e.getMessage());
        }
    }

    public void testCreateAndDeleteIndexConcurrently() throws InterruptedException {
        createIndex("test");
        final AtomicInteger indexVersion = new AtomicInteger(0);
        final Object indexVersionLock = new Object();
        final CountDownLatch latch = new CountDownLatch(1);
        int numDocs = randomIntBetween(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "test").setSource("index_version", indexVersion.get()).get();
        }
        synchronized (indexVersionLock) { // not necessarily needed here but for completeness we lock here too
            indexVersion.incrementAndGet();
        }
        client().admin().indices().prepareDelete("test").execute(new ActionListener<DeleteIndexResponse>() { // this happens async!!!
                @Override
                public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                    Thread thread = new Thread() {
                     @Override
                    public void run() {
                         try {
                             // recreate that index
                             client().prepareIndex("test", "test").setSource("index_version", indexVersion.get()).get();
                             synchronized (indexVersionLock) {
                                 // we sync here since we have to ensure that all indexing operations below for a given ID are done before
                                 // we increment the index version otherwise a doc that is in-flight could make it into an index that it
                                 // was supposed to be deleted for and our assertion fail...
                                 indexVersion.incrementAndGet();
                             }
                             // from here on all docs with index_version == 0|1 must be gone!!!! only 2 are ok;
                             assertAcked(client().admin().indices().prepareDelete("test").get());
                         } finally {
                             latch.countDown();
                         }
                     }
                    };
                    thread.start();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
        numDocs = randomIntBetween(100, 200);
        for (int i = 0; i < numDocs; i++) {
            try {
                synchronized (indexVersionLock) {
                    client().prepareIndex("test", "test").setSource("index_version", indexVersion.get())
                        .setTimeout(TimeValue.timeValueSeconds(10)).get();
                }
            } catch (IndexNotFoundException inf) {
                // fine
            } catch (UnavailableShardsException ex) {
                assertEquals(ex.getCause().getClass(), IndexNotFoundException.class);
                // fine we run into a delete index while retrying
            }
        }
        latch.await();
        refresh();

        // we only really assert that we never reuse segments of old indices or anything like this here and that nothing fails with
        // crazy exceptions
        SearchResponse expected = client().prepareSearch("test").setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setQuery(new RangeQueryBuilder("index_version").from(indexVersion.get(), true)).get();
        SearchResponse all = client().prepareSearch("test").setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        assertEquals(expected + " vs. " + all, expected.getHits().getTotalHits(), all.getHits().getTotalHits());
        logger.info("total: {}", expected.getHits().getTotalHits());
    }

    /**
     * Asserts that the root cause of mapping conflicts is readable.
     */
    public void testMappingConflictRootCause() throws Exception {
        CreateIndexRequestBuilder b = prepareCreate("test");
        b.addMapping("type1", jsonBuilder().startObject().startObject("properties")
                .startObject("text")
                    .field("type", "text")
                    .field("analyzer", "standard")
                    .field("search_analyzer", "whitespace")
                .endObject().endObject().endObject());
        b.addMapping("type2", jsonBuilder().humanReadable(true).startObject().startObject("properties")
                .startObject("text")
                    .field("type", "text")
                .endObject().endObject().endObject());
        try {
            b.get();
        } catch (MapperParsingException e) {
            StringBuilder messages = new StringBuilder();
            for (Exception rootCause: e.guessRootCauses()) {
                messages.append(rootCause.getMessage());
            }
            assertThat(messages.toString(), containsString("mapper [text] is used by multiple types"));
        }
    }

    public void testRestartIndexCreationAfterFullClusterRestart() throws Exception {
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().put("cluster.routing.allocation.enable",
            "none")).get();
        client().admin().indices().prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).setSettings(indexSettings()).get();
        internalCluster().fullRestart();
        ensureGreen("test");
    }

    public void testCreateShrinkIndexToN() {
        int[][] possibleShardSplits = new int[][] {{8,4,2}, {9, 3, 1}, {4, 2, 1}, {15,5,1}};
        int[] shardSplits = randomFrom(possibleShardSplits);
        assertEquals(shardSplits[0], (shardSplits[0] / shardSplits[1]) * shardSplits[1]);
        assertEquals(shardSplits[1], (shardSplits[1] / shardSplits[2]) * shardSplits[2]);
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", shardSplits[0])).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source", "t1", Integer.toString(i)).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}").get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes()
            .getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        String mergeNode = discoveryNodes[0].getName();
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin().indices().prepareUpdateSettings("source")
            .setSettings(Settings.builder()
                .put("index.routing.allocation.require._name", mergeNode)
                .put("index.blocks.write", true)).get();
        ensureGreen();
        // now merge source into a 4 shard index
        assertAcked(client().admin().indices().prepareShrinkIndex("source", "first_shrink")
            .setSettings(Settings.builder()
                .put("index.number_of_replicas", 0)
                .put("index.number_of_shards", shardSplits[1]).build()).get());
        ensureGreen();
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

        for (int i = 0; i < 20; i++) { // now update
            client().prepareIndex("first_shrink", "t1", Integer.toString(i)).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}").get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

        // relocate all shards to one node such that we can merge it.
        client().admin().indices().prepareUpdateSettings("first_shrink")
            .setSettings(Settings.builder()
                .put("index.routing.allocation.require._name", mergeNode)
                .put("index.blocks.write", true)).get();
        ensureGreen();
        // now merge source into a 2 shard index
        assertAcked(client().admin().indices().prepareShrinkIndex("first_shrink", "second_shrink")
            .setSettings(Settings.builder()
                .put("index.number_of_replicas", 0)
                .put("index.number_of_shards", shardSplits[2]).build()).get());
        ensureGreen();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        // let it be allocated anywhere and bump replicas
        client().admin().indices().prepareUpdateSettings("second_shrink")
            .setSettings(Settings.builder()
                .putNull("index.routing.allocation.include._id")
                .put("index.number_of_replicas", 1)).get();
        ensureGreen();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

        for (int i = 0; i < 20; i++) { // now update
            client().prepareIndex("second_shrink", "t1", Integer.toString(i)).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}").get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
    }

    public void testCreateShrinkIndex() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", randomIntBetween(2, 7))).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source", randomFrom("t1", "t2", "t3")).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}").get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes()
            .getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        String mergeNode = discoveryNodes[0].getName();
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin().indices().prepareUpdateSettings("source")
            .setSettings(Settings.builder()
                .put("index.routing.allocation.require._name", mergeNode)
                .put("index.blocks.write", true)).get();
        ensureGreen();
        // now merge source into a single shard index
        assertAcked(client().admin().indices().prepareShrinkIndex("source", "target")
            .setSettings(Settings.builder().put("index.number_of_replicas", 0).build()).get());
        ensureGreen();
        assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        // bump replicas
        client().admin().indices().prepareUpdateSettings("target")
            .setSettings(Settings.builder()
                .put("index.number_of_replicas", 1)).get();
        ensureGreen();
        assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

        for (int i = 20; i < 40; i++) {
            client().prepareIndex("target", randomFrom("t1", "t2", "t3")).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}").get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 40);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

    }
    /**
     * Tests that we can manually recover from a failed allocation due to shards being moved away etc.
     */
    public void testCreateShrinkIndexFails() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings())
            .put("number_of_shards", randomIntBetween(2, 7))
            .put("number_of_replicas", 0)).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source", randomFrom("t1", "t2", "t3")).setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}").get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes()
            .getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode.class);
        String spareNode = discoveryNodes[0].getName();
        String mergeNode = discoveryNodes[1].getName();
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin().indices().prepareUpdateSettings("source")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode)
                .put("index.blocks.write", true)).get();
        ensureGreen();

        // now merge source into a single shard index
        client().admin().indices().prepareShrinkIndex("source", "target")
            .setSettings(Settings.builder()
            .put("index.routing.allocation.exclude._name", mergeNode) // we manually exclude the merge node to forcefully fuck it up
            .put("index.number_of_replicas", 0)
            .put("index.allocation.max_retries", 1).build()).get();

        // now we move all shards away from the merge node
        client().admin().indices().prepareUpdateSettings("source")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", spareNode)
                .put("index.blocks.write", true)).get();
        ensureGreen("source");

        client().admin().indices().prepareUpdateSettings("target") // erase the forcefully fuckup!
            .setSettings(Settings.builder().putNull("index.routing.allocation.exclude._name")).get();
        // wait until it fails
        assertBusy(() -> {
            ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
            RoutingTable routingTables = clusterStateResponse.getState().routingTable();
            assertTrue(routingTables.index("target").shard(0).getShards().get(0).unassigned());
            assertEquals(UnassignedInfo.Reason.ALLOCATION_FAILED,
                routingTables.index("target").shard(0).getShards().get(0).unassignedInfo().getReason());
            assertEquals(1,
                routingTables.index("target").shard(0).getShards().get(0).unassignedInfo().getNumFailedAllocations());
        });
        client().admin().indices().prepareUpdateSettings("source") // now relocate them all to the right node
            .setSettings(Settings.builder()
                .put("index.routing.allocation.require._name", mergeNode)).get();
        ensureGreen("source");

        final InternalClusterInfoService infoService = (InternalClusterInfoService) internalCluster().getInstance(ClusterInfoService.class,
            internalCluster().getMasterName());
        infoService.refresh();
        // kick off a retry and wait until it's done!
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        long expectedShardSize = clusterRerouteResponse.getState().routingTable().index("target")
            .shard(0).getShards().get(0).getExpectedShardSize();
        // we support the expected shard size in the allocator to sum up over the source index shards
        assertTrue("expected shard size must be set but wasn't: " + expectedShardSize, expectedShardSize > 0);
        ensureGreen();
        assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
    }

    /**
     * This test ensures that index creation adheres to the {@link IndexMetaData#SETTING_WAIT_FOR_ACTIVE_SHARDS}.
     */
    public void testDefaultWaitForActiveShardsUsesIndexSetting() throws Exception {
        final int numReplicas = internalCluster().numDataNodes();
        Settings settings = Settings.builder()
                                .put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(numReplicas))
                                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), numReplicas)
                                .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());

        // all should fail
        settings = Settings.builder()
                       .put(settings)
                       .put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "all")
                       .build();
        assertFalse(client().admin().indices().prepareCreate("test-idx-2").setSettings(settings).setTimeout("100ms").get().isShardsAcked());

        // the numeric equivalent of all should also fail
        settings = Settings.builder()
                       .put(settings)
                       .put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(numReplicas + 1))
                       .build();
        assertFalse(client().admin().indices().prepareCreate("test-idx-3").setSettings(settings).setTimeout("100ms").get().isShardsAcked());
    }
}
