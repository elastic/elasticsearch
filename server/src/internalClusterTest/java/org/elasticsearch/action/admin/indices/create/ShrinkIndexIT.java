/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentType;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ShrinkIndexIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testCreateShrinkIndexToN() {

        assumeFalse("https://github.com/elastic/elasticsearch/issues/34080", Constants.WINDOWS);

        int[][] possibleShardSplits = new int[][] { { 8, 4, 2 }, { 9, 3, 1 }, { 4, 2, 1 }, { 15, 5, 1 } };
        int[] shardSplits = randomFrom(possibleShardSplits);
        assertEquals(shardSplits[0], (shardSplits[0] / shardSplits[1]) * shardSplits[1]);
        assertEquals(shardSplits[1], (shardSplits[1] / shardSplits[2]) * shardSplits[2]);
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", shardSplits[0])).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source")
                .setId(Integer.toString(i))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON)
                .get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode[]::new);
        String mergeNode = discoveryNodes[0].getName();
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin()
            .indices()
            .prepareUpdateSettings("source")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true))
            .get();
        ensureGreen();
        // now merge source into a 4 shard index
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("source", "first_shrink")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", shardSplits[1])
                        .putNull("index.blocks.write")
                        .build()
                )
                .get()
        );
        ensureGreen();
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

        for (int i = 0; i < 20; i++) { // now update
            client().prepareIndex("first_shrink")
                .setId(Integer.toString(i))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON)
                .get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

        // relocate all shards to one node such that we can merge it.
        client().admin()
            .indices()
            .prepareUpdateSettings("first_shrink")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true))
            .get();
        ensureGreen();
        // now merge source into a 2 shard index
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("first_shrink", "second_shrink")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", shardSplits[2])
                        .putNull("index.blocks.write")
                        .putNull("index.routing.allocation.require._name")
                        .build()
                )
                .get()
        );
        ensureGreen();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        // let it be allocated anywhere and bump replicas
        client().admin()
            .indices()
            .prepareUpdateSettings("second_shrink")
            .setSettings(Settings.builder().putNull("index.routing.allocation.include._id").put("index.number_of_replicas", 1))
            .get();
        ensureGreen();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

        for (int i = 0; i < 20; i++) { // now update
            client().prepareIndex("second_shrink")
                .setId(Integer.toString(i))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON)
                .get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
    }

    public void testShrinkIndexPrimaryTerm() throws Exception {
        int numberOfShards = randomIntBetween(2, 20);
        int numberOfTargetShards = randomValueOtherThanMany(n -> numberOfShards % n != 0, () -> randomIntBetween(1, numberOfShards - 1));
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", numberOfShards)).get();

        final ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getDataNodes();
        assertThat(dataNodes.size(), greaterThanOrEqualTo(2));
        final DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode[]::new);
        final String mergeNode = discoveryNodes[0].getName();
        // This needs more than the default timeout if a large number of shards were created.
        ensureGreen(TimeValue.timeValueSeconds(120));

        // fail random primary shards to force primary terms to increase
        final Index source = resolveIndex("source");
        final int iterations = scaledRandomIntBetween(0, 16);
        for (int i = 0; i < iterations; i++) {
            final String node = randomSubsetOf(1, internalCluster().nodesInclude("source")).get(0);
            final IndicesService indexServices = internalCluster().getInstance(IndicesService.class, node);
            final IndexService indexShards = indexServices.indexServiceSafe(source);
            for (final Integer shardId : indexShards.shardIds()) {
                final IndexShard shard = indexShards.getShard(shardId);
                if (shard.routingEntry().primary() && randomBoolean()) {
                    disableAllocation("source");
                    shard.failShard("test", new Exception("test"));
                    // this can not succeed until the shard is failed and a replica is promoted
                    int id = 0;
                    while (true) {
                        // find an ID that routes to the right shard, we will only index to the shard that saw a primary failure
                        final String s = Integer.toString(id);
                        final int hash = Math.floorMod(Murmur3HashFunction.hash(s), numberOfShards);
                        if (hash == shardId) {
                            final IndexRequest request = new IndexRequest("source").id(s)
                                .source("{ \"f\": \"" + s + "\"}", XContentType.JSON);
                            client().index(request).get();
                            break;
                        } else {
                            id++;
                        }
                    }
                    enableAllocation("source");
                    ensureGreen();
                }
            }
        }

        // relocate all shards to one node such that we can merge it.
        final Settings.Builder prepareShrinkSettings = Settings.builder()
            .put("index.routing.allocation.require._name", mergeNode)
            .put("index.blocks.write", true);
        client().admin().indices().prepareUpdateSettings("source").setSettings(prepareShrinkSettings).get();
        ensureGreen(TimeValue.timeValueSeconds(120)); // needs more than the default to relocate many shards

        final IndexMetadata indexMetadata = indexMetadata(client(), "source");
        final long beforeShrinkPrimaryTerm = IntStream.range(0, numberOfShards).mapToLong(indexMetadata::primaryTerm).max().getAsLong();

        // now merge source into target
        final Settings shrinkSettings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", numberOfTargetShards)
            .build();
        assertAcked(client().admin().indices().prepareResizeIndex("source", "target").setSettings(shrinkSettings).get());

        ensureGreen(TimeValue.timeValueSeconds(120));

        final IndexMetadata afterShrinkIndexMetadata = indexMetadata(client(), "target");
        for (int shardId = 0; shardId < numberOfTargetShards; shardId++) {
            assertThat(afterShrinkIndexMetadata.primaryTerm(shardId), equalTo(beforeShrinkPrimaryTerm + 1));
        }
    }

    private static IndexMetadata indexMetadata(final Client client, final String index) {
        final ClusterStateResponse clusterStateResponse = client.admin().cluster().state(new ClusterStateRequest()).actionGet();
        return clusterStateResponse.getState().metadata().index(index);
    }

    public void testCreateShrinkIndex() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Version version = VersionUtils.randomVersion(random());
        prepareCreate("source").setSettings(
            Settings.builder().put(indexSettings()).put("number_of_shards", randomIntBetween(2, 7)).put("index.version.created", version)
        ).get();
        final int docs = randomIntBetween(0, 128);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("source").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode[]::new);
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin()
            .indices()
            .prepareUpdateSettings("source")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.require._name", discoveryNodes[0].getName())
                    .put("index.blocks.write", true)
            )
            .get();
        ensureGreen();

        final IndicesStatsResponse sourceStats = client().admin().indices().prepareStats("source").setSegments(true).get();

        // disable rebalancing to be able to capture the right stats. balancing can move the target primary
        // making it hard to pin point the source shards.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            )
            .get();

        // now merge source into a single shard index
        final boolean createWithReplicas = randomBoolean();
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("source", "target")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", createWithReplicas ? 1 : 0)
                        .putNull("index.blocks.write")
                        .putNull("index.routing.allocation.require._name")
                        .build()
                )
                .get()
        );
        ensureGreen();

        // resolve true merge node - this is not always the node we required as all shards may be on another node
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode mergeNode = state.nodes().get(state.getRoutingTable().index("target").shard(0).primaryShard().currentNodeId());
        logger.info("merge node {}", mergeNode);

        final long maxSeqNo = Arrays.stream(sourceStats.getShards())
            .filter(shard -> shard.getShardRouting().currentNodeId().equals(mergeNode.getId()))
            .map(ShardStats::getSeqNoStats)
            .mapToLong(SeqNoStats::getMaxSeqNo)
            .max()
            .getAsLong();
        final long maxUnsafeAutoIdTimestamp = Arrays.stream(sourceStats.getShards())
            .filter(shard -> shard.getShardRouting().currentNodeId().equals(mergeNode.getId()))
            .map(ShardStats::getStats)
            .map(CommonStats::getSegments)
            .mapToLong(SegmentsStats::getMaxUnsafeAutoIdTimestamp)
            .max()
            .getAsLong();

        final IndicesStatsResponse targetStats = client().admin().indices().prepareStats("target").get();
        for (final ShardStats shardStats : targetStats.getShards()) {
            final SeqNoStats seqNoStats = shardStats.getSeqNoStats();
            final ShardRouting shardRouting = shardStats.getShardRouting();
            assertThat("failed on " + shardRouting, seqNoStats.getMaxSeqNo(), equalTo(maxSeqNo));
            assertThat("failed on " + shardRouting, seqNoStats.getLocalCheckpoint(), equalTo(maxSeqNo));
            assertThat(
                "failed on " + shardRouting,
                shardStats.getStats().getSegments().getMaxUnsafeAutoIdTimestamp(),
                equalTo(maxUnsafeAutoIdTimestamp)
            );
        }

        final int size = docs > 0 ? 2 * docs : 1;
        assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);

        if (createWithReplicas == false) {
            // bump replicas
            client().admin()
                .indices()
                .prepareUpdateSettings("target")
                .setSettings(Settings.builder().put("index.number_of_replicas", 1))
                .get();
            ensureGreen();
            assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
        }

        for (int i = docs; i < 2 * docs; i++) {
            client().prepareIndex("target").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("target").setSize(2 * size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 2 * docs);
        assertHitCount(client().prepareSearch("source").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
        GetSettingsResponse target = client().admin().indices().prepareGetSettings("target").get();
        assertEquals(version, target.getIndexToSettings().get("target").getAsVersion("index.version.created", null));

        // clean up
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), (String) null)
            )
            .get();
    }

    /**
     * Tests that we can manually recover from a failed allocation due to shards being moved away etc.
     */
    public void testCreateShrinkIndexFails() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(
            Settings.builder().put(indexSettings()).put("number_of_shards", randomIntBetween(2, 7)).put("number_of_replicas", 0)
        ).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode[]::new);
        String spareNode = discoveryNodes[0].getName();
        String mergeNode = discoveryNodes[1].getName();
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin()
            .indices()
            .prepareUpdateSettings("source")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true))
            .get();
        ensureGreen();

        // now merge source into a single shard index
        client().admin()
            .indices()
            .prepareResizeIndex("source", "target")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.exclude._name", mergeNode) // we manually exclude the merge node to forcefully fuck it up
                    .put("index.number_of_replicas", 0)
                    .put("index.allocation.max_retries", 1)
                    .build()
            )
            .get();
        client().admin().cluster().prepareHealth("target").setWaitForEvents(Priority.LANGUID).get();

        // now we move all shards away from the merge node
        client().admin()
            .indices()
            .prepareUpdateSettings("source")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", spareNode).put("index.blocks.write", true))
            .get();
        ensureGreen("source");

        client().admin()
            .indices()
            .prepareUpdateSettings("target") // erase the forcefully fuckup!
            .setSettings(Settings.builder().putNull("index.routing.allocation.exclude._name"))
            .get();
        // wait until it fails
        assertBusy(() -> {
            ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
            RoutingTable routingTables = clusterStateResponse.getState().routingTable();
            assertTrue(routingTables.index("target").shard(0).shard(0).unassigned());
            assertEquals(
                UnassignedInfo.Reason.ALLOCATION_FAILED,
                routingTables.index("target").shard(0).shard(0).unassignedInfo().getReason()
            );
            assertEquals(1, routingTables.index("target").shard(0).shard(0).unassignedInfo().getNumFailedAllocations());
        });
        client().admin()
            .indices()
            .prepareUpdateSettings("source") // now relocate them all to the right node
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode))
            .get();
        ensureGreen("source");

        refreshClusterInfo();
        // kick off a retry and wait until it's done!
        ClusterRerouteResponse clusterRerouteResponse = client().admin().cluster().prepareReroute().setRetryFailed(true).get();
        long expectedShardSize = clusterRerouteResponse.getState().routingTable().index("target").shard(0).shard(0).getExpectedShardSize();
        // we support the expected shard size in the allocator to sum up over the source index shards
        assertTrue("expected shard size must be set but wasn't: " + expectedShardSize, expectedShardSize > 0);
        ensureGreen();
        assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
    }

    public void testCreateShrinkWithIndexSort() throws Exception {
        SortField expectedSortField = new SortedSetSortField("id", true, SortedSetSelector.Type.MAX);
        expectedSortField.setMissingValue(SortedSetSortField.STRING_FIRST);
        Sort expectedIndexSort = new Sort(expectedSortField);
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("sort.field", "id")
                .put("sort.order", "desc")
                .put("number_of_shards", 8)
                .put("number_of_replicas", 0)
        ).setMapping("id", "type=keyword,doc_values=true").get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source")
                .setId(Integer.toString(i))
                .setSource("{\"foo\" : \"bar\", \"id\" : " + i + "}", XContentType.JSON)
                .get();
        }
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode[]::new);
        String mergeNode = discoveryNodes[0].getName();
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();

        flushAndRefresh();
        assertSortedSegments("source", expectedIndexSort);

        // relocate all shards to one node such that we can merge it.
        client().admin()
            .indices()
            .prepareUpdateSettings("source")
            .setSettings(Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true))
            .get();
        ensureGreen();

        // check that index sort cannot be set on the target index
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareResizeIndex("source", "target")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", "2")
                        .put("index.sort.field", "foo")
                        .build()
                )
                .get()
        );
        assertThat(exc.getMessage(), containsString("can't override index sort when resizing an index"));

        // check that the index sort order of `source` is correctly applied to the `target`
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("source", "target")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", "2")
                        .putNull("index.blocks.write")
                        .build()
                )
                .get()
        );
        ensureGreen();
        flushAndRefresh();
        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings("target").execute().actionGet();
        assertEquals(settingsResponse.getSetting("target", "index.sort.field"), "id");
        assertEquals(settingsResponse.getSetting("target", "index.sort.order"), "desc");
        assertSortedSegments("target", expectedIndexSort);

        // ... and that the index sort is also applied to updates
        for (int i = 20; i < 40; i++) {
            client().prepareIndex("target").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        flushAndRefresh();
        assertSortedSegments("target", expectedIndexSort);
    }

    public void testShrinkCommitsMergeOnIdle() throws Exception {
        prepareCreate("source").setSettings(
            Settings.builder().put(indexSettings()).put("index.number_of_replicas", 0).put("number_of_shards", 5)
        ).get();
        for (int i = 0; i < 30; i++) {
            client().prepareIndex("source").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        client().admin().indices().prepareFlush("source").get();
        ImmutableOpenMap<String, DiscoveryNode> dataNodes = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getDataNodes();
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode[]::new);
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin()
            .indices()
            .prepareUpdateSettings("source")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.require._name", discoveryNodes[0].getName())
                    .put("index.blocks.write", true)
            )
            .get();
        ensureGreen();
        IndicesSegmentResponse sourceStats = client().admin().indices().prepareSegments("source").get();

        // disable rebalancing to be able to capture the right stats. balancing can move the target primary
        // making it hard to pin point the source shards.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            )
            .get();

        // now merge source into a single shard index
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("source", "target")
                .setSettings(Settings.builder().put("index.number_of_replicas", 0).build())
                .get()
        );
        ensureGreen();
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        IndexMetadata target = clusterStateResponse.getState().getMetadata().index("target");
        client().admin().indices().prepareForceMerge("target").setMaxNumSegments(1).setFlush(false).get();
        IndicesSegmentResponse targetSegStats = client().admin().indices().prepareSegments("target").get();
        ShardSegments segmentsStats = targetSegStats.getIndices().get("target").getShards().get(0).shards()[0];
        assertTrue(segmentsStats.getNumberOfCommitted() > 0);
        assertNotEquals(segmentsStats.getSegments(), segmentsStats.getNumberOfCommitted());

        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            if (service.hasIndex(target.getIndex())) {
                IndexService indexShards = service.indexService(target.getIndex());
                IndexShard shard = indexShards.getShard(0);
                assertTrue(shard.isActive());
                shard.flushOnIdle(0);
                assertFalse(shard.isActive());
            }
        }
        assertBusy(() -> {
            IndicesSegmentResponse targetStats = client().admin().indices().prepareSegments("target").get();
            ShardSegments targetShardSegments = targetStats.getIndices().get("target").getShards().get(0).shards()[0];
            Map<Integer, IndexShardSegments> source = sourceStats.getIndices().get("source").getShards();
            int numSourceSegments = 0;
            for (IndexShardSegments s : source.values()) {
                numSourceSegments += s.getAt(0).getNumberOfCommitted();
            }
            assertTrue(targetShardSegments.getSegments().size() < numSourceSegments);
            assertEquals(targetShardSegments.getNumberOfCommitted(), targetShardSegments.getNumberOfSearch());
            assertEquals(targetShardSegments.getNumberOfCommitted(), targetShardSegments.getSegments().size());
            assertEquals(1, targetShardSegments.getSegments().size());
        });

        // clean up
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), (String) null)
            )
            .get();
    }

    public void testShrinkThenSplitWithFailedNode() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        final String shrinkNode = internalCluster().startDataOnlyNode();

        final int shardCount = between(2, 5);
        prepareCreate("original").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
        ).get();
        client().admin().indices().prepareFlush("original").get();
        ensureGreen();
        client().admin()
            .indices()
            .prepareUpdateSettings("original")
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), shrinkNode)
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
            )
            .get();
        ensureGreen();

        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("original", "shrunk")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey())
                        .build()
                )
                .setResizeType(ResizeType.SHRINK)
                .get()
        );
        ensureGreen();

        final int nodeCount = cluster().size();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(shrinkNode));
        ensureStableCluster(nodeCount - 1);

        // demonstrate that the index.routing.allocation.initial_recovery setting from the shrink doesn't carry over into the split index,
        // because this would cause the shrink to fail as the initial_recovery node is no longer present.

        logger.info("--> executing split");
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("shrunk", "splitagain")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
                        .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey())
                        .build()
                )
                .setResizeType(ResizeType.SPLIT)
        );
        ensureGreen("splitagain");
    }
}
