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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class ShrinkIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class);
    }

    public void testCreateShrinkIndexToN() {
        int[][] possibleShardSplits = new int[][] {{8,4,2}, {9, 3, 1}, {4, 2, 1}, {15,5,1}};
        int[] shardSplits = randomFrom(possibleShardSplits);
        assertEquals(shardSplits[0], (shardSplits[0] / shardSplits[1]) * shardSplits[1]);
        assertEquals(shardSplits[1], (shardSplits[1] / shardSplits[2]) * shardSplits[2]);
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings()).put("number_of_shards", shardSplits[0])).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source", "t1", Integer.toString(i))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
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
            client().prepareIndex("first_shrink", "t1", Integer.toString(i))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
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
            client().prepareIndex("second_shrink", "t1", Integer.toString(i))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("second_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("first_shrink").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
    }

    public void testCreateShrinkIndex() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Version version = VersionUtils.randomVersion(random());
        prepareCreate("source").setSettings(Settings.builder().put(indexSettings())
            .put("number_of_shards", randomIntBetween(2, 7))
            .put("index.version.created", version)
        ).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("source", randomFrom("t1", "t2", "t3"))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
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

        final boolean createWithReplicas = randomBoolean();
        assertAcked(client().admin().indices().prepareShrinkIndex("source", "target")
            .setSettings(Settings.builder().put("index.number_of_replicas", createWithReplicas ? 1 : 0).build()).get());
        ensureGreen();
        assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);

        if (createWithReplicas == false) {
            // bump replicas
            client().admin().indices().prepareUpdateSettings("target")
                .setSettings(Settings.builder()
                    .put("index.number_of_replicas", 1)).get();
            ensureGreen();
            assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        }

        for (int i = 20; i < 40; i++) {
            client().prepareIndex("target", randomFrom("t1", "t2", "t3"))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("target").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 40);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), 20);
        GetSettingsResponse target = client().admin().indices().prepareGetSettings("target").get();
        assertEquals(version, target.getIndexToSettings().get("target").getAsVersion("index.version.created", null));
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
            client().prepareIndex("source", randomFrom("t1", "t2", "t3"))
                .setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", XContentType.JSON).get();
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
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(Settings.builder()
                .put("index.routing.allocation.exclude._name", mergeNode) // we manually exclude the merge node to forcefully fuck it up
                .put("index.number_of_replicas", 0)
                .put("index.allocation.max_retries", 1).build()).get();
        client().admin().cluster().prepareHealth("target").setWaitForEvents(Priority.LANGUID).get();

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
}
