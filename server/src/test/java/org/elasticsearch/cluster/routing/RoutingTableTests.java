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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Before;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class RoutingTableTests extends ESAllocationTestCase {

    private static final String TEST_INDEX_1 = "test1";
    private static final String TEST_INDEX_2 = "test2";
    private RoutingTable emptyRoutingTable;
    private int numberOfShards;
    private int numberOfReplicas;
    private int shardsPerIndex;
    private int totalNumberOfShards;
    private static final Settings DEFAULT_SETTINGS = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
    private final AllocationService ALLOCATION_SERVICE = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE) // don't limit recoveries
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", Integer.MAX_VALUE)
            .build());
    private ClusterState clusterState;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.numberOfShards = randomIntBetween(1, 5);
        this.numberOfReplicas = randomIntBetween(1, 5);
        this.shardsPerIndex = this.numberOfShards * (this.numberOfReplicas + 1);
        this.totalNumberOfShards = this.shardsPerIndex * 2;
        logger.info("Setup test with {} shards and {} replicas.", this.numberOfShards, this.numberOfReplicas);
        this.emptyRoutingTable = new RoutingTable.Builder().build();
        MetaData metaData = MetaData.builder()
                .put(createIndexMetaData(TEST_INDEX_1))
                .put(createIndexMetaData(TEST_INDEX_2))
                .build();

        RoutingTable testRoutingTable = new RoutingTable.Builder()
                .add(new IndexRoutingTable.Builder(metaData.index(TEST_INDEX_1).
                    getIndex()).initializeAsNew(metaData.index(TEST_INDEX_1)).build())
                .add(new IndexRoutingTable.Builder(metaData.index(TEST_INDEX_2)
                    .getIndex()).initializeAsNew(metaData.index(TEST_INDEX_2)).build())
                .build();
        this.clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(testRoutingTable).build();
    }

    /**
     * puts primary shard indexRoutings into initializing state
     */
    private void initPrimaries() {
        logger.info("adding {} nodes and performing rerouting", this.numberOfReplicas + 1);
        Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < this.numberOfReplicas + 1; i++) {
            discoBuilder = discoBuilder.add(newNode("node" + i));
        }
        this.clusterState = ClusterState.builder(clusterState).nodes(discoBuilder).build();
        ClusterState rerouteResult = ALLOCATION_SERVICE.reroute(clusterState, "reroute");
        assertThat(rerouteResult, not(equalTo(this.clusterState)));
        this.clusterState = rerouteResult;
    }

    private void startInitializingShards(String index) {
        logger.info("start primary shards for index {}", index);
        clusterState = startInitializingShardsAndReroute(ALLOCATION_SERVICE, clusterState, index);
    }

    private IndexMetaData.Builder createIndexMetaData(String indexName) {
        return new IndexMetaData.Builder(indexName)
                .settings(DEFAULT_SETTINGS)
                .numberOfReplicas(this.numberOfReplicas)
                .numberOfShards(this.numberOfShards);
    }

    public void testAllShards() {
        assertThat(this.emptyRoutingTable.allShards().size(), is(0));
        assertThat(this.clusterState.routingTable().allShards().size(), is(this.totalNumberOfShards));

        assertThat(this.clusterState.routingTable().allShards(TEST_INDEX_1).size(), is(this.shardsPerIndex));
        try {
            assertThat(this.clusterState.routingTable().allShards("not_existing").size(), is(0));
            fail("Exception expected when calling allShards() with non existing index name");
        } catch (IndexNotFoundException e) {
            // expected
        }
    }

    public void testHasIndex() {
        assertThat(clusterState.routingTable().hasIndex(TEST_INDEX_1), is(true));
        assertThat(clusterState.routingTable().hasIndex("foobar"), is(false));
    }

    public void testIndex() {
        assertThat(clusterState.routingTable().index(TEST_INDEX_1).getIndex().getName(), is(TEST_INDEX_1));
        assertThat(clusterState.routingTable().index("foobar"), is(nullValue()));
    }

    public void testIndicesRouting() {
        assertThat(clusterState.routingTable().indicesRouting().size(), is(2));
        assertThat(clusterState.routingTable().getIndicesRouting().size(), is(2));
        assertSame(clusterState.routingTable().getIndicesRouting(), clusterState.routingTable().indicesRouting());
    }

    public void testShardsWithState() {
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));

        initPrimaries();
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - 2 * this.numberOfShards));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));

        startInitializingShards(TEST_INDEX_1);
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(this.numberOfShards));
        int initializingExpected = this.numberOfShards + this.numberOfShards * this.numberOfReplicas;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - initializingExpected - this.numberOfShards));

        startInitializingShards(TEST_INDEX_2);
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(2 * this.numberOfShards));
        initializingExpected = 2 * this.numberOfShards * this.numberOfReplicas;
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected));
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size(),
            is(this.totalNumberOfShards - initializingExpected - 2 * this.numberOfShards));

        // now start all replicas too
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_2);
        assertThat(clusterState.routingTable().shardsWithState(ShardRoutingState.STARTED).size(), is(this.totalNumberOfShards));
    }

    public void testActivePrimaryShardsGrouped() {
        assertThat(this.emptyRoutingTable.activePrimaryShardsGrouped(new String[0], true).size(), is(0));
        assertThat(this.emptyRoutingTable.activePrimaryShardsGrouped(new String[0], false).size(), is(0));

        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, true).size(),
            is(this.numberOfShards));

        initPrimaries();
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, true).size(),
            is(this.numberOfShards));

        startInitializingShards(TEST_INDEX_1);
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, false).size(),
            is(this.numberOfShards));
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(
            new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(this.numberOfShards));
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(
            new String[]{TEST_INDEX_1}, true).size(), is(this.numberOfShards));

        startInitializingShards(TEST_INDEX_2);
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(
            new String[]{TEST_INDEX_2}, false).size(), is(this.numberOfShards));
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(
            new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(2 * this.numberOfShards));
        assertThat(clusterState.routingTable().activePrimaryShardsGrouped(
            new String[]{TEST_INDEX_1, TEST_INDEX_2}, true).size(), is(2 * this.numberOfShards));

        try {
            clusterState.routingTable().activePrimaryShardsGrouped(new String[]{TEST_INDEX_1, "not_exists"}, true);
            fail("Calling with non-existing index name should raise IndexMissingException");
        } catch (IndexNotFoundException e) {
            // expected
        }
    }

    public void testAllActiveShardsGrouped() {
        assertThat(this.emptyRoutingTable.allActiveShardsGrouped(new String[0], true).size(), is(0));
        assertThat(this.emptyRoutingTable.allActiveShardsGrouped(new String[0], false).size(), is(0));

        assertThat(clusterState.routingTable().allActiveShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(clusterState.routingTable().allActiveShardsGrouped(
            new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        initPrimaries();
        assertThat(clusterState.routingTable().allActiveShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(clusterState.routingTable().allActiveShardsGrouped(
            new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        startInitializingShards(TEST_INDEX_1);
        assertThat(clusterState.routingTable().allActiveShardsGrouped(
            new String[]{TEST_INDEX_1}, false).size(), is(this.numberOfShards));
        assertThat(clusterState.routingTable().allActiveShardsGrouped(
            new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(this.numberOfShards));
        assertThat(clusterState.routingTable().allActiveShardsGrouped(
            new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        startInitializingShards(TEST_INDEX_2);
        assertThat(clusterState.routingTable().allActiveShardsGrouped(
            new String[]{TEST_INDEX_2}, false).size(), is(this.numberOfShards));
        assertThat(clusterState.routingTable().allActiveShardsGrouped(
            new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(2 * this.numberOfShards));
        assertThat(clusterState.routingTable().allActiveShardsGrouped(
            new String[]{TEST_INDEX_1, TEST_INDEX_2}, true).size(), is(this.totalNumberOfShards));

        try {
            clusterState.routingTable().allActiveShardsGrouped(new String[]{TEST_INDEX_1, "not_exists"}, true);
        } catch (IndexNotFoundException e) {
            fail("Calling with non-existing index should be ignored at the moment");
        }
    }

    public void testAllAssignedShardsGrouped() {
        assertThat(clusterState.routingTable().allAssignedShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(clusterState.routingTable().allAssignedShardsGrouped(
            new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        initPrimaries();
        assertThat(clusterState.routingTable().allAssignedShardsGrouped(
            new String[]{TEST_INDEX_1}, false).size(), is(this.numberOfShards));
        assertThat(clusterState.routingTable().allAssignedShardsGrouped(
            new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        assertThat(clusterState.routingTable().allAssignedShardsGrouped(
            new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(2 * this.numberOfShards));
        assertThat(clusterState.routingTable().allAssignedShardsGrouped(
            new String[]{TEST_INDEX_1, TEST_INDEX_2}, true).size(), is(this.totalNumberOfShards));

        try {
            clusterState.routingTable().allAssignedShardsGrouped(new String[]{TEST_INDEX_1, "not_exists"}, false);
        } catch (IndexNotFoundException e) {
            fail("Calling with non-existing index should be ignored at the moment");
        }
    }

    public void testAllShardsForMultipleIndices() {
        assertThat(this.emptyRoutingTable.allShards(new String[0]).size(), is(0));

        assertThat(clusterState.routingTable().allShards(new String[]{TEST_INDEX_1}).size(), is(this.shardsPerIndex));

        initPrimaries();
        assertThat(clusterState.routingTable().allShards(new String[]{TEST_INDEX_1}).size(), is(this.shardsPerIndex));

        startInitializingShards(TEST_INDEX_1);
        assertThat(clusterState.routingTable().allShards(new String[]{TEST_INDEX_1}).size(), is(this.shardsPerIndex));

        startInitializingShards(TEST_INDEX_2);
        assertThat(clusterState.routingTable().allShards(new String[]{TEST_INDEX_1, TEST_INDEX_2}).size(), is(this.totalNumberOfShards));

        try {
            clusterState.routingTable().allShards(new String[]{TEST_INDEX_1, "not_exists"});
        } catch (IndexNotFoundException e) {
            fail("Calling with non-existing index should be ignored at the moment");
        }
    }

    public void testRoutingTableBuiltMoreThanOnce() {
        RoutingTable.Builder b = RoutingTable.builder();
        b.build(); // Ok the first time
        try {
            b.build();
            fail("expected exception");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("cannot be reused"));
        }
        try {
            b.add((IndexRoutingTable) null);
            fail("expected exception");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("cannot be reused"));
        }
        try {
            b.updateNumberOfReplicas(1, new String[]{"foo"});
            fail("expected exception");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("cannot be reused"));
        }
        try {
            b.remove("foo");
            fail("expected exception");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), containsString("cannot be reused"));
        }

    }

    public void testValidations() {
        final String indexName = "test";
        final int numShards = 1;
        final int numReplicas = randomIntBetween(0, 1);
        IndexMetaData indexMetaData = IndexMetaData.builder(indexName)
                                                   .settings(settings(Version.CURRENT))
                                                   .numberOfShards(numShards)
                                                   .numberOfReplicas(numReplicas)
                                                   .build();
        final RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        final RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        final IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetaData, counter);
        indexMetaData = updateActiveAllocations(indexRoutingTable, indexMetaData);
        MetaData metaData = MetaData.builder().put(indexMetaData, true).build();
        // test no validation errors
        assertTrue(indexRoutingTable.validate(metaData));
        // test wrong number of shards causes validation errors
        indexMetaData = IndexMetaData.builder(indexName)
                                     .settings(settings(Version.CURRENT))
                                     .numberOfShards(numShards + 1)
                                     .numberOfReplicas(numReplicas)
                                     .build();
        final MetaData metaData2 = MetaData.builder().put(indexMetaData, true).build();
        expectThrows(IllegalStateException.class, () -> indexRoutingTable.validate(metaData2));
        // test wrong number of replicas causes validation errors
        indexMetaData = IndexMetaData.builder(indexName)
                                     .settings(settings(Version.CURRENT))
                                     .numberOfShards(numShards)
                                     .numberOfReplicas(numReplicas + 1)
                                     .build();
        final MetaData metaData3 = MetaData.builder().put(indexMetaData, true).build();
        expectThrows(IllegalStateException.class, () -> indexRoutingTable.validate(metaData3));
        // test wrong number of shards and replicas causes validation errors
        indexMetaData = IndexMetaData.builder(indexName)
                                     .settings(settings(Version.CURRENT))
                                     .numberOfShards(numShards + 1)
                                     .numberOfReplicas(numReplicas + 1)
                                     .build();
        final MetaData metaData4 = MetaData.builder().put(indexMetaData, true).build();
        expectThrows(IllegalStateException.class, () -> indexRoutingTable.validate(metaData4));
    }

    public void testDistinctNodes() {
        ShardId shardId = new ShardId(new Index("index", "uuid"), 0);
        ShardRouting routing1 = TestShardRouting.newShardRouting(shardId, "node1", randomBoolean(), ShardRoutingState.STARTED);
        ShardRouting routing2 = TestShardRouting.newShardRouting(shardId, "node2", randomBoolean(), ShardRoutingState.STARTED);
        ShardRouting routing3 = TestShardRouting.newShardRouting(shardId, "node1", randomBoolean(), ShardRoutingState.STARTED);
        ShardRouting routing4 = TestShardRouting.newShardRouting(
            shardId, "node3", "node2", randomBoolean(), ShardRoutingState.RELOCATING);
        assertTrue(IndexShardRoutingTable.Builder.distinctNodes(Arrays.asList(routing1, routing2)));
        assertFalse(IndexShardRoutingTable.Builder.distinctNodes(Arrays.asList(routing1, routing3)));
        assertFalse(IndexShardRoutingTable.Builder.distinctNodes(Arrays.asList(routing1, routing2, routing3)));
        assertTrue(IndexShardRoutingTable.Builder.distinctNodes(Arrays.asList(routing1, routing4)));
        assertFalse(IndexShardRoutingTable.Builder.distinctNodes(Arrays.asList(routing2, routing4)));
    }

    public void testAddAsRecovery() {
        {
            final IndexMetaData indexMetaData = createIndexMetaData(TEST_INDEX_1).state(IndexMetaData.State.OPEN).build();
            final RoutingTable routingTable = new RoutingTable.Builder().addAsRecovery(indexMetaData).build();
            assertThat(routingTable.hasIndex(TEST_INDEX_1), is(true));
            assertThat(routingTable.allShards(TEST_INDEX_1).size(), is(this.shardsPerIndex));
            assertThat(routingTable.index(TEST_INDEX_1).shardsWithState(UNASSIGNED).size(), is(this.shardsPerIndex));
        }
        {
            final IndexMetaData indexMetaData = createIndexMetaData(TEST_INDEX_1).state(IndexMetaData.State.CLOSE).build();
            final RoutingTable routingTable = new RoutingTable.Builder().addAsRecovery(indexMetaData).build();
            assertThat(routingTable.hasIndex(TEST_INDEX_1), is(false));
            expectThrows(IndexNotFoundException.class, () -> routingTable.allShards(TEST_INDEX_1));
        }
        {
            final IndexMetaData indexMetaData = createIndexMetaData(TEST_INDEX_1).build();
            final IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData)
                .state(IndexMetaData.State.CLOSE)
                .settings(Settings.builder()
                    .put(indexMetaData.getSettings())
                    .put(MetaDataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), true)
                    .build())
                .settingsVersion(indexMetaData.getSettingsVersion() + 1);
            final RoutingTable routingTable = new RoutingTable.Builder().addAsRecovery(indexMetaDataBuilder.build()).build();
            assertThat(routingTable.hasIndex(TEST_INDEX_1), is(true));
            assertThat(routingTable.allShards(TEST_INDEX_1).size(), is(this.shardsPerIndex));
            assertThat(routingTable.index(TEST_INDEX_1).shardsWithState(UNASSIGNED).size(), is(this.shardsPerIndex));
        }
    }

    /** reverse engineer the in sync aid based on the given indexRoutingTable **/
    public static IndexMetaData updateActiveAllocations(IndexRoutingTable indexRoutingTable, IndexMetaData indexMetaData) {
        IndexMetaData.Builder imdBuilder = IndexMetaData.builder(indexMetaData);
        for (IndexShardRoutingTable shardTable : indexRoutingTable) {
            for (ShardRouting shardRouting : shardTable) {
                Set<String> insyncAids = shardTable.activeShards().stream().map(
                    shr -> shr.allocationId().getId()).collect(Collectors.toSet());
                final ShardRouting primaryShard = shardTable.primaryShard();
                if (primaryShard.initializing() && primaryShard.recoverySource().getType() == RecoverySource.Type.EXISTING_STORE) {
                    // simulate a primary was initialized based on aid
                    insyncAids.add(primaryShard.allocationId().getId());
                }
                imdBuilder.putInSyncAllocationIds(shardRouting.id(), insyncAids);
            }
        }
        return imdBuilder.build();
    }
}
