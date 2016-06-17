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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Before;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RoutingTableTests extends ESAllocationTestCase {

    private static final String TEST_INDEX_1 = "test1";
    private static final String TEST_INDEX_2 = "test2";
    private RoutingTable emptyRoutingTable;
    private RoutingTable testRoutingTable;
    private int numberOfShards;
    private int numberOfReplicas;
    private int shardsPerIndex;
    private int totalNumberOfShards;
    private final static Settings DEFAULT_SETTINGS = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
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

        this.testRoutingTable = new RoutingTable.Builder()
                .add(new IndexRoutingTable.Builder(metaData.index(TEST_INDEX_1).getIndex()).initializeAsNew(metaData.index(TEST_INDEX_1)).build())
                .add(new IndexRoutingTable.Builder(metaData.index(TEST_INDEX_2).getIndex()).initializeAsNew(metaData.index(TEST_INDEX_2)).build())
                .build();
        this.clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(testRoutingTable).build();
    }

    /**
     * puts primary shard routings into initializing state
     */
    private void initPrimaries() {
        logger.info("adding {} nodes and performing rerouting", this.numberOfReplicas + 1);
        Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < this.numberOfReplicas + 1; i++) {
            discoBuilder = discoBuilder.put(newNode("node" + i));
        }
        this.clusterState = ClusterState.builder(clusterState).nodes(discoBuilder).build();
        RoutingAllocation.Result rerouteResult = ALLOCATION_SERVICE.reroute(clusterState, "reroute");
        this.testRoutingTable = rerouteResult.routingTable();
        assertThat(rerouteResult.changed(), is(true));
        this.clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
    }

    private void startInitializingShards(String index) {
        this.clusterState = ClusterState.builder(clusterState).routingTable(this.testRoutingTable).build();
        logger.info("start primary shards for index {}", index);
        RoutingAllocation.Result rerouteResult = ALLOCATION_SERVICE.applyStartedShards(this.clusterState, this.clusterState.getRoutingNodes().shardsWithState(index, INITIALIZING));
        this.clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        this.testRoutingTable = rerouteResult.routingTable();
    }

    private IndexMetaData.Builder createIndexMetaData(String indexName) {
        return new IndexMetaData.Builder(indexName)
                .settings(DEFAULT_SETTINGS)
                .numberOfReplicas(this.numberOfReplicas)
                .numberOfShards(this.numberOfShards);
    }

    public void testAllShards() {
        assertThat(this.emptyRoutingTable.allShards().size(), is(0));
        assertThat(this.testRoutingTable.allShards().size(), is(this.totalNumberOfShards));

        assertThat(this.testRoutingTable.allShards(TEST_INDEX_1).size(), is(this.shardsPerIndex));
        try {
            assertThat(this.testRoutingTable.allShards("not_existing").size(), is(0));
            fail("Exception expected when calling allShards() with non existing index name");
        } catch (IndexNotFoundException e) {
            // expected
        }
    }

    public void testHasIndex() {
        assertThat(this.testRoutingTable.hasIndex(TEST_INDEX_1), is(true));
        assertThat(this.testRoutingTable.hasIndex("foobar"), is(false));
    }

    public void testIndex() {
        assertThat(this.testRoutingTable.index(TEST_INDEX_1).getIndex().getName(), is(TEST_INDEX_1));
        assertThat(this.testRoutingTable.index("foobar"), is(nullValue()));
    }

    public void testIndicesRouting() {
        assertThat(this.testRoutingTable.indicesRouting().size(), is(2));
        assertThat(this.testRoutingTable.getIndicesRouting().size(), is(2));
        assertSame(this.testRoutingTable.getIndicesRouting(), this.testRoutingTable.indicesRouting());
    }

    public void testShardsWithState() {
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards));

        initPrimaries();
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards - 2 * this.numberOfShards));
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.INITIALIZING).size(), is(2 * this.numberOfShards));

        startInitializingShards(TEST_INDEX_1);
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.STARTED).size(), is(this.numberOfShards));
        int initializingExpected = this.numberOfShards + this.numberOfShards * this.numberOfReplicas;
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected));
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards - initializingExpected - this.numberOfShards));

        startInitializingShards(TEST_INDEX_2);
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.STARTED).size(), is(2 * this.numberOfShards));
        initializingExpected = 2 * this.numberOfShards * this.numberOfReplicas;
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.INITIALIZING).size(), is(initializingExpected));
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.UNASSIGNED).size(), is(this.totalNumberOfShards - initializingExpected - 2 * this.numberOfShards));

        // now start all replicas too
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_2);
        assertThat(this.testRoutingTable.shardsWithState(ShardRoutingState.STARTED).size(), is(this.totalNumberOfShards));
    }

    public void testActivePrimaryShardsGrouped() {
        assertThat(this.emptyRoutingTable.activePrimaryShardsGrouped(new String[0], true).size(), is(0));
        assertThat(this.emptyRoutingTable.activePrimaryShardsGrouped(new String[0], false).size(), is(0));

        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, true).size(), is(this.numberOfShards));

        initPrimaries();
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, true).size(), is(this.numberOfShards));

        startInitializingShards(TEST_INDEX_1);
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(this.numberOfShards));
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(this.numberOfShards));
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1}, true).size(), is(this.numberOfShards));

        startInitializingShards(TEST_INDEX_2);
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_2}, false).size(), is(this.numberOfShards));
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(2 * this.numberOfShards));
        assertThat(this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1, TEST_INDEX_2}, true).size(), is(2 * this.numberOfShards));

        try {
            this.testRoutingTable.activePrimaryShardsGrouped(new String[]{TEST_INDEX_1, "not_exists"}, true);
            fail("Calling with non-existing index name should raise IndexMissingException");
        } catch (IndexNotFoundException e) {
            // expected
        }
    }

    public void testAllActiveShardsGrouped() {
        assertThat(this.emptyRoutingTable.allActiveShardsGrouped(new String[0], true).size(), is(0));
        assertThat(this.emptyRoutingTable.allActiveShardsGrouped(new String[0], false).size(), is(0));

        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        initPrimaries();
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        startInitializingShards(TEST_INDEX_1);
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(this.numberOfShards));
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(this.numberOfShards));
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        startInitializingShards(TEST_INDEX_2);
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_2}, false).size(), is(this.numberOfShards));
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(2 * this.numberOfShards));
        assertThat(this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1, TEST_INDEX_2}, true).size(), is(this.totalNumberOfShards));

        try {
            this.testRoutingTable.allActiveShardsGrouped(new String[]{TEST_INDEX_1, "not_exists"}, true);
        } catch (IndexNotFoundException e) {
            fail("Calling with non-existing index should be ignored at the moment");
        }
    }

    public void testAllAssignedShardsGrouped() {
        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(0));
        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        initPrimaries();
        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[]{TEST_INDEX_1}, false).size(), is(this.numberOfShards));
        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[]{TEST_INDEX_1}, true).size(), is(this.shardsPerIndex));

        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[]{TEST_INDEX_1, TEST_INDEX_2}, false).size(), is(2 * this.numberOfShards));
        assertThat(this.testRoutingTable.allAssignedShardsGrouped(new String[]{TEST_INDEX_1, TEST_INDEX_2}, true).size(), is(this.totalNumberOfShards));

        try {
            this.testRoutingTable.allAssignedShardsGrouped(new String[]{TEST_INDEX_1, "not_exists"}, false);
        } catch (IndexNotFoundException e) {
            fail("Calling with non-existing index should be ignored at the moment");
        }
    }

    public void testAllShardsForMultipleIndices() {
        assertThat(this.emptyRoutingTable.allShards(new String[0]).size(), is(0));

        assertThat(this.testRoutingTable.allShards(new String[]{TEST_INDEX_1}).size(), is(this.shardsPerIndex));

        initPrimaries();
        assertThat(this.testRoutingTable.allShards(new String[]{TEST_INDEX_1}).size(), is(this.shardsPerIndex));

        startInitializingShards(TEST_INDEX_1);
        assertThat(this.testRoutingTable.allShards(new String[]{TEST_INDEX_1}).size(), is(this.shardsPerIndex));

        startInitializingShards(TEST_INDEX_2);
        assertThat(this.testRoutingTable.allShards(new String[]{TEST_INDEX_1, TEST_INDEX_2}).size(), is(this.totalNumberOfShards));

        try {
            this.testRoutingTable.allShards(new String[]{TEST_INDEX_1, "not_exists"});
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
            b.updateNumberOfReplicas(1, "foo");
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
        MetaData metaData = MetaData.builder().put(indexMetaData, true).build();
        final RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        final RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        final IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetaData, counter);
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
}
