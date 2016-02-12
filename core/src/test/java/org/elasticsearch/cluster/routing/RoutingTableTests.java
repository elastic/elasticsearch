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
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESAllocationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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
    private final AllocationService ALLOCATION_SERVICE = createAllocationService(settingsBuilder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE) // don't limit recoveries
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", Integer.MAX_VALUE)
            .build());
    private ClusterState clusterState;

    private final Map<String, long[]> primaryTermsPerIndex = new HashMap<>();
    private final Map<String, long[]> versionsPerIndex = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.numberOfShards = randomIntBetween(1, 5);
        this.numberOfReplicas = randomIntBetween(1, 5);
        this.shardsPerIndex = this.numberOfShards * (this.numberOfReplicas + 1);
        this.totalNumberOfShards = this.shardsPerIndex * 2;
        logger.info("Setup test with " + this.numberOfShards + " shards and " + this.numberOfReplicas + " replicas.");
        this.emptyRoutingTable = new RoutingTable.Builder().build();
        this.primaryTermsPerIndex.clear();
        MetaData metaData = MetaData.builder()
                .put(createIndexMetaData(TEST_INDEX_1))
                .put(createIndexMetaData(TEST_INDEX_2))
                .build();

        this.testRoutingTable = new RoutingTable.Builder()
                .add(new IndexRoutingTable.Builder(metaData.index(TEST_INDEX_1).getIndex()).initializeAsNew(metaData.index(TEST_INDEX_1)).build())
                .add(new IndexRoutingTable.Builder(metaData.index(TEST_INDEX_2).getIndex()).initializeAsNew(metaData.index(TEST_INDEX_2)).build())
                .build();
        this.versionsPerIndex.clear();
        this.versionsPerIndex.put(TEST_INDEX_1, new long[numberOfShards]);
        this.versionsPerIndex.put(TEST_INDEX_2, new long[numberOfShards]);

        this.clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(testRoutingTable).build();
    }

    /**
     * puts primary shard routings into initializing state
     */
    private void initPrimaries() {
        logger.info("adding " + (this.numberOfReplicas + 1) + " nodes and performing rerouting");
        Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < this.numberOfReplicas + 1; i++) {
            discoBuilder = discoBuilder.put(newNode("node" + i));
        }
        this.clusterState = ClusterState.builder(clusterState).nodes(discoBuilder).build();
        RoutingAllocation.Result rerouteResult = ALLOCATION_SERVICE.reroute(clusterState, "reroute");
        this.testRoutingTable = rerouteResult.routingTable();
        assertThat(rerouteResult.changed(), is(true));
        applyRerouteResult(rerouteResult);
        versionsPerIndex.keySet().forEach(this::incrementVersion);
        primaryTermsPerIndex.keySet().forEach(this::incrementPrimaryTerm);
    }

    private void incrementVersion(String index) {
        final long[] versions = versionsPerIndex.get(index);
        for (int i = 0; i < versions.length; i++) {
            versions[i]++;
        }
    }

    private void incrementVersion(String index, int shard) {
        versionsPerIndex.get(index)[shard]++;
    }

    private void incrementPrimaryTerm(String index) {
        final long[] primaryTerms = primaryTermsPerIndex.get(index);
        for (int i = 0; i < primaryTerms.length; i++) {
            primaryTerms[i]++;
        }
    }

    private void incrementPrimaryTerm(String index, int shard) {
        primaryTermsPerIndex.get(index)[shard]++;
    }

    private void startInitializingShards(String index) {
        this.clusterState = ClusterState.builder(clusterState).routingTable(this.testRoutingTable).build();
        logger.info("start primary shards for index " + index);
        RoutingAllocation.Result rerouteResult = ALLOCATION_SERVICE.applyStartedShards(this.clusterState, this.clusterState.getRoutingNodes().shardsWithState(index, INITIALIZING));
        // TODO: this simulate the code in InternalClusterState.UpdateTask.run() we should unify this.
        applyRerouteResult(rerouteResult);
        incrementVersion(index);
    }

    private void applyRerouteResult(RoutingAllocation.Result rerouteResult) {
        ClusterState previousClusterState = this.clusterState;
        ClusterState newClusterState = ClusterState.builder(previousClusterState).routingResult(rerouteResult).build();
        ClusterState.Builder builder = ClusterState.builder(newClusterState).incrementVersion();
        if (previousClusterState.routingTable() != newClusterState.routingTable()) {
            builder.routingTable(RoutingTable.builder(newClusterState.routingTable()).version(newClusterState.routingTable().version() + 1).build());
        }
        if (previousClusterState.metaData() != newClusterState.metaData()) {
            builder.metaData(MetaData.builder(newClusterState.metaData()).version(newClusterState.metaData().version() + 1));
        }
        this.clusterState = builder.build();
        this.testRoutingTable = rerouteResult.routingTable();
    }

    private void failSomePrimaries(String index) {
        this.clusterState = ClusterState.builder(clusterState).routingTable(this.testRoutingTable).build();
        final IndexRoutingTable indexShardRoutingTable = testRoutingTable.index(index);
        Set<Integer> shardIdsToFail = new HashSet<>();
        for (int i = 1 + randomInt(numberOfShards - 1); i > 0; i--) {
            shardIdsToFail.add(randomInt(numberOfShards - 1));
        }
        logger.info("failing primary shards {} for index [{}]", shardIdsToFail, index);
        List<FailedRerouteAllocation.FailedShard> failedShards = new ArrayList<>();
        for (int shard : shardIdsToFail) {
            failedShards.add(new FailedRerouteAllocation.FailedShard(indexShardRoutingTable.shard(shard).primaryShard(), "test", null));
            incrementPrimaryTerm(index, shard); // the primary failure should increment the primary term;
            incrementVersion(index, shard); // version is incremented once when the primary is unassigned
            incrementVersion(index, shard); // and another time when the primary flag is set to false
        }
        RoutingAllocation.Result rerouteResult = ALLOCATION_SERVICE.applyFailedShards(this.clusterState, failedShards);
        applyRerouteResult(rerouteResult);
    }

    private IndexMetaData.Builder createIndexMetaData(String indexName) {
        primaryTermsPerIndex.put(indexName, new long[numberOfShards]);
        final IndexMetaData.Builder builder = new IndexMetaData.Builder(indexName)
                .settings(DEFAULT_SETTINGS)
                .numberOfReplicas(this.numberOfReplicas)
                .numberOfShards(this.numberOfShards);
        for (int i = 0; i < numberOfShards; i++) {
            builder.primaryTerm(i, randomInt(200));
            primaryTermsPerIndex.get(indexName)[i] = builder.primaryTerm(i);
        }
        return builder;
    }

    private void assertAllPrimaryTerm() {
        versionsPerIndex.keySet().forEach(this::assertPrimaryTerm);
    }

    private void assertPrimaryTerm(String index) {
        final long[] terms = primaryTermsPerIndex.get(index);
        final IndexMetaData indexMetaData = clusterState.metaData().index(index);
        for (IndexShardRoutingTable shardRoutingTable : this.testRoutingTable.index(index)) {
            final int shard = shardRoutingTable.shardId().id();
            for (ShardRouting routing : shardRoutingTable) {
                assertThat("wrong primary term in " + routing, routing.primaryTerm(), equalTo(terms[shard]));
            }
            assertThat("primary term mismatch between indexMetaData of [" + index + "] and shard [" + shard + "]'s routing", indexMetaData.primaryTerm(shard), equalTo(terms[shard]));
        }
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

    public void testVersionAndPrimaryTermNormalization() {
        assertAllPrimaryTerm();

        initPrimaries();
        assertAllPrimaryTerm();

        startInitializingShards(TEST_INDEX_1);
        assertAllPrimaryTerm();

        startInitializingShards(TEST_INDEX_2);
        assertAllPrimaryTerm();

        // now start all replicas too
        startInitializingShards(TEST_INDEX_1);
        startInitializingShards(TEST_INDEX_2);
        assertAllPrimaryTerm();

        failSomePrimaries(TEST_INDEX_1);
        assertAllPrimaryTerm();
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
}
