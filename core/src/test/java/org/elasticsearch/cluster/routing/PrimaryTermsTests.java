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
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESAllocationTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PrimaryTermsTests extends ESAllocationTestCase {

    private static final String TEST_INDEX_1 = "test1";
    private static final String TEST_INDEX_2 = "test2";
    private RoutingTable testRoutingTable;
    private int numberOfShards;
    private int numberOfReplicas;
    private static final Settings DEFAULT_SETTINGS = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
    private AllocationService allocationService;
    private ClusterState clusterState;

    private final Map<String, long[]> primaryTermsPerIndex = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.allocationService = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE) // don't limit recoveries
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", Integer.MAX_VALUE)
                .build());
        this.numberOfShards = randomIntBetween(1, 5);
        this.numberOfReplicas = randomIntBetween(1, 5);
        logger.info("Setup test with {} shards and {} replicas.", this.numberOfShards, this.numberOfReplicas);
        this.primaryTermsPerIndex.clear();
        MetaData metaData = MetaData.builder()
                .put(createIndexMetaData(TEST_INDEX_1))
                .put(createIndexMetaData(TEST_INDEX_2))
                .build();

        this.testRoutingTable = new RoutingTable.Builder()
                .add(new IndexRoutingTable.Builder(metaData.index(TEST_INDEX_1).getIndex()).initializeAsNew(metaData.index(TEST_INDEX_1))
                        .build())
                .add(new IndexRoutingTable.Builder(metaData.index(TEST_INDEX_2).getIndex()).initializeAsNew(metaData.index(TEST_INDEX_2))
                        .build())
                .build();

        this.clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData).routingTable(testRoutingTable).build();
    }

    /**
     * puts primary shard routings into initializing state
     */
    private void initPrimaries() {
        logger.info("adding {} nodes and performing rerouting", this.numberOfReplicas + 1);
        Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < this.numberOfReplicas + 1; i++) {
            discoBuilder = discoBuilder.add(newNode("node" + i));
        }
        this.clusterState = ClusterState.builder(clusterState).nodes(discoBuilder).build();
        RoutingAllocation.Result rerouteResult = allocationService.reroute(clusterState, "reroute");
        this.testRoutingTable = rerouteResult.routingTable();
        assertThat(rerouteResult.changed(), is(true));
        applyRerouteResult(rerouteResult);
        primaryTermsPerIndex.keySet().forEach(this::incrementPrimaryTerm);
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

    private boolean startInitializingShards(String index) {
        this.clusterState = ClusterState.builder(clusterState).routingTable(this.testRoutingTable).build();
        final List<ShardRouting> startedShards = this.clusterState.getRoutingNodes().shardsWithState(index, INITIALIZING);
        logger.info("start primary shards for index [{}]: {} ", index, startedShards);
        RoutingAllocation.Result rerouteResult = allocationService.applyStartedShards(this.clusterState, startedShards);
        applyRerouteResult(rerouteResult);
        return rerouteResult.changed();
    }

    private void applyRerouteResult(RoutingAllocation.Result rerouteResult) {
        ClusterState previousClusterState = this.clusterState;
        ClusterState newClusterState = ClusterState.builder(previousClusterState).routingResult(rerouteResult).build();
        ClusterState.Builder builder = ClusterState.builder(newClusterState).incrementVersion();
        if (previousClusterState.routingTable() != newClusterState.routingTable()) {
            builder.routingTable(RoutingTable.builder(newClusterState.routingTable()).version(newClusterState.routingTable().version() + 1)
                    .build());
        }
        if (previousClusterState.metaData() != newClusterState.metaData()) {
            builder.metaData(MetaData.builder(newClusterState.metaData()).version(newClusterState.metaData().version() + 1));
        }
        this.clusterState = builder.build();
        this.testRoutingTable = rerouteResult.routingTable();
        final ClusterStateHealth clusterHealth = new ClusterStateHealth(clusterState);
        logger.info("applied reroute. active shards: p [{}], t [{}], init shards: [{}], relocating: [{}]",
                clusterHealth.getActivePrimaryShards(), clusterHealth.getActiveShards(),
                clusterHealth.getInitializingShards(), clusterHealth.getRelocatingShards());
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
        }
        RoutingAllocation.Result rerouteResult = allocationService.applyFailedShards(this.clusterState, failedShards);
        applyRerouteResult(rerouteResult);
    }

    private void addNodes() {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
        final int newNodes = randomInt(10);
        logger.info("adding [{}] nodes", newNodes);
        for (int i = 0; i < newNodes; i++) {
            nodesBuilder.add(newNode("extra_" + i));
        }
        this.clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();
        RoutingAllocation.Result rerouteResult = allocationService.reroute(this.clusterState, "nodes added");
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
        primaryTermsPerIndex.keySet().forEach(this::assertPrimaryTerm);
    }

    private void assertPrimaryTerm(String index) {
        final long[] terms = primaryTermsPerIndex.get(index);
        final IndexMetaData indexMetaData = clusterState.metaData().index(index);
        for (IndexShardRoutingTable shardRoutingTable : this.testRoutingTable.index(index)) {
            final int shard = shardRoutingTable.shardId().id();
            assertThat("primary term mismatch between indexMetaData of [" + index + "] and shard [" + shard + "]'s routing",
                    indexMetaData.primaryTerm(shard), equalTo(terms[shard]));
        }
    }

    public void testPrimaryTermMetaDataSync() {
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

        // relocations shouldn't change much
        addNodes();
        assertAllPrimaryTerm();
        boolean changed = true;
        while (changed) {
            changed = startInitializingShards(TEST_INDEX_1);
            assertAllPrimaryTerm();
            changed |= startInitializingShards(TEST_INDEX_2);
            assertAllPrimaryTerm();
        }

        // primary promotion
        failSomePrimaries(TEST_INDEX_1);
        assertAllPrimaryTerm();

        // stablize cluster
        changed = true;
        while (changed) {
            changed = startInitializingShards(TEST_INDEX_1);
            assertAllPrimaryTerm();
            changed |= startInitializingShards(TEST_INDEX_2);
            assertAllPrimaryTerm();
        }
    }
}
