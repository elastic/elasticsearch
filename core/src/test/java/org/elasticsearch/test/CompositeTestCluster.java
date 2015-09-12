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
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.google.common.collect.Iterators;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * A test cluster implementation that holds a fixed set of external nodes as well as a InternalTestCluster
 * which is used to run mixed version clusters in tests like backwards compatibility tests.
 * Note: this is an experimental API
 */
public class CompositeTestCluster extends TestCluster {
    private final InternalTestCluster cluster;
    private final ExternalNode[] externalNodes;
    private final ExternalClient client = new ExternalClient();
    private static final String NODE_PREFIX = "external_";

    public CompositeTestCluster(InternalTestCluster cluster, int numExternalNodes, ExternalNode externalNode) throws IOException {
        super(cluster.seed());
        this.cluster = cluster;
        this.externalNodes = new ExternalNode[numExternalNodes];
        for (int i = 0; i < externalNodes.length; i++) {
            externalNodes[i] = externalNode;
        }
    }

    @Override
    public synchronized void afterTest() throws IOException {
        cluster.afterTest();
    }

    @Override
    public synchronized void beforeTest(Random random, double transportClientRatio) throws IOException {
        super.beforeTest(random, transportClientRatio);
        cluster.beforeTest(random, transportClientRatio);
        Settings defaultSettings = cluster.getDefaultSettings();
        final Client client = cluster.size() > 0 ? cluster.client() : cluster.clientNodeClient();
        for (int i = 0; i < externalNodes.length; i++) {
            if (!externalNodes[i].running()) {
                try {
                    externalNodes[i] = externalNodes[i].start(client, defaultSettings, NODE_PREFIX + i, cluster.getClusterName(), i);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    return;
                }
            }
            externalNodes[i].reset(random.nextLong());
        }
        if (size() > 0) {
            client().admin().cluster().prepareHealth().setWaitForNodes(">=" + Integer.toString(this.size())).get();
        }
    }

    private Collection<ExternalNode> runningNodes() {
        return Arrays
                .stream(externalNodes)
                .filter(input -> input.running())
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * Upgrades one external running node to a node from the version running the tests. Commonly this is used
     * to move from a node with version N-1 to a node running version N. This works seamless since they will
     * share the same data directory. This method will return <tt>true</tt> iff a node got upgraded otherwise if no
     * external node is running it returns <tt>false</tt>
     */
    public synchronized boolean upgradeOneNode() throws InterruptedException, IOException {
      return upgradeOneNode(Settings.EMPTY);
    }

    /**
     * Upgrades all external running nodes to a node from the version running the tests.
     * All nodes are shut down before the first upgrade happens.
     * @return <code>true</code> iff at least one node as upgraded.
     */
    public synchronized boolean upgradeAllNodes() throws InterruptedException, IOException {
        return upgradeAllNodes(Settings.EMPTY);
    }


    /**
     * Upgrades all external running nodes to a node from the version running the tests.
     * All nodes are shut down before the first upgrade happens.
     * @return <code>true</code> iff at least one node as upgraded.
     * @param nodeSettings settings for the upgrade nodes
     */
    public synchronized boolean upgradeAllNodes(Settings nodeSettings) throws InterruptedException, IOException {
        boolean upgradedOneNode = false;
        while(upgradeOneNode(nodeSettings)) {
            upgradedOneNode = true;
        }
        return upgradedOneNode;
    }

    /**
     * Upgrades one external running node to a node from the version running the tests. Commonly this is used
     * to move from a node with version N-1 to a node running version N. This works seamless since they will
     * share the same data directory. This method will return <tt>true</tt> iff a node got upgraded otherwise if no
     * external node is running it returns <tt>false</tt>
     */
    public synchronized boolean upgradeOneNode(Settings nodeSettings) throws InterruptedException, IOException {
        Collection<ExternalNode> runningNodes = runningNodes();
        if (!runningNodes.isEmpty()) {
            final Client existingClient = cluster.client();
            ExternalNode externalNode = RandomPicks.randomFrom(random, runningNodes);
            externalNode.stop();
            String s = cluster.startNode(nodeSettings);
            ExternalNode.waitForNode(existingClient, s);
            assertNoTimeout(existingClient.admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(size())).get());
            return true;
        }
        return false;
    }


    /**
     * Returns the a simple pattern that matches all "new" nodes in the cluster.
     */
    public String newNodePattern() {
        return cluster.nodePrefix() + "*";
    }

    /**
     * Returns the a simple pattern that matches all "old" / "backwardss" nodes in the cluster.
     */
    public String backwardsNodePattern() {
        return NODE_PREFIX + "*";
    }

    /**
     * Allows allocation of shards of the given indices on all nodes in the cluster.
     */
    public void allowOnAllNodes(String... index) {
        Settings build = Settings.builder().put("index.routing.allocation.exclude._name", "").build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
    }

    /**
     * Allows allocation of shards of the given indices only on "new" nodes in the cluster.
     * Note: if a shard is allocated on an "old" node and can't be allocated on a "new" node it will only be removed it can
     * be allocated on some other "new" node.
     */
    public void allowOnlyNewNodes(String... index) {
        Settings build = Settings.builder().put("index.routing.allocation.exclude._name", backwardsNodePattern()).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
    }

    /**
     * Starts a current version data node
     */
    public void startNewNode() {
        cluster.startNode();
    }


    @Override
    public synchronized Client client() {
       return client;
    }

    @Override
    public synchronized int size() {
        return runningNodes().size() + cluster.size();
    }

    @Override
    public int numDataNodes() {
        return runningNodes().size() + cluster.numDataNodes();
    }

    @Override
    public int numDataAndMasterNodes() {
        return runningNodes().size() + cluster.numDataAndMasterNodes();
    }

    @Override
    public InetSocketAddress[] httpAddresses() {
        return cluster.httpAddresses();
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.close(externalNodes);
        } finally {
            IOUtils.close(cluster);
        }
    }

    @Override
    public void ensureEstimatedStats() {
        if (size() > 0) {
            NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats()
                    .clear().setBreaker(true).execute().actionGet();
            for (NodeStats stats : nodeStats.getNodes()) {
                assertThat("Fielddata breaker not reset to 0 on node: " + stats.getNode(),
                        stats.getBreaker().getStats(CircuitBreaker.FIELDDATA).getEstimated(), equalTo(0L));
            }
            // CompositeTestCluster does not check the request breaker,
            // because checking it requires a network request, which in
            // turn increments the breaker, making it non-0
        }
    }

    @Override
    public String getClusterName() {
        return cluster.getClusterName();
    }

    @Override
    public synchronized Iterator<Client> iterator() {
        return Iterators.singletonIterator(client());
    }

    /**
     * Delegates to {@link org.elasticsearch.test.InternalTestCluster#fullRestart()}
     */
    public void fullRestartInternalCluster() throws Exception {
        cluster.fullRestart();
    }

    /**
     * Returns the number of current version data nodes in the cluster
     */
    public int numNewDataNodes() {
        return cluster.numDataNodes();
    }

    /**
     * Returns the number of former version data nodes in the cluster
     */
    public int numBackwardsDataNodes() {
        return runningNodes().size();
    }

    public TransportAddress externalTransportAddress() {
        return RandomPicks.randomFrom(random, externalNodes).getTransportAddress();
    }

    public InternalTestCluster internalCluster() {
        return cluster;
    }

    private synchronized Client internalClient() {
        Collection<ExternalNode> externalNodes = runningNodes();
        return random.nextBoolean() && !externalNodes.isEmpty() ? RandomPicks.randomFrom(random, externalNodes).getClient() : cluster.client();
    }

    private final class ExternalClient extends FilterClient {

        public ExternalClient() {
            super(internalClient());
        }

        @Override
        public void close() {
            // never close this client
        }
    }
}
