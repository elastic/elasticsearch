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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.MockTcpTransportPlugin;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.transport.nio.NioTransportPlugin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.ESTestCase.getTestTransportType;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * External cluster to run the tests against.
 * It is a pure immutable test cluster that allows to send requests to a pre-existing cluster
 * and supports by nature all the needed test operations like wipeIndices etc.
 */
public final class ExternalTestCluster extends TestCluster {

    private static final Logger logger = Loggers.getLogger(ExternalTestCluster.class);

    private static final AtomicInteger counter = new AtomicInteger();
    public static final String EXTERNAL_CLUSTER_PREFIX = "external_";

    private final MockTransportClient client;

    private final InetSocketAddress[] httpAddresses;

    private final String clusterName;

    private final int numDataNodes;
    private final int numMasterAndDataNodes;

    public ExternalTestCluster(Path tempDir, Settings additionalSettings, Collection<Class<? extends Plugin>> pluginClasses,
                               TransportAddress... transportAddresses) {
        super(0);
        Settings.Builder clientSettingsBuilder = Settings.builder()
            .put(additionalSettings)
            .put("node.name", InternalTestCluster.TRANSPORT_CLIENT_PREFIX + EXTERNAL_CLUSTER_PREFIX + counter.getAndIncrement())
            .put("client.transport.ignore_cluster_name", true)
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir);
        boolean addMockTcpTransport = additionalSettings.get(NetworkModule.TRANSPORT_TYPE_KEY) == null;

        if (addMockTcpTransport) {
            String transport = getTestTransportType();
            clientSettingsBuilder.put(NetworkModule.TRANSPORT_TYPE_KEY, transport);
            if (pluginClasses.contains(MockTcpTransportPlugin.class) == false &&
                pluginClasses.contains(NioTransportPlugin.class) == false) {
                pluginClasses = new ArrayList<>(pluginClasses);
                if (transport.equals(NioTransportPlugin.NIO_TRANSPORT_NAME)) {
                    pluginClasses.add(NioTransportPlugin.class);
                } else {
                    pluginClasses.add(MockTcpTransportPlugin.class);
                }
            }
        }
        Settings clientSettings = clientSettingsBuilder.build();
        MockTransportClient client = new MockTransportClient(clientSettings, pluginClasses);
        try {
            client.addTransportAddresses(transportAddresses);
            NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().clear().setSettings(true).setHttp(true).get();
            httpAddresses = new InetSocketAddress[nodeInfos.getNodes().size()];
            this.clusterName = nodeInfos.getClusterName().value();
            int dataNodes = 0;
            int masterAndDataNodes = 0;
            for (int i = 0; i < nodeInfos.getNodes().size(); i++) {
                NodeInfo nodeInfo = nodeInfos.getNodes().get(i);
                httpAddresses[i] = nodeInfo.getHttp().address().publishAddress().address();
                if (DiscoveryNode.isDataNode(nodeInfo.getSettings())) {
                    dataNodes++;
                    masterAndDataNodes++;
                } else if (DiscoveryNode.isMasterNode(nodeInfo.getSettings())) {
                    masterAndDataNodes++;
                }
            }
            this.numDataNodes = dataNodes;
            this.numMasterAndDataNodes = masterAndDataNodes;
            this.client = client;

            logger.info("Setup ExternalTestCluster [{}] made of [{}] nodes", nodeInfos.getClusterName().value(), size());
        } catch (Exception e) {
            client.close();
            throw e;
        }
    }

    @Override
    public void afterTest() {

    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public int size() {
        return httpAddresses.length;
    }

    @Override
    public int numDataNodes() {
        return numDataNodes;
    }

    @Override
    public int numDataAndMasterNodes() {
        return numMasterAndDataNodes;
    }

    @Override
    public InetSocketAddress[] httpAddresses() {
        return httpAddresses;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public void ensureEstimatedStats() {
        if (size() > 0) {
            NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats()
                    .clear().setBreaker(true).setIndices(true).execute().actionGet();
            for (NodeStats stats : nodeStats.getNodes()) {
                assertThat("Fielddata breaker not reset to 0 on node: " + stats.getNode(),
                        stats.getBreaker().getStats(CircuitBreaker.FIELDDATA).getEstimated(), equalTo(0L));
                // ExternalTestCluster does not check the request breaker,
                // because checking it requires a network request, which in
                // turn increments the breaker, making it non-0

                assertThat("Fielddata size must be 0 on node: " +
                    stats.getNode(), stats.getIndices().getFieldData().getMemorySizeInBytes(), equalTo(0L));
                assertThat("Query cache size must be 0 on node: " +
                    stats.getNode(), stats.getIndices().getQueryCache().getMemorySizeInBytes(), equalTo(0L));
                assertThat("FixedBitSet cache size must be 0 on node: " +
                    stats.getNode(), stats.getIndices().getSegments().getBitsetMemoryInBytes(), equalTo(0L));
            }
        }
    }

    @Override
    public Iterable<Client> getClients() {
        return Collections.singleton(client);
    }

    @Override
    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return client.getNamedWriteableRegistry();
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }
}
