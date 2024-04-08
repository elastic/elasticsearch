/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.getTestTransportPlugin;
import static org.elasticsearch.test.ESTestCase.getTestTransportType;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * External cluster to run the tests against.
 * It is a pure immutable test cluster that allows to send requests to a pre-existing cluster
 * and supports by nature all the needed test operations like wipeIndices etc.
 *
 * @deprecated not a realistic test setup since the removal of the transport client, use {@link ESIntegTestCase} for internal-cluster tests
 *             or {@link org.elasticsearch.test.rest.ESRestTestCase} otherwise.
 */
@Deprecated(forRemoval = true)
public final class ExternalTestCluster extends TestCluster {

    private static final Logger logger = LogManager.getLogger(ExternalTestCluster.class);

    private static final AtomicInteger counter = new AtomicInteger();
    public static final String EXTERNAL_CLUSTER_PREFIX = "external_";

    private final MockNode node;
    private final Client client;

    private final InetSocketAddress[] httpAddresses;

    private final String clusterName;

    private final int numDataNodes;
    private final int numMasterAndDataNodes;

    public ExternalTestCluster(
        Path tempDir,
        Settings additionalSettings,
        Collection<Class<? extends Plugin>> pluginClasses,
        Function<Client, Client> clientWrapper,
        String clusterName,
        TransportAddress... transportAddresses
    ) {
        super(0);
        this.clusterName = clusterName;
        Settings.Builder clientSettingsBuilder = Settings.builder()
            .put(additionalSettings)
            .putList("node.roles", Collections.emptyList())
            .put("node.name", EXTERNAL_CLUSTER_PREFIX + counter.getAndIncrement())
            .put("cluster.name", clusterName)
            .put(TransportSettings.PORT.getKey(), ESTestCase.getPortRange())
            .putList(
                "discovery.seed_hosts",
                Arrays.stream(transportAddresses).map(TransportAddress::toString).collect(Collectors.toList())
            );
        if (Environment.PATH_HOME_SETTING.exists(additionalSettings) == false) {
            clientSettingsBuilder.put(Environment.PATH_HOME_SETTING.getKey(), tempDir);
        }
        boolean addMockTcpTransport = additionalSettings.get(NetworkModule.TRANSPORT_TYPE_KEY) == null;

        if (addMockTcpTransport) {
            String transport = getTestTransportType();
            clientSettingsBuilder.put(NetworkModule.TRANSPORT_TYPE_KEY, transport);
            if (pluginClasses.contains(getTestTransportPlugin()) == false) {
                pluginClasses = new ArrayList<>(pluginClasses);
                pluginClasses.add(getTestTransportPlugin());
            }
        }
        pluginClasses = new ArrayList<>(pluginClasses);
        pluginClasses.add(MockHttpTransport.TestPlugin.class);
        Settings clientSettings = clientSettingsBuilder.build();
        MockNode mockNode = new MockNode(clientSettings, pluginClasses);
        Client wrappedClient = clientWrapper.apply(mockNode.client());
        try {
            mockNode.start();
            NodesInfoResponse nodeInfos = wrappedClient.admin().cluster().prepareNodesInfo().clear().setSettings(true).setHttp(true).get();
            httpAddresses = new InetSocketAddress[nodeInfos.getNodes().size()];
            int dataNodes = 0;
            int masterAndDataNodes = 0;
            for (int i = 0; i < nodeInfos.getNodes().size(); i++) {
                NodeInfo nodeInfo = nodeInfos.getNodes().get(i);
                httpAddresses[i] = nodeInfo.getInfo(HttpInfo.class).address().publishAddress().address();
                if (DiscoveryNode.canContainData(nodeInfo.getSettings())) {
                    dataNodes++;
                    masterAndDataNodes++;
                } else if (DiscoveryNode.isMasterNode(nodeInfo.getSettings())) {
                    masterAndDataNodes++;
                }
            }
            this.numDataNodes = dataNodes;
            this.numMasterAndDataNodes = masterAndDataNodes;
            this.client = wrappedClient;
            this.node = mockNode;

            logger.info("Setup ExternalTestCluster [{}] made of [{}] nodes", nodeInfos.getClusterName().value(), size());
        } catch (NodeValidationException e) {
            try {
                IOUtils.close(mockNode);
            } catch (IOException e1) {
                e.addSuppressed(e1);
            }
            throw new ElasticsearchException(e);
        } catch (Exception e) {
            try {
                IOUtils.close(mockNode);
            } catch (IOException e1) {
                e.addSuppressed(e1);
            }
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
        IOUtils.close(node);
    }

    @Override
    public void ensureEstimatedStats() {
        if (size() > 0) {
            NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().clear().setBreaker(true).setIndices(true).get();
            for (NodeStats stats : nodeStats.getNodes()) {
                assertThat(
                    "Fielddata breaker not reset to 0 on node: " + stats.getNode(),
                    stats.getBreaker().getStats(CircuitBreaker.FIELDDATA).getEstimated(),
                    equalTo(0L)
                );
                // ExternalTestCluster does not check the request breaker,
                // because checking it requires a network request, which in
                // turn increments the breaker, making it non-0

                assertThat(
                    "Fielddata size must be 0 on node: " + stats.getNode(),
                    stats.getIndices().getFieldData().getMemorySizeInBytes(),
                    equalTo(0L)
                );
                assertThat(
                    "Query cache size must be 0 on node: " + stats.getNode(),
                    stats.getIndices().getQueryCache().getMemorySizeInBytes(),
                    equalTo(0L)
                );
                assertThat(
                    "FixedBitSet cache size must be 0 on node: " + stats.getNode(),
                    stats.getIndices().getSegments().getBitsetMemoryInBytes(),
                    equalTo(0L)
                );
            }
        }
    }

    @Override
    public Iterable<Client> getClients() {
        return List.of(client);
    }

    @Override
    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return node.getNamedWriteableRegistry();
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }
}
