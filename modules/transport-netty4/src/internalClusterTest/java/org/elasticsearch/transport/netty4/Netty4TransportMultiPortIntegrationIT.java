/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport.netty4;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.transport.MockTransportClient;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest.Metric.TRANSPORT;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0)
public class Netty4TransportMultiPortIntegrationIT extends ESNetty4IntegTestCase {

    private static final int NUMBER_OF_CLIENT_PORTS = Constants.WINDOWS ? 300 : 10;

    private static int randomPort = -1;
    private static String randomPortRange;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        if (randomPort == -1) {
            randomPort = randomIntBetween(49152, 65535 - NUMBER_OF_CLIENT_PORTS);
            randomPortRange = String.format(Locale.ROOT, "%s-%s", randomPort, randomPort + NUMBER_OF_CLIENT_PORTS);
        }
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("network.host", "127.0.0.1")
            .put("transport.profiles.client1.port", randomPortRange)
            .put("transport.profiles.client1.publish_host", "127.0.0.7")
            .put("transport.profiles.client1.publish_port", "4321")
            .put("transport.profiles.client1.reuse_address", true);
        return builder.build();
    }

    public void testThatTransportClientCanConnect() {
        Settings settings = Settings.builder()
            .put("cluster.name", internalCluster().getClusterName())
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        try (TransportClient transportClient = new MockTransportClient(settings, Arrays.asList(Netty4Plugin.class))) {
            final List<TransportAddress> addresses = new ArrayList<>();
            for (final Transport transport : internalCluster().getInstances(Transport.class)) {
                for (final TransportAddress transportAddress : transport.boundAddress().boundAddresses()) {
                    addresses.add(transportAddress);
                }
                for (final BoundTransportAddress profileAddresses : transport.profileBoundAddresses().values()) {
                    for (final TransportAddress transportAddress : profileAddresses.boundAddresses()) {
                        addresses.add(transportAddress);
                    }
                }
            }
            logger.info("--> all addresses: {}", addresses);
            final TransportAddress[] targetAddresses = randomNonEmptySubsetOf(addresses).toArray(new TransportAddress[0]);
            logger.info("--> target addresses: {}", Arrays.toString(targetAddresses));
            transportClient.addTransportAddresses(targetAddresses);
            ClusterHealthResponse response = transportClient.admin().cluster().prepareHealth().get();
            assertThat(response.getStatus(), is(ClusterHealthStatus.GREEN));
        }
    }

    @Network
    public void testThatInfosAreExposed() throws Exception {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().clear().addMetric(TRANSPORT.metricName()).get();
        for (NodeInfo nodeInfo : response.getNodes()) {
            assertThat(nodeInfo.getInfo(TransportInfo.class).getProfileAddresses().keySet(), hasSize(1));
            assertThat(nodeInfo.getInfo(TransportInfo.class).getProfileAddresses(), hasKey("client1"));
            BoundTransportAddress boundTransportAddress = nodeInfo.getInfo(TransportInfo.class).getProfileAddresses().get("client1");
            for (TransportAddress transportAddress : boundTransportAddress.boundAddresses()) {
                assertThat(transportAddress, instanceOf(TransportAddress.class));
            }

            // bound addresses
            for (TransportAddress transportAddress : boundTransportAddress.boundAddresses()) {
                assertThat(transportAddress, instanceOf(TransportAddress.class));
                assertThat(
                    transportAddress.address().getPort(),
                    is(allOf(greaterThanOrEqualTo(randomPort), lessThanOrEqualTo(randomPort + 10)))
                );
            }

            // publish address
            assertThat(boundTransportAddress.publishAddress(), instanceOf(TransportAddress.class));
            TransportAddress publishAddress = boundTransportAddress.publishAddress();
            assertThat(NetworkAddress.format(publishAddress.address().getAddress()), is("127.0.0.7"));
            assertThat(publishAddress.address().getPort(), is(4321));
        }
    }
}
