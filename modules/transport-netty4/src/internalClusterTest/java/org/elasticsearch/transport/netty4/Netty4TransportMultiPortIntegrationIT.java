/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport.netty4;

import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.transport.TransportInfo;

import java.util.Locale;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1, numClientNodes = 0)
public class Netty4TransportMultiPortIntegrationIT extends ESNetty4IntegTestCase {

    private static int randomPort = -1;
    private static String randomPortRange;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        if (randomPort == -1) {
            randomPort = randomIntBetween(49152, 65525);
            randomPortRange = String.format(Locale.ROOT, "%s-%s", randomPort, randomPort + 10);
        }
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("network.host", "127.0.0.1")
            .put("transport.profiles.client1.port", randomPortRange)
            .put("transport.profiles.client1.publish_host", "127.0.0.7")
            .put("transport.profiles.client1.publish_port", "4321")
            .put("transport.profiles.client1.tcp.reuse_address", true);
        return builder.build();
    }

    @Network
    public void testThatInfosAreExposed() throws Exception {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().clear().setTransport(true).get();
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
