/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.AbstractSimpleTransportTests;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

public class SimpleNettyTransportTests extends AbstractSimpleTransportTests {

    @Override
    protected TransportService build(Settings settings, Version version) {
        int startPort = 11000 + randomIntBetween(0, 255);
        int endPort = startPort + 10;
        settings = ImmutableSettings.builder().put(settings).put("transport.tcp.port", startPort + "-" + endPort).build();
        return new TransportService(settings, new NettyTransport(settings, threadPool, new NetworkService(settings), version), threadPool).start();
    }

    @Test
    public void testConnectException() {
        try {
            serviceA.connectToNode(new DiscoveryNode("C", new InetSocketTransportAddress("localhost", 9876), Version.CURRENT));
            assert false;
        } catch (ConnectTransportException e) {
//            e.printStackTrace();
            // all is well
        }
    }
}