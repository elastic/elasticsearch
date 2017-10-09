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
package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.function.Consumer;

public class MockTcpTransportTests extends AbstractSimpleTransportTestCase<MockTcpTransport.MockChannel> {

    @Override
    protected MockTransportService build(Settings settings, Version version, ClusterSettings clusterSettings, boolean doHandshake,
                                         Consumer<MockTcpTransport.MockChannel> onChannelOpen) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        Transport transport = new MockTcpTransport(settings, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(Collections.emptyList()), version) {
            @Override
            protected Version executeHandshake(DiscoveryNode node, MockChannel mockChannel, TimeValue timeout) throws IOException,
                InterruptedException {
                if (doHandshake) {
                    return super.executeHandshake(node, mockChannel, timeout);
                } else {
                    return version.minimumCompatibilityVersion();
                }
            }

            @Override
            protected void onChannelOpen(MockChannel mockChannel) {
                onChannelOpen.accept(mockChannel);
            }
        };
        MockTransportService mockTransportService =
            MockTransportService.createNewService(Settings.EMPTY, transport, version, threadPool, clusterSettings);
        mockTransportService.start();
        return mockTransportService;
    }

    @Override
    protected void close(MockTcpTransport.MockChannel mockChannel) {
        try {
            mockChannel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean opensMultipleChannels() {
        return false;
    }

}
