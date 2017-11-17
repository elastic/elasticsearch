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
package org.elasticsearch.transport.netty4;

import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4TransportIT extends ESNetty4IntegTestCase {
    // static so we can use it in anonymous classes
    private static String channelProfileName = null;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.TRANSPORT_TYPE_KEY, "exception-throwing").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> list = new ArrayList<>();
        list.add(ExceptionThrowingNetty4Transport.TestPlugin.class);
        list.addAll(super.nodePlugins());
        return Collections.unmodifiableCollection(list);
    }

    public void testThatConnectionFailsAsIntended() throws Exception {
        Client transportClient = internalCluster().transportClient();
        ClusterHealthResponse clusterIndexHealths = transportClient.admin().cluster().prepareHealth().get();
        assertThat(clusterIndexHealths.getStatus(), is(ClusterHealthStatus.GREEN));
        try {
            transportClient.filterWithHeader(Collections.singletonMap("ERROR", "MY MESSAGE")).admin().cluster().prepareHealth().get();
            fail("Expected exception, but didn't happen");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("MY MESSAGE"));
            assertThat(channelProfileName, is(TcpTransport.DEFAULT_PROFILE));
        }
    }

    public static final class ExceptionThrowingNetty4Transport extends Netty4Transport {

        public static class TestPlugin extends Plugin implements NetworkPlugin {

            @Override
            public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                                                  CircuitBreakerService circuitBreakerService,
                                                                  NamedWriteableRegistry namedWriteableRegistry,
                                                                  NetworkService networkService) {
                return Collections.singletonMap("exception-throwing",
                    () -> new ExceptionThrowingNetty4Transport(settings, threadPool, networkService, bigArrays,
                    namedWriteableRegistry, circuitBreakerService));
            }
        }

        public ExceptionThrowingNetty4Transport(
                Settings settings,
                ThreadPool threadPool,
                NetworkService networkService,
                BigArrays bigArrays,
                NamedWriteableRegistry namedWriteableRegistry,
                CircuitBreakerService circuitBreakerService) {
            super(settings, threadPool, networkService, bigArrays, namedWriteableRegistry, circuitBreakerService);
        }

        @Override
        protected String handleRequest(TcpChannel channel, String profileName,
                                       StreamInput stream, long requestId, int messageLengthBytes, Version version,
                                       InetSocketAddress remoteAddress, byte status) throws IOException {
            String action = super.handleRequest(channel, profileName, stream, requestId, messageLengthBytes, version,
                    remoteAddress, status);
            channelProfileName = TcpTransport.DEFAULT_PROFILE;
            return action;
        }

        @Override
        protected void validateRequest(StreamInput buffer, long requestId, String action)
                throws IOException {
            super.validateRequest(buffer, requestId, action);
            String error = threadPool.getThreadContext().getHeader("ERROR");
            if (error != null) {
                throw new ElasticsearchException(error);
            }
        }

    }

}
