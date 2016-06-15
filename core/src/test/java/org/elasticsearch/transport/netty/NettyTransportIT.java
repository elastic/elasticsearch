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
package org.elasticsearch.transport.netty;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportSettings;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class NettyTransportIT extends ESIntegTestCase {
    // static so we can use it in anonymous classes
    private static String channelProfileName = null;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(Node.NODE_MODE_SETTING.getKey(), "network")
                .put(NetworkModule.TRANSPORT_TYPE_KEY, "exception-throwing").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ExceptionThrowingNettyTransport.TestPlugin.class);
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
            assertThat(channelProfileName, is(TransportSettings.DEFAULT_PROFILE));
        }
    }

    public static final class ExceptionThrowingNettyTransport extends NettyTransport {

        public static class TestPlugin extends Plugin {
            public void onModule(NetworkModule module) {
                module.registerTransport("exception-throwing", ExceptionThrowingNettyTransport.class);
            }
        }

        @Inject
        public ExceptionThrowingNettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                                               Version version, NamedWriteableRegistry namedWriteableRegistry,
                                               CircuitBreakerService circuitBreakerService) {
            super(settings, threadPool, networkService, bigArrays, version, namedWriteableRegistry, circuitBreakerService);
        }

        @Override
        public ChannelPipelineFactory configureServerChannelPipelineFactory(String name, Settings groupSettings) {
            return new ErrorPipelineFactory(this, name, groupSettings);
        }

        private static class ErrorPipelineFactory extends ServerChannelPipelineFactory {

            private final ESLogger logger;

            public ErrorPipelineFactory(ExceptionThrowingNettyTransport nettyTransport, String name, Settings groupSettings) {
                super(nettyTransport, name, groupSettings);
                this.logger = nettyTransport.logger;
            }

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = super.getPipeline();
                pipeline.replace("dispatcher", "dispatcher",
                    new MessageChannelHandler(nettyTransport, logger, TransportSettings.DEFAULT_PROFILE) {

                    @Override
                    protected String handleRequest(Channel channel, Marker marker, StreamInput buffer, long requestId,
                                                   int messageLengthBytes, Version version) throws IOException {
                        String action = super.handleRequest(channel, marker, buffer, requestId, messageLengthBytes, version);
                        channelProfileName = this.profileName;
                        return action;
                    }

                    @Override
                    protected void validateRequest(Marker marker, StreamInput buffer, long requestId, String action) throws IOException {
                        super.validateRequest(marker, buffer, requestId, action);
                        String error = threadPool.getThreadContext().getHeader("ERROR");
                        if (error != null) {
                            throw new ElasticsearchException(error);
                        }
                    }
                });
                return pipeline;
            }
        }
    }
}
