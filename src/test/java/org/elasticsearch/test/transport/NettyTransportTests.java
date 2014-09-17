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
package org.elasticsearch.test.transport;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.netty.MessageChannelHandler;
import org.elasticsearch.transport.netty.NettyTransport;
import org.elasticsearch.transport.netty.NettyTransportChannel;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 1)
public class NettyTransportTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put("node.mode", "network")
                .put(TransportModule.TRANSPORT_TYPE_KEY, ExceptionThrowingNettyTransport.class.getName()).build();
    }

    @Test
    public void testThatConnectionFailsAsIntended() throws Exception {
        Client transportClient = internalCluster().transportClient();
        ClusterHealthResponse clusterIndexHealths = transportClient.admin().cluster().prepareHealth().get();
        assertThat(clusterIndexHealths.getStatus(), is(ClusterHealthStatus.GREEN));

        try {
            transportClient.admin().cluster().prepareHealth().putHeader("ERROR", "MY MESSAGE").get();
            fail("Expected exception, but didnt happen");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("MY MESSAGE"));
        }
    }

    public static final class ExceptionThrowingNettyTransport extends NettyTransport {

        @Inject
        public ExceptionThrowingNettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays, Version version) {
            super(settings, threadPool, networkService, bigArrays, version);
        }

        @Override
        public ChannelPipelineFactory configureServerChannelPipelineFactory() {
            return new ErrorPipelineFactory(this);
        }

        private static class ErrorPipelineFactory extends ServerChannelPipeFactory {

            private final ESLogger logger;

            public ErrorPipelineFactory(ExceptionThrowingNettyTransport exceptionThrowingNettyTransport) {
                super(exceptionThrowingNettyTransport);
                this.logger = exceptionThrowingNettyTransport.logger;
            }

            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = super.getPipeline();
                pipeline.replace("dispatcher", "dispatcher", new MessageChannelHandler(nettyTransport, logger) {

                    @Override
                    protected String handleRequest(Channel channel, StreamInput buffer, long requestId, Version version) throws IOException {
                        final String action = buffer.readString();

                        final NettyTransportChannel transportChannel = new NettyTransportChannel(transport, action, channel, requestId, version);
                        try {
                            final TransportRequestHandler handler = transportServiceAdapter.handler(action, version);
                            if (handler == null) {
                                throw new ActionNotFoundTransportException(action);
                            }
                            final TransportRequest request = handler.newInstance();
                            request.remoteAddress(new InetSocketTransportAddress((InetSocketAddress) channel.getRemoteAddress()));
                            request.readFrom(buffer);
                            if (request.hasHeader("ERROR")) {
                                throw new ElasticsearchException((String) request.getHeader("ERROR"));
                            }
                            if (handler.executor() == ThreadPool.Names.SAME) {
                                //noinspection unchecked
                                handler.messageReceived(request, transportChannel);
                            } else {
                                threadPool.executor(handler.executor()).execute(new RequestHandler(handler, request, transportChannel, action));
                            }
                        } catch (Throwable e) {
                            try {
                                transportChannel.sendResponse(e);
                            } catch (IOException e1) {
                                logger.warn("Failed to send error message back to client for action [" + action + "]", e);
                                logger.warn("Actual Exception", e1);
                            }
                        }
                        return action;
                    }

                    class RequestHandler extends AbstractRunnable {
                        private final TransportRequestHandler handler;
                        private final TransportRequest request;
                        private final NettyTransportChannel transportChannel;
                        private final String action;

                        public RequestHandler(TransportRequestHandler handler, TransportRequest request, NettyTransportChannel transportChannel, String action) {
                            this.handler = handler;
                            this.request = request;
                            this.transportChannel = transportChannel;
                            this.action = action;
                        }

                        @SuppressWarnings({"unchecked"})
                        @Override
                        protected void doRun() throws Exception {
                            handler.messageReceived(request, transportChannel);
                        }

                        @Override
                        public boolean isForceExecution() {
                            return handler.isForceExecution();
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            if (transport.lifecycleState() == Lifecycle.State.STARTED) {
                                // we can only send a response transport is started....
                                try {
                                    transportChannel.sendResponse(e);
                                } catch (Throwable e1) {
                                    logger.warn("Failed to send error message back to client for action [" + action + "]", e1);
                                    logger.warn("Actual Exception", e);
                                }
                            }                        }
                    }
                });
                return pipeline;
            }
        }
    }
}
