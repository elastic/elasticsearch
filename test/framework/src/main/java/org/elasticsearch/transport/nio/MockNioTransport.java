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

package org.elasticsearch.transport.nio;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.BytesChannelContext;
import org.elasticsearch.nio.BytesWriteHandler;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpServerChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class MockNioTransport extends TcpTransport {

    private static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = Transports.NIO_TRANSPORT_WORKER_THREAD_NAME_PREFIX;

    private final PageCacheRecycler pageCacheRecycler;
    private final ConcurrentMap<String, MockTcpChannelFactory> profileToChannelFactory = newConcurrentMap();
    private volatile NioGroup nioGroup;
    private volatile MockTcpChannelFactory clientChannelFactory;

    MockNioTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                     PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                     CircuitBreakerService circuitBreakerService) {
        super("mock-nio", settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService);
        this.pageCacheRecycler = pageCacheRecycler;
    }

    @Override
    protected MockServerChannel bind(String name, InetSocketAddress address) throws IOException {
        MockTcpChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        return nioGroup.bindServerChannel(address, channelFactory);
    }

    @Override
    protected MockSocketChannel initiateChannel(DiscoveryNode node, ActionListener<Void> connectListener) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        MockSocketChannel channel = nioGroup.openChannel(address, clientChannelFactory);
        channel.addConnectListener(ActionListener.toBiConsumer(connectListener));
        return channel;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            nioGroup = new NioGroup(daemonThreadFactory(this.settings, TRANSPORT_WORKER_THREAD_NAME_PREFIX), 2,
                (s) -> new TestingSocketEventHandler(this::onNonChannelException, s));

            ProfileSettings clientProfileSettings = new ProfileSettings(settings, "default");
            clientChannelFactory = new MockTcpChannelFactory(clientProfileSettings, "client");

            if (NetworkService.NETWORK_SERVER.get(settings)) {
                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    String profileName = profileSettings.profileName;
                    MockTcpChannelFactory factory = new MockTcpChannelFactory(profileSettings, profileName);
                    profileToChannelFactory.putIfAbsent(profileName, factory);
                    bindServer(profileSettings);
                }
            }

            super.doStart();
            success = true;
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    @Override
    protected void stopInternal() {
        try {
            nioGroup.close();
        } catch (Exception e) {
            logger.warn("unexpected exception while stopping nio group", e);
        }
        profileToChannelFactory.clear();
    }

    @Override
    protected ConnectionProfile maybeOverrideConnectionProfile(ConnectionProfile connectionProfile) {
        if (connectionProfile.getNumConnections() <= 3) {
            return connectionProfile;
        }
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        Set<TransportRequestOptions.Type> allTypesWithConnection = new HashSet<>();
        Set<TransportRequestOptions.Type> allTypesWithoutConnection = new HashSet<>();
        for (TransportRequestOptions.Type type : TransportRequestOptions.Type.values()) {
            int numConnections = connectionProfile.getNumConnectionsPerType(type);
            if (numConnections > 0) {
                allTypesWithConnection.add(type);
            } else {
                allTypesWithoutConnection.add(type);
            }
        }

        // make sure we maintain at least the types that are supported by this profile even if we only use a single channel for them.
        builder.addConnections(3, allTypesWithConnection.toArray(new TransportRequestOptions.Type[0]));
        if (allTypesWithoutConnection.isEmpty() == false) {
            builder.addConnections(0, allTypesWithoutConnection.toArray(new TransportRequestOptions.Type[0]));
        }
        builder.setHandshakeTimeout(connectionProfile.getHandshakeTimeout());
        builder.setConnectTimeout(connectionProfile.getConnectTimeout());
        return builder.build();
    }

    private void exceptionCaught(NioSocketChannel channel, Exception exception) {
        onException((TcpChannel) channel, exception);
    }

    private void acceptChannel(NioSocketChannel channel) {
        serverAcceptedChannel((TcpChannel) channel);
    }

    private class MockTcpChannelFactory extends ChannelFactory<MockServerChannel, MockSocketChannel> {

        private final String profileName;

        private MockTcpChannelFactory(ProfileSettings profileSettings, String profileName) {
            super(new RawChannelFactory(profileSettings.tcpNoDelay,
                profileSettings.tcpKeepAlive,
                profileSettings.reuseAddress,
                Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
                Math.toIntExact(profileSettings.receiveBufferSize.getBytes())));
            this.profileName = profileName;
        }

        @Override
        public MockSocketChannel createChannel(NioSelector selector, SocketChannel channel) throws IOException {
            MockSocketChannel nioChannel = new MockSocketChannel(profileName, channel, selector);
            Supplier<InboundChannelBuffer.Page> pageSupplier = () -> {
                Recycler.V<byte[]> bytes = pageCacheRecycler.bytePage(false);
                return new InboundChannelBuffer.Page(ByteBuffer.wrap(bytes.v()), bytes::close);
            };
            MockTcpReadWriteHandler readWriteHandler = new MockTcpReadWriteHandler(nioChannel, MockNioTransport.this);
            BytesChannelContext context = new BytesChannelContext(nioChannel, selector, (e) -> exceptionCaught(nioChannel, e),
                readWriteHandler, new InboundChannelBuffer(pageSupplier));
            nioChannel.setContext(context);
            return nioChannel;
        }

        @Override
        public MockServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel) throws IOException {
            MockServerChannel nioServerChannel = new MockServerChannel(profileName, channel);
            Consumer<Exception> exceptionHandler = (e) -> logger.error(() ->
                new ParameterizedMessage("exception from server channel caught on transport layer [{}]", channel), e);
            ServerChannelContext context = new ServerChannelContext(nioServerChannel, this, selector, MockNioTransport.this::acceptChannel,
                exceptionHandler);
            nioServerChannel.setContext(context);
            return nioServerChannel;
        }
    }

    private static class MockTcpReadWriteHandler extends BytesWriteHandler {

        private final MockSocketChannel channel;
        private final TcpTransport transport;

        private MockTcpReadWriteHandler(MockSocketChannel channel, TcpTransport transport) {
            this.channel = channel;
            this.transport = transport;
        }

        @Override
        public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
            BytesReference bytesReference = BytesReference.fromByteBuffers(channelBuffer.sliceBuffersTo(channelBuffer.getIndex()));
            return transport.consumeNetworkReads(channel, bytesReference);
        }
    }

    private static class MockServerChannel extends NioServerSocketChannel implements TcpServerChannel {

        private final String profile;

        MockServerChannel(String profile, ServerSocketChannel channel) {
            super(channel);
            this.profile = profile;
        }

        @Override
        public void close() {
            getContext().closeChannel();
        }

        @Override
        public String getProfile() {
            return profile;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            addCloseListener(ActionListener.toBiConsumer(listener));
        }
    }

    private static class MockSocketChannel extends NioSocketChannel implements TcpChannel {

        private final String profile;

        private MockSocketChannel(String profile, java.nio.channels.SocketChannel socketChannel, NioSelector selector) {
            super(socketChannel);
            this.profile = profile;
        }

        @Override
        public void close() {
            getContext().closeChannel();
        }

        @Override
        public String getProfile() {
            return profile;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            addCloseListener(ActionListener.toBiConsumer(listener));
        }

        @Override
        public void setSoLinger(int value) throws IOException {
            if (isOpen()) {
                getRawChannel().setOption(StandardSocketOptions.SO_LINGER, value);
            }
        }

        @Override
        public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
            getContext().sendMessage(BytesReference.toByteBuffers(reference), ActionListener.toBiConsumer(listener));
        }
    }
}
