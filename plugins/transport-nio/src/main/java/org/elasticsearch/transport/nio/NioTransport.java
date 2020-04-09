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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.BytesChannelContext;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.Config;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportSettings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class NioTransport extends TcpTransport {

    private static final Logger logger = LogManager.getLogger(NioTransport.class);

    protected final PageAllocator pageAllocator;
    private final ConcurrentMap<String, TcpChannelFactory> profileToChannelFactory = newConcurrentMap();
    private final NioGroupFactory groupFactory;
    private volatile NioGroup nioGroup;
    private volatile Function<DiscoveryNode, TcpChannelFactory> clientChannelFactory;

    protected NioTransport(Settings settings, Version version, ThreadPool threadPool, NetworkService networkService,
                           PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                           CircuitBreakerService circuitBreakerService, NioGroupFactory groupFactory) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService);
        this.pageAllocator = new PageAllocator(pageCacheRecycler);
        this.groupFactory = groupFactory;
    }

    @Override
    protected NioTcpServerChannel bind(String name, InetSocketAddress address) throws IOException {
        TcpChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        NioTcpServerChannel serverChannel = nioGroup.bindServerChannel(address, channelFactory);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        serverChannel.addBindListener(ActionListener.toBiConsumer(future));
        future.actionGet();
        return serverChannel;
    }

    @Override
    protected NioTcpChannel initiateChannel(DiscoveryNode node) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        return nioGroup.openChannel(address, clientChannelFactory.apply(node));
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            nioGroup = groupFactory.getTransportGroup();

            ProfileSettings clientProfileSettings = new ProfileSettings(settings, TransportSettings.DEFAULT_PROFILE);
            clientChannelFactory = clientChannelFactoryFunction(clientProfileSettings);

            if (NetworkService.NETWORK_SERVER.get(settings)) {
                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    String profileName = profileSettings.profileName;
                    TcpChannelFactory factory = serverChannelFactory(profileSettings);
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

    protected void acceptChannel(NioSocketChannel channel) {
        serverAcceptedChannel((NioTcpChannel) channel);
    }

    protected TcpChannelFactory serverChannelFactory(ProfileSettings profileSettings) {
        return new TcpChannelFactoryImpl(profileSettings, false);
    }

    protected Function<DiscoveryNode, TcpChannelFactory> clientChannelFactoryFunction(ProfileSettings profileSettings) {
        return (n) -> new TcpChannelFactoryImpl(profileSettings, true);
    }

    protected abstract class TcpChannelFactory extends ChannelFactory<NioTcpServerChannel, NioTcpChannel> {

        protected TcpChannelFactory(ProfileSettings profileSettings) {
            super(profileSettings.tcpNoDelay, profileSettings.tcpKeepAlive, profileSettings.tcpKeepIdle, profileSettings.tcpKeepInterval,
                profileSettings.tcpKeepCount, profileSettings.reuseAddress, Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
                Math.toIntExact(profileSettings.receiveBufferSize.getBytes()));
        }
    }

    private class TcpChannelFactoryImpl extends TcpChannelFactory {

        private final boolean isClient;
        private final String profileName;

        private TcpChannelFactoryImpl(ProfileSettings profileSettings, boolean isClient) {
            super(profileSettings);
            this.isClient = isClient;
            this.profileName = profileSettings.profileName;
        }

        @Override
        public NioTcpChannel createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) {
            NioTcpChannel nioChannel = new NioTcpChannel(isClient == false, profileName, channel);
            TcpReadWriteHandler handler = new TcpReadWriteHandler(nioChannel, NioTransport.this);
            Consumer<Exception> exceptionHandler = (e) -> onException(nioChannel, e);
            BytesChannelContext context = new BytesChannelContext(nioChannel, selector, socketConfig, exceptionHandler, handler,
                new InboundChannelBuffer(pageAllocator));
            nioChannel.setContext(context);
            return nioChannel;
        }

        @Override
        public NioTcpServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel,
                                                       Config.ServerSocket socketConfig) {
            NioTcpServerChannel nioChannel = new NioTcpServerChannel(channel);
            Consumer<Exception> exceptionHandler = (e) -> onServerException(nioChannel, e);
            Consumer<NioSocketChannel> acceptor = NioTransport.this::acceptChannel;
            ServerChannelContext context = new ServerChannelContext(nioChannel, this, selector, socketConfig, acceptor, exceptionHandler);
            nioChannel.setContext(context);
            return nioChannel;
        }
    }
}
