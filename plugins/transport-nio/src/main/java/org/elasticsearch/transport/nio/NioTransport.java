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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.BytesChannelContext;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.EventHandler;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class NioTransport extends TcpTransport {

    private static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = Transports.NIO_TRANSPORT_WORKER_THREAD_NAME_PREFIX;

    public static final Setting<Integer> NIO_WORKER_COUNT =
        new Setting<>("transport.nio.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.nio.worker_count"), Setting.Property.NodeScope);

    protected final PageCacheRecycler pageCacheRecycler;
    private final ConcurrentMap<String, TcpChannelFactory> profileToChannelFactory = newConcurrentMap();
    private volatile NioGroup nioGroup;
    private volatile TcpChannelFactory clientChannelFactory;

    protected NioTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                 PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                 CircuitBreakerService circuitBreakerService) {
        super("nio", settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService);
        this.pageCacheRecycler = pageCacheRecycler;
    }

    @Override
    protected TcpNioServerSocketChannel bind(String name, InetSocketAddress address) throws IOException {
        TcpChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        return nioGroup.bindServerChannel(address, channelFactory);
    }

    @Override
    protected TcpNioSocketChannel initiateChannel(InetSocketAddress address, ActionListener<Void> connectListener) throws IOException {
        TcpNioSocketChannel channel = nioGroup.openChannel(address, clientChannelFactory);
        channel.addConnectListener(ActionListener.toBiConsumer(connectListener));
        return channel;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            nioGroup = new NioGroup(daemonThreadFactory(this.settings, TRANSPORT_WORKER_THREAD_NAME_PREFIX),
                NioTransport.NIO_WORKER_COUNT.get(settings), (s) -> new EventHandler(this::onNonChannelException, s));

            ProfileSettings clientProfileSettings = new ProfileSettings(settings, "default");
            clientChannelFactory = channelFactory(clientProfileSettings, true);

            if (NetworkService.NETWORK_SERVER.get(settings)) {
                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    String profileName = profileSettings.profileName;
                    TcpChannelFactory factory = channelFactory(profileSettings, false);
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

    protected void exceptionCaught(NioSocketChannel channel, Exception exception) {
        onException((TcpChannel) channel, exception);
    }

    protected void acceptChannel(NioSocketChannel channel) {
        serverAcceptedChannel((TcpNioSocketChannel) channel);
    }

    protected TcpChannelFactory channelFactory(ProfileSettings settings, boolean isClient) {
        return new TcpChannelFactoryImpl(settings);
    }

    protected abstract class TcpChannelFactory extends ChannelFactory<TcpNioServerSocketChannel, TcpNioSocketChannel> {

        protected TcpChannelFactory(RawChannelFactory rawChannelFactory) {
            super(rawChannelFactory);
        }
    }

    private class TcpChannelFactoryImpl extends TcpChannelFactory {

        private final String profileName;

        private TcpChannelFactoryImpl(ProfileSettings profileSettings) {
            super(new RawChannelFactory(profileSettings.tcpNoDelay,
                profileSettings.tcpKeepAlive,
                profileSettings.reuseAddress,
                Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
                Math.toIntExact(profileSettings.receiveBufferSize.getBytes())));
            this.profileName = profileSettings.profileName;
        }

        @Override
        public TcpNioSocketChannel createChannel(NioSelector selector, SocketChannel channel) throws IOException {
            TcpNioSocketChannel nioChannel = new TcpNioSocketChannel(profileName, channel);
            Supplier<InboundChannelBuffer.Page> pageSupplier = () -> {
                Recycler.V<byte[]> bytes = pageCacheRecycler.bytePage(false);
                return new InboundChannelBuffer.Page(ByteBuffer.wrap(bytes.v()), bytes::close);
            };
            TcpReadWriteHandler readWriteHandler = new TcpReadWriteHandler(nioChannel, NioTransport.this);
            Consumer<Exception> exceptionHandler = (e) -> exceptionCaught(nioChannel, e);
            BytesChannelContext context = new BytesChannelContext(nioChannel, selector, exceptionHandler, readWriteHandler,
                new InboundChannelBuffer(pageSupplier));
            nioChannel.setContext(context);
            return nioChannel;
        }

        @Override
        public TcpNioServerSocketChannel createServerChannel(NioSelector selector, ServerSocketChannel channel) throws IOException {
            TcpNioServerSocketChannel nioChannel = new TcpNioServerSocketChannel(profileName, channel);
            Consumer<Exception> exceptionHandler = (e) -> logger.error(() ->
                new ParameterizedMessage("exception from server channel caught on transport layer [{}]", channel), e);
            Consumer<NioSocketChannel> acceptor = NioTransport.this::acceptChannel;
            ServerChannelContext context = new ServerChannelContext(nioChannel, this, selector, acceptor, exceptionHandler);
            nioChannel.setContext(context);
            return nioChannel;
        }
    }
}
