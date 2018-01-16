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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.AcceptingSelector;
import org.elasticsearch.nio.AcceptorEventHandler;
import org.elasticsearch.nio.BytesReadContext;
import org.elasticsearch.nio.BytesWriteContext;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ReadContext;
import org.elasticsearch.nio.SocketEventHandler;
import org.elasticsearch.nio.SocketSelector;
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
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class NioTransport extends TcpTransport {

    private static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = Transports.NIO_TRANSPORT_WORKER_THREAD_NAME_PREFIX;
    private static final String TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX = Transports.NIO_TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX;

    public static final Setting<Integer> NIO_WORKER_COUNT =
        new Setting<>("transport.nio.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.nio.worker_count"), Setting.Property.NodeScope);

    public static final Setting<Integer> NIO_ACCEPTOR_COUNT =
        intSetting("transport.nio.acceptor_count", 1, 1, Setting.Property.NodeScope);

    private final PageCacheRecycler pageCacheRecycler;
    private final ConcurrentMap<String, TcpChannelFactory> profileToChannelFactory = newConcurrentMap();
    private volatile NioGroup nioGroup;
    private volatile TcpChannelFactory clientChannelFactory;

    NioTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
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
            int acceptorCount = 0;
            boolean useNetworkServer = NetworkService.NETWORK_SERVER.get(settings);
            if (useNetworkServer) {
                acceptorCount = NioTransport.NIO_ACCEPTOR_COUNT.get(settings);
            }
            nioGroup = new NioGroup(logger, daemonThreadFactory(this.settings, TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX), acceptorCount,
                AcceptorEventHandler::new, daemonThreadFactory(this.settings, TRANSPORT_WORKER_THREAD_NAME_PREFIX),
                NioTransport.NIO_WORKER_COUNT.get(settings), SocketEventHandler::new);

            ProfileSettings clientProfileSettings = new ProfileSettings(settings, "default");
            clientChannelFactory = new TcpChannelFactory(clientProfileSettings);

            if (useNetworkServer) {
                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    String profileName = profileSettings.profileName;
                    TcpChannelFactory factory = new TcpChannelFactory(profileSettings);
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

    private void exceptionCaught(NioSocketChannel channel, Exception exception) {
        onException((TcpChannel) channel, exception);
    }

    private void acceptChannel(NioSocketChannel channel) {
        serverAcceptedChannel((TcpNioSocketChannel) channel);
    }

    private class TcpChannelFactory extends ChannelFactory<TcpNioServerSocketChannel, TcpNioSocketChannel> {

        private final String profileName;

        TcpChannelFactory(TcpTransport.ProfileSettings profileSettings) {
            super(new RawChannelFactory(profileSettings.tcpNoDelay,
                profileSettings.tcpKeepAlive,
                profileSettings.reuseAddress,
                Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
                Math.toIntExact(profileSettings.receiveBufferSize.getBytes())));
            this.profileName = profileSettings.profileName;
        }

        @Override
        public TcpNioSocketChannel createChannel(SocketSelector selector, SocketChannel channel) throws IOException {
            TcpNioSocketChannel nioChannel = new TcpNioSocketChannel(profileName, channel, selector);
            Supplier<InboundChannelBuffer.Page> pageSupplier = () -> {
                Recycler.V<byte[]> bytes = pageCacheRecycler.bytePage(false);
                return new InboundChannelBuffer.Page(ByteBuffer.wrap(bytes.v()), bytes::close);
            };
            ReadContext.ReadConsumer nioReadConsumer = channelBuffer ->
                consumeNetworkReads(nioChannel, BytesReference.fromByteBuffers(channelBuffer.sliceBuffersTo(channelBuffer.getIndex())));
            BytesReadContext readContext = new BytesReadContext(nioChannel, nioReadConsumer, new InboundChannelBuffer(pageSupplier));
            nioChannel.setContexts(readContext, new BytesWriteContext(nioChannel), NioTransport.this::exceptionCaught);
            return nioChannel;
        }

        @Override
        public TcpNioServerSocketChannel createServerChannel(AcceptingSelector selector, ServerSocketChannel channel) throws IOException {
            TcpNioServerSocketChannel nioServerChannel = new TcpNioServerSocketChannel(profileName, channel, this, selector);
            nioServerChannel.setAcceptContext(NioTransport.this::acceptChannel);
            return nioServerChannel;
        }
    }
}
