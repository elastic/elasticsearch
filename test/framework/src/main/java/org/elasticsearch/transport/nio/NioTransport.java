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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.TcpChannelFactory;
import org.elasticsearch.transport.nio.channel.TcpNioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.TcpNioSocketChannel;
import org.elasticsearch.transport.nio.channel.TcpReadContext;
import org.elasticsearch.transport.nio.channel.TcpWriteContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class NioTransport extends TcpTransport {

    public static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = Transports.NIO_TRANSPORT_WORKER_THREAD_NAME_PREFIX;
    public static final String TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX = Transports.NIO_TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX;

    public static final Setting<Integer> NIO_WORKER_COUNT =
        new Setting<>("transport.nio.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.nio.worker_count"), Setting.Property.NodeScope);

    public static final Setting<Integer> NIO_ACCEPTOR_COUNT =
        intSetting("transport.nio.acceptor_count", 1, 1, Setting.Property.NodeScope);

    private final PageCacheRecycler pageCacheRecycler;
    private final ConcurrentMap<String, TcpChannelFactory> profileToChannelFactory = newConcurrentMap();
    private final ArrayList<AcceptingSelector> acceptors = new ArrayList<>();
    private final ArrayList<SocketSelector> socketSelectors = new ArrayList<>();
    private RoundRobinSelectorSupplier clientSelectorSupplier;
    private TcpChannelFactory clientChannelFactory;
    private int acceptorNumber;

    public NioTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                        PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                        CircuitBreakerService circuitBreakerService) {
        super("nio", settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService);
        this.pageCacheRecycler = pageCacheRecycler;
    }

    @Override
    protected TcpNioServerSocketChannel bind(String name, InetSocketAddress address) throws IOException {
        TcpChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        AcceptingSelector selector = acceptors.get(++acceptorNumber % NioTransport.NIO_ACCEPTOR_COUNT.get(settings));
        return channelFactory.openNioServerSocketChannel(address, selector);
    }

    @Override
    protected TcpNioSocketChannel initiateChannel(DiscoveryNode node, TimeValue connectTimeout, ActionListener<Void> connectListener)
        throws IOException {
        TcpNioSocketChannel channel = clientChannelFactory.openNioChannel(node.getAddress().address(), clientSelectorSupplier.get());
        channel.addConnectListener(connectListener);
        return channel;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            int workerCount = NioTransport.NIO_WORKER_COUNT.get(settings);
            for (int i = 0; i < workerCount; ++i) {
                SocketSelector selector = new SocketSelector(getSocketEventHandler());
                socketSelectors.add(selector);
            }

            for (SocketSelector selector : socketSelectors) {
                if (selector.isRunning() == false) {
                    ThreadFactory threadFactory = daemonThreadFactory(this.settings, TRANSPORT_WORKER_THREAD_NAME_PREFIX);
                    threadFactory.newThread(selector::runLoop).start();
                    selector.isRunningFuture().actionGet();
                }
            }

            Consumer<NioSocketChannel> clientContextSetter = getContextSetter("client-socket");
            clientSelectorSupplier = new RoundRobinSelectorSupplier(socketSelectors);
            ProfileSettings clientProfileSettings = new ProfileSettings(settings, "default");
            clientChannelFactory = new TcpChannelFactory(clientProfileSettings, clientContextSetter, getServerContextSetter());

            if (NetworkService.NETWORK_SERVER.get(settings)) {
                int acceptorCount = NioTransport.NIO_ACCEPTOR_COUNT.get(settings);
                for (int i = 0; i < acceptorCount; ++i) {
                    Supplier<SocketSelector> selectorSupplier = new RoundRobinSelectorSupplier(socketSelectors);
                    AcceptorEventHandler eventHandler = new AcceptorEventHandler(logger, selectorSupplier);
                    AcceptingSelector acceptor = new AcceptingSelector(eventHandler);
                    acceptors.add(acceptor);
                }

                for (AcceptingSelector acceptor : acceptors) {
                    if (acceptor.isRunning() == false) {
                        ThreadFactory threadFactory = daemonThreadFactory(this.settings, TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX);
                        threadFactory.newThread(acceptor::runLoop).start();
                        acceptor.isRunningFuture().actionGet();
                    }
                }

                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    String profileName = profileSettings.profileName;
                    Consumer<NioSocketChannel> contextSetter = getContextSetter(profileName);
                    TcpChannelFactory factory = new TcpChannelFactory(profileSettings, contextSetter, getServerContextSetter());
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
        NioShutdown nioShutdown = new NioShutdown(logger);
        nioShutdown.orderlyShutdown(acceptors, socketSelectors);

        profileToChannelFactory.clear();
        socketSelectors.clear();
    }

    protected SocketEventHandler getSocketEventHandler() {
        return new SocketEventHandler(logger);
    }

    final void exceptionCaught(NioSocketChannel channel, Exception exception) {
        onException((TcpNioSocketChannel) channel, exception);
    }

    private Consumer<NioSocketChannel> getContextSetter(String profileName) {
        return (c) -> {
            Supplier<InboundChannelBuffer.Page> pageSupplier = () -> {
                Recycler.V<byte[]> bytes = pageCacheRecycler.bytePage(false);
                return new InboundChannelBuffer.Page(ByteBuffer.wrap(bytes.v()), bytes);
            };
            c.setContexts(new TcpReadContext(c, new TcpReadHandler(profileName, this), new InboundChannelBuffer(pageSupplier)),
                new TcpWriteContext(c), this::exceptionCaught);
        };
    }

    private void acceptChannel(NioSocketChannel channel) {
        TcpNioSocketChannel tcpChannel = (TcpNioSocketChannel) channel;
        serverAcceptedChannel(tcpChannel);

    }

    private Consumer<NioServerSocketChannel> getServerContextSetter() {
        return (c) -> c.setAcceptContext(this::acceptChannel);
    }
}
