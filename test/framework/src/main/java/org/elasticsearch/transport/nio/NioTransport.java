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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class NioTransport extends TcpTransport<NioChannel> {

    // TODO: Need to add to places where we check if transport thread
    public static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = "transport_worker";
    public static final String TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX = "transport_acceptor";

    public static final Setting<Integer> NIO_WORKER_COUNT =
        new Setting<>("transport.nio.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.nio.worker_count"), Setting.Property.NodeScope);

    public static final Setting<Integer> NIO_ACCEPTOR_COUNT =
        intSetting("transport.nio.acceptor_count", 1, 1, Setting.Property.NodeScope);

    private final TcpReadHandler tcpReadHandler = new TcpReadHandler(this);
    private final BigArrays bigArrays;
    private final ConcurrentMap<String, ChannelFactory> profileToChannelFactory = newConcurrentMap();
    private final OpenChannels openChannels = new OpenChannels(logger);
    private final ArrayList<AcceptingSelector> acceptors = new ArrayList<>();
    private final ArrayList<SocketSelector> socketSelectors = new ArrayList<>();
    private NioClient client;
    private int acceptorNumber;

    public NioTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                        NamedWriteableRegistry namedWriteableRegistry, CircuitBreakerService circuitBreakerService) {
        super("nio", settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService);
        this.bigArrays = bigArrays;
    }

    @Override
    public long getNumOpenServerConnections() {
        return openChannels.serverChannelsCount();
    }

    @Override
    protected InetSocketAddress getLocalAddress(NioChannel channel) {
        return channel.getLocalAddress();
    }

    @Override
    protected NioServerSocketChannel bind(String name, InetSocketAddress address) throws IOException {
        ChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        NioServerSocketChannel serverSocketChannel = channelFactory.openNioServerSocketChannel(name, address);
        acceptors.get(++acceptorNumber % NioTransport.NIO_ACCEPTOR_COUNT.get(settings)).registerServerChannel(serverSocketChannel);
        return serverSocketChannel;
    }

    @Override
    protected void closeChannels(List<NioChannel> channels) throws IOException {
        IOException closingExceptions = null;
        for (final NioChannel channel : channels) {
            if (channel != null && channel.isOpen()) {
                try {
                    channel.closeAsync().awaitClose();
                } catch (Exception e) {
                    if (closingExceptions == null) {
                        closingExceptions = new IOException("failed to close channels");
                    }
                    closingExceptions.addSuppressed(e.getCause());
                }
            }
        }

        if (closingExceptions != null) {
            throw closingExceptions;
        }
    }

    @Override
    protected void sendMessage(NioChannel channel, BytesReference reference, ActionListener<NioChannel> listener) {
        if (channel instanceof NioSocketChannel) {
            NioSocketChannel nioSocketChannel = (NioSocketChannel) channel;
            nioSocketChannel.getWriteContext().sendMessage(reference, listener);
        } else {
            logger.error("cannot send message to channel of this type [{}]", channel.getClass());
        }
    }

    @Override
    protected NodeChannels connectToChannels(DiscoveryNode node, ConnectionProfile profile, Consumer<NioChannel> onChannelClose)
        throws IOException {
        NioSocketChannel[] channels = new NioSocketChannel[profile.getNumConnections()];
        ClientChannelCloseListener closeListener = new ClientChannelCloseListener(onChannelClose);
        boolean connected = client.connectToChannels(node, channels, profile.getConnectTimeout(), closeListener);
        if (connected == false) {
            throw new ElasticsearchException("client is shutdown");
        }
        return new NodeChannels(node, channels, profile);
    }

    @Override
    protected boolean isOpen(NioChannel channel) {
        return channel.isOpen();
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                int workerCount = NioTransport.NIO_WORKER_COUNT.get(settings);
                for (int i = 0; i < workerCount; ++i) {
                    SocketSelector selector = new SocketSelector(getSocketEventHandler());
                    socketSelectors.add(selector);
                }

                int acceptorCount = NioTransport.NIO_ACCEPTOR_COUNT.get(settings);
                for (int i = 0; i < acceptorCount; ++i) {
                    Supplier<SocketSelector> selectorSupplier = new RoundRobinSelectorSupplier(socketSelectors);
                    AcceptorEventHandler eventHandler = new AcceptorEventHandler(logger, openChannels, selectorSupplier);
                    AcceptingSelector acceptor = new AcceptingSelector(eventHandler);
                    acceptors.add(acceptor);
                }
                // loop through all profiles and start them up, special handling for default one
                for (Map.Entry<String, Settings> entry : buildProfileSettings().entrySet()) {
                    // merge fallback settings with default settings with profile settings so we have complete settings with default values
                    final Settings settings = Settings.builder()
                        .put(createFallbackSettings())
                        .put(entry.getValue()).build();
                    profileToChannelFactory.putIfAbsent(entry.getKey(), new ChannelFactory(settings, tcpReadHandler));
                    bindServer(entry.getKey(), settings);
                }
            }
            client = createClient();

            for (SocketSelector selector : socketSelectors) {
                if (selector.isRunning() == false) {
                    ThreadFactory threadFactory = daemonThreadFactory(this.settings, TRANSPORT_WORKER_THREAD_NAME_PREFIX);
                    threadFactory.newThread(selector::runLoop).start();
                }
            }

            for (AcceptingSelector acceptor : acceptors) {
                ThreadFactory threadFactory = daemonThreadFactory(this.settings, TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX);
                threadFactory.newThread(acceptor::runLoop).start();
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
        nioShutdown.orderlyShutdown(openChannels, client, acceptors, socketSelectors);

        profileToChannelFactory.clear();
        socketSelectors.clear();
    }

    protected SocketEventHandler getSocketEventHandler() {
        return new SocketEventHandler(logger, this::exceptionCaught);
    }

    final void exceptionCaught(NioSocketChannel channel, Throwable cause) {
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable t = unwrapped != null ? unwrapped : cause;
        onException(channel, t instanceof Exception ? (Exception) t : new ElasticsearchException(t));
    }

    private Settings createFallbackSettings() {
        Settings.Builder fallbackSettingsBuilder = Settings.builder();

        List<String> fallbackBindHost = TransportSettings.BIND_HOST.get(settings);
        if (fallbackBindHost.isEmpty() == false) {
            fallbackSettingsBuilder.putArray("bind_host", fallbackBindHost);
        }

        List<String> fallbackPublishHost = TransportSettings.PUBLISH_HOST.get(settings);
        if (fallbackPublishHost.isEmpty() == false) {
            fallbackSettingsBuilder.putArray("publish_host", fallbackPublishHost);
        }

        boolean fallbackTcpNoDelay = settings.getAsBoolean("transport.nio.tcp_no_delay",
            NetworkService.TcpSettings.TCP_NO_DELAY.get(settings));
        fallbackSettingsBuilder.put("tcp_no_delay", fallbackTcpNoDelay);

        boolean fallbackTcpKeepAlive = settings.getAsBoolean("transport.nio.tcp_keep_alive",
            NetworkService.TcpSettings.TCP_KEEP_ALIVE.get(settings));
        fallbackSettingsBuilder.put("tcp_keep_alive", fallbackTcpKeepAlive);

        boolean fallbackReuseAddress = settings.getAsBoolean("transport.nio.reuse_address",
            NetworkService.TcpSettings.TCP_REUSE_ADDRESS.get(settings));
        fallbackSettingsBuilder.put("reuse_address", fallbackReuseAddress);

        ByteSizeValue fallbackTcpSendBufferSize = settings.getAsBytesSize("transport.nio.tcp_send_buffer_size",
            TCP_SEND_BUFFER_SIZE.get(settings));
        if (fallbackTcpSendBufferSize.getBytes() >= 0) {
            fallbackSettingsBuilder.put("tcp_send_buffer_size", fallbackTcpSendBufferSize);
        }

        ByteSizeValue fallbackTcpBufferSize = settings.getAsBytesSize("transport.nio.tcp_receive_buffer_size",
            TCP_RECEIVE_BUFFER_SIZE.get(settings));
        if (fallbackTcpBufferSize.getBytes() >= 0) {
            fallbackSettingsBuilder.put("tcp_receive_buffer_size", fallbackTcpBufferSize);
        }

        return fallbackSettingsBuilder.build();
    }

    private NioClient createClient() {
        Supplier<SocketSelector> selectorSupplier = new RoundRobinSelectorSupplier(socketSelectors);
        ChannelFactory channelFactory = new ChannelFactory(settings, tcpReadHandler);
        return new NioClient(logger, openChannels, selectorSupplier, defaultConnectionProfile.getConnectTimeout(), channelFactory);
    }

    class ClientChannelCloseListener implements Consumer<NioChannel> {

        private final Consumer<NioChannel> consumer;

        private ClientChannelCloseListener(Consumer<NioChannel> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void accept(final NioChannel channel) {
            consumer.accept(channel);
            openChannels.channelClosed(channel);
        }
    }
}
