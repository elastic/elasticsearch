/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.BytesChannelContext;
import org.elasticsearch.nio.BytesWriteHandler;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.Config;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSelectorGroup;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.Page;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.StatsTracker;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpServerChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportRequestOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

public class MockNioTransport extends TcpTransport {
    private static final Logger logger = LogManager.getLogger(MockNioTransport.class);

    private final ConcurrentMap<String, MockTcpChannelFactory> profileToChannelFactory = newConcurrentMap();
    private final TransportThreadWatchdog transportThreadWatchdog;
    private volatile NioSelectorGroup nioGroup;
    private volatile MockTcpChannelFactory clientChannelFactory;

    public MockNioTransport(Settings settings, Version version, ThreadPool threadPool, NetworkService networkService,
                            PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                            CircuitBreakerService circuitBreakerService) {
        super(settings, version, threadPool, pageCacheRecycler, circuitBreakerService, namedWriteableRegistry, networkService);
        this.transportThreadWatchdog = new TransportThreadWatchdog(threadPool, settings);
    }

    @Override
    protected MockServerChannel bind(String name, InetSocketAddress address) throws IOException {
        MockTcpChannelFactory channelFactory = this.profileToChannelFactory.get(name);
        MockServerChannel serverChannel = nioGroup.bindServerChannel(address, channelFactory);
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        serverChannel.addBindListener(ActionListener.toBiConsumer(future));
        future.actionGet();
        return serverChannel;
    }

    @Override
    protected MockSocketChannel initiateChannel(DiscoveryNode node) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        return nioGroup.openChannel(address, clientChannelFactory);
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            nioGroup = new NioSelectorGroup(daemonThreadFactory(this.settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX), 2,
                (s) -> new TestEventHandler(this::onNonChannelException, s, transportThreadWatchdog));

            ProfileSettings clientProfileSettings = new ProfileSettings(settings, "default");
            clientChannelFactory = new MockTcpChannelFactory(true, clientProfileSettings, "client");

            if (NetworkService.NETWORK_SERVER.get(settings)) {
                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    String profileName = profileSettings.profileName;
                    MockTcpChannelFactory factory = new MockTcpChannelFactory(false, profileSettings, profileName);
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
            transportThreadWatchdog.stop();
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
        builder.setPingInterval(connectionProfile.getPingInterval());
        builder.setCompressionEnabled(connectionProfile.getCompressionEnabled());
        builder.setCompressionScheme(connectionProfile.getCompressionScheme());
        return builder.build();
    }

    private void onNonChannelException(Exception exception) {
        logger.warn(new ParameterizedMessage("exception caught on transport layer [thread={}]", Thread.currentThread().getName()),
            exception);
    }

    private void exceptionCaught(NioSocketChannel channel, Exception exception) {
        onException((TcpChannel) channel, exception);
    }

    private void acceptChannel(NioSocketChannel channel) {
        serverAcceptedChannel((TcpChannel) channel);
    }

    private class MockTcpChannelFactory extends ChannelFactory<MockServerChannel, MockSocketChannel> {

        private final boolean isClient;
        private final String profileName;

        private MockTcpChannelFactory(boolean isClient, ProfileSettings profileSettings, String profileName) {
            super(profileSettings.tcpNoDelay,
                profileSettings.tcpKeepAlive,
                profileSettings.tcpKeepIdle,
                profileSettings.tcpKeepInterval,
                profileSettings.tcpKeepCount,
                profileSettings.reuseAddress,
                Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
                Math.toIntExact(profileSettings.receiveBufferSize.getBytes()));
            this.isClient = isClient;
            this.profileName = profileName;
        }

        @Override
        public MockSocketChannel createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) {
            MockSocketChannel nioChannel = new MockSocketChannel(isClient == false, profileName, channel);
            IntFunction<Page> pageSupplier = (length) -> {
                if (length > PageCacheRecycler.BYTE_PAGE_SIZE) {
                    return new Page(ByteBuffer.allocate(length), () -> {});
                } else {
                    Recycler.V<byte[]> bytes = pageCacheRecycler.bytePage(false);
                    return new Page(ByteBuffer.wrap(bytes.v(), 0, length), bytes);
                }
            };
            MockTcpReadWriteHandler readWriteHandler = new MockTcpReadWriteHandler(nioChannel, pageCacheRecycler, MockNioTransport.this);
            BytesChannelContext context = new BytesChannelContext(nioChannel, selector, socketConfig, e -> exceptionCaught(nioChannel, e),
                readWriteHandler, new InboundChannelBuffer(pageSupplier));
            nioChannel.setContext(context);
            nioChannel.addConnectListener((v, e) -> {
                if (e == null) {
                    if (channel.isConnected()) {
                        try {
                            channel.setOption(StandardSocketOptions.SO_LINGER, 0);
                        } catch (IOException ex) {
                            throw new UncheckedIOException(new IOException());
                        }
                    }
                }
            });
            return nioChannel;
        }

        @Override
        public MockServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel, Config.ServerSocket socketConfig) {
            MockServerChannel nioServerChannel = new MockServerChannel(channel);
            ServerChannelContext context = new ServerChannelContext(nioServerChannel, this, selector, socketConfig,
                MockNioTransport.this::acceptChannel, e -> onServerException(nioServerChannel, e)) {
                @Override
                public void acceptChannels(Supplier<NioSelector> selectorSupplier) throws IOException {
                    int acceptCount = 0;
                    SocketChannel acceptedChannel;
                    while ((acceptedChannel = accept(rawChannel)) != null) {
                        NioSocketChannel nioChannel = MockTcpChannelFactory.this.acceptNioChannel(acceptedChannel, selectorSupplier);
                        acceptChannel(nioChannel);
                        ++acceptCount;
                        if (acceptCount % 100 == 0) {
                            logger.warn("Accepted [{}] connections in a single select loop iteration on [{}]", acceptCount, channel);
                        }
                    }
                }
            };
            nioServerChannel.setContext(context);
            return nioServerChannel;
        }
    }

    private static final class LeakAwareRefCounted extends AbstractRefCounted {
        private final LeakTracker.Leak<Releasable> leak;

        private final Releasable releasable;

        LeakAwareRefCounted(Releasable releasable) {
            super("leak-aware-ref-counted");
            this.releasable = releasable;
            leak = LeakTracker.INSTANCE.track(releasable);
        }

        @Override
        protected void closeInternal() {
            boolean leakReleased = leak.close(releasable);
            assert leakReleased : "leak should not have been released already";
            Releasables.closeExpectNoException(releasable);
        }

        @Override
        protected void touch() {
            leak.record();
        }
    }

    private static class MockTcpReadWriteHandler extends BytesWriteHandler {

        private final MockSocketChannel channel;
        private final InboundPipeline pipeline;

        private MockTcpReadWriteHandler(MockSocketChannel channel, PageCacheRecycler recycler, TcpTransport transport) {
            this.channel = channel;
            final ThreadPool threadPool = transport.getThreadPool();
            final Supplier<CircuitBreaker> breaker = transport.getInflightBreaker();
            final RequestHandlers requestHandlers = transport.getRequestHandlers();
            final Version version = transport.getVersion();
            final StatsTracker statsTracker = transport.getStatsTracker();
            this.pipeline = new InboundPipeline(version, statsTracker, recycler, threadPool::relativeTimeInMillis, breaker,
                requestHandlers::getHandler, transport::inboundMessage);
        }

        @Override
        public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
            Page[] pages = channelBuffer.sliceAndRetainPagesTo(channelBuffer.getIndex());
            BytesReference[] references = new BytesReference[pages.length];
            for (int i = 0; i < pages.length; ++i) {
                references[i] = BytesReference.fromByteBuffer(pages[i].byteBuffer());
            }
            Releasable releasable = pages.length == 1 ? pages[0] : () -> Releasables.closeExpectNoException(pages);
            try (ReleasableBytesReference reference =
                         new ReleasableBytesReference(CompositeBytesReference.of(references), new LeakAwareRefCounted(releasable))) {
                pipeline.handleBytes(channel, reference);
                return reference.length();
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(pipeline, super::close);
        }
    }

    private static class MockServerChannel extends NioServerSocketChannel implements TcpServerChannel {

        MockServerChannel(ServerSocketChannel channel) {
            super(channel);
        }

        @Override
        public void close() {
            getContext().closeChannel();
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            addCloseListener(ActionListener.toBiConsumer(listener));
        }
    }

    private static class MockSocketChannel extends NioSocketChannel implements TcpChannel {

        private final boolean isServer;
        private final String profile;
        private final ChannelStats stats = new ChannelStats();

        private MockSocketChannel(boolean isServer, String profile, SocketChannel socketChannel) {
            super(socketChannel);
            this.isServer = isServer;
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
        public boolean isServerChannel() {
            return isServer;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            addCloseListener(ActionListener.toBiConsumer(listener));
        }

        @Override
        public void addConnectListener(ActionListener<Void> listener) {
            addConnectListener(ActionListener.toBiConsumer(listener));
        }

        @Override
        public ChannelStats getChannelStats() {
            return stats;
        }

        @Override
        public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
            getContext().sendMessage(BytesReference.toByteBuffers(reference), ActionListener.toBiConsumer(listener));
        }
    }

    static final class TransportThreadWatchdog {
        // Only check every 2s to not flood the logs on a blocked thread.
        // We mostly care about long blocks and not random slowness anyway and in tests would randomly catch slow operations that block for
        // less than 2s eventually.
        private static final TimeValue CHECK_INTERVAL = TimeValue.timeValueSeconds(2);

        private final long warnThreshold;
        private final ThreadPool threadPool;
        private final ConcurrentHashMap<Thread, Long> registry = new ConcurrentHashMap<>();

        private volatile boolean stopped;

        TransportThreadWatchdog(ThreadPool threadPool, Settings settings) {
            this.threadPool = threadPool;
            warnThreshold = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(settings).nanos() + TimeValue.timeValueMillis(100L).nanos();
            threadPool.schedule(this::logLongRunningExecutions, CHECK_INTERVAL, ThreadPool.Names.GENERIC);
        }

        public boolean register() {
            Long previousValue = registry.put(Thread.currentThread(), threadPool.relativeTimeInNanos());
            return previousValue == null;
        }

        public void unregister() {
            Long previousValue = registry.remove(Thread.currentThread());
            assert previousValue != null;
            maybeLogElapsedTime(previousValue);
        }

        private void maybeLogElapsedTime(long startTime) {
            long elapsedTime = threadPool.relativeTimeInNanos() - startTime;
            if (elapsedTime > warnThreshold) {
                logger.warn(
                    new ParameterizedMessage("Slow execution on network thread [{} milliseconds]",
                        TimeUnit.NANOSECONDS.toMillis(elapsedTime)),
                    new RuntimeException("Slow exception on network thread"));
            }
        }

        private void logLongRunningExecutions() {
            for (Map.Entry<Thread, Long> entry : registry.entrySet()) {
                final Long blockedSinceInNanos = entry.getValue();
                final long elapsedTimeInNanos = threadPool.relativeTimeInNanos() - blockedSinceInNanos;
                if (elapsedTimeInNanos > warnThreshold) {
                    final Thread thread = entry.getKey();
                    final String stackTrace =
                        Arrays.stream(thread.getStackTrace()).map(Object::toString).collect(Collectors.joining("\n"));
                    final Thread.State threadState = thread.getState();
                    if (blockedSinceInNanos == registry.get(thread)) {
                        logger.warn("Potentially blocked execution on network thread [{}] [{}] [{} milliseconds]: \n{}",
                            thread.getName(), threadState, TimeUnit.NANOSECONDS.toMillis(elapsedTimeInNanos), stackTrace);
                    }
                }
            }
            if (stopped == false) {
                threadPool.scheduleUnlessShuttingDown(CHECK_INTERVAL, ThreadPool.Names.GENERIC, this::logLongRunningExecutions);
            }
        }

        public void stop() {
            stopped = true;
        }
    }
}
