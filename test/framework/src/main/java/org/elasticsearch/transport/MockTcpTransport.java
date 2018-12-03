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
package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.concurrent.CompletableContext;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.mocksocket.MockSocket;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a socket based blocking TcpTransport implementation that is used for tests
 * that need real networking. This implementation is a test only implementation that implements
 * the networking layer in the worst possible way since it blocks and uses a thread per request model.
 */
public class MockTcpTransport extends TcpTransport {
    private static final Logger logger = LogManager.getLogger(MockTcpTransport.class);

    /**
     * A pre-built light connection profile that shares a single connection across all
     * types.
     */
    static final ConnectionProfile LIGHT_PROFILE;

    private final Set<MockChannel> openChannels = new HashSet<>();

    static {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE);
        LIGHT_PROFILE = builder.build();
    }

    private final ExecutorService executor;

    public MockTcpTransport(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                            CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                            NetworkService networkService) {
        this(settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService,
            Version.CURRENT);
    }

    public MockTcpTransport(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                            CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                            NetworkService networkService, Version mockVersion) {
        super("mock-tcp-transport", settings, mockVersion, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry,
            networkService);
        // we have our own crazy cached threadpool this one is not bounded at all...
        // using the ES thread factory here is crucial for tests otherwise disruption tests won't block that thread
        executor = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory(settings, Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX));
    }

    @Override
    protected MockChannel bind(final String name, InetSocketAddress address) throws IOException {
        MockServerSocket socket = new MockServerSocket();
        socket.setReuseAddress(TCP_REUSE_ADDRESS.get(settings));
        ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.getBytes() > 0) {
            socket.setReceiveBufferSize(tcpReceiveBufferSize.bytesAsInt());
        }
        socket.bind(address);
        MockChannel serverMockChannel = new MockChannel(socket, name);
        CountDownLatch started = new CountDownLatch(1);
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                onException(serverMockChannel, e);
            }

            @Override
            protected void doRun() throws Exception {
                started.countDown();
                serverMockChannel.accept(executor);
            }
        });
        try {
            started.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return serverMockChannel;
    }

    private void readMessage(MockChannel mockChannel, StreamInput input) throws IOException {
        Socket socket = mockChannel.activeChannel;
        byte[] minimalHeader = new byte[TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE];
        try {
            input.readFully(minimalHeader);
        } catch (EOFException eof) {
            throw new IOException("Connection reset by peer");
        }

        // Read message length will throw stream corrupted exception if the marker bytes incorrect
        int msgSize = TcpTransport.readMessageLength(new BytesArray(minimalHeader));
        if (msgSize == -1) {
            socket.getOutputStream().flush();
        } else {
            final byte[] buffer = new byte[msgSize];
            input.readFully(buffer);
            int expectedSize = TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE + msgSize;
            try (BytesStreamOutput output = new ReleasableBytesStreamOutput(expectedSize, bigArrays)) {
                output.write(minimalHeader);
                output.write(buffer);
                consumeNetworkReads(mockChannel, output.bytes());
            }
        }
    }

    @Override
    @SuppressForbidden(reason = "real socket for mocking remote connections")
    protected MockChannel initiateChannel(DiscoveryNode node) throws IOException {
        InetSocketAddress address = node.getAddress().address();
        final MockSocket socket = new MockSocket();
        final MockChannel channel = new MockChannel(socket, address, false, "none");

        boolean success = false;
        try {
            configureSocket(socket);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.close(socket);
            }
        }

        executor.submit(() -> {
            try {
                socket.connect(address);
                socket.setSoLinger(false, 0);
                channel.connectFuture.complete(null);
                channel.loopRead(executor);
            } catch (Exception ex) {
                channel.connectFuture.completeExceptionally(ex);
            }
        });

        return channel;
    }

    @Override
    protected ConnectionProfile maybeOverrideConnectionProfile(ConnectionProfile connectionProfile) {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        Set<TransportRequestOptions.Type> allTypesWithConnection = new HashSet<>();
        Set<TransportRequestOptions.Type> allTypesWithoutConnection = new HashSet<>();
        for (ConnectionProfile.ConnectionTypeHandle handle : connectionProfile.getHandles()) {
            Set<TransportRequestOptions.Type> types = handle.getTypes();
            if (handle.length > 0) {
                allTypesWithConnection.addAll(types);
            } else {
                allTypesWithoutConnection.addAll(types);
            }
        }
        // make sure we maintain at least the types that are supported by this profile even if we only use a single channel for them.
        builder.addConnections(1, allTypesWithConnection.toArray(new TransportRequestOptions.Type[0]));
        if (allTypesWithoutConnection.isEmpty() == false) {
            builder.addConnections(0, allTypesWithoutConnection.toArray(new TransportRequestOptions.Type[0]));
        }
        builder.setHandshakeTimeout(connectionProfile.getHandshakeTimeout());
        builder.setConnectTimeout(connectionProfile.getConnectTimeout());
        builder.setPingInterval(connectionProfile.getPingInterval());
        builder.setCompressionEnabled(connectionProfile.getCompressionEnabled());
        return builder.build();
    }

    private void configureSocket(Socket socket) throws SocketException {
        socket.setTcpNoDelay(TCP_NO_DELAY.get(settings));
        ByteSizeValue tcpSendBufferSize = TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.getBytes() > 0) {
            socket.setSendBufferSize(tcpSendBufferSize.bytesAsInt());
        }
        ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.getBytes() > 0) {
            socket.setReceiveBufferSize(tcpReceiveBufferSize.bytesAsInt());
        }
        socket.setReuseAddress(TCP_REUSE_ADDRESS.get(settings));
    }

    public final class MockChannel implements Closeable, TcpChannel, TcpServerChannel {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final InetSocketAddress localAddress;
        private final ServerSocket serverSocket;
        private final Set<MockChannel> workerChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private final Socket activeChannel;
        private final boolean isServer;
        private final String profile;
        private final CancellableThreads cancellableThreads = new CancellableThreads();
        private final CompletableContext<Void> closeFuture = new CompletableContext<>();
        private final CompletableContext<Void> connectFuture = new CompletableContext<>();
        private final ChannelStats stats = new ChannelStats();

        /**
         * Constructs a new MockChannel instance intended for handling the actual incoming / outgoing traffic.
         *
         * @param socket The client socket. Mut not be null.
         * @param localAddress Address associated with the corresponding local server socket. Must not be null.
         * @param profile The associated profile name.
         */
        MockChannel(Socket socket, InetSocketAddress localAddress, boolean isServer, String profile) {
            this.localAddress = localAddress;
            this.activeChannel = socket;
            this.isServer = isServer;
            this.serverSocket = null;
            this.profile = profile;
            synchronized (openChannels) {
                openChannels.add(this);
            }
        }

        /**
         * Constructs a new MockChannel instance intended for accepting requests.
         *
         * @param serverSocket The associated server socket. Must not be null.
         * @param profile The associated profile name.
         */
        MockChannel(ServerSocket serverSocket, String profile) {
            this.localAddress = (InetSocketAddress) serverSocket.getLocalSocketAddress();
            this.serverSocket = serverSocket;
            this.profile = profile;
            this.isServer = false;
            this.activeChannel = null;
            synchronized (openChannels) {
                openChannels.add(this);
            }
        }

        public void accept(Executor executor) throws IOException {
            while (isOpen.get()) {
                Socket incomingSocket = serverSocket.accept();
                MockChannel incomingChannel = null;
                try {
                    configureSocket(incomingSocket);
                    synchronized (this) {
                        if (isOpen.get()) {
                            InetSocketAddress localAddress = new InetSocketAddress(incomingSocket.getLocalAddress(),
                                incomingSocket.getPort());
                            incomingChannel = new MockChannel(incomingSocket, localAddress, true, profile);
                            MockChannel finalIncomingChannel = incomingChannel;
                            incomingChannel.addCloseListener(new ActionListener<Void>() {
                                @Override
                                public void onResponse(Void aVoid) {
                                    workerChannels.remove(finalIncomingChannel);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    workerChannels.remove(finalIncomingChannel);
                                }
                            });
                            serverAcceptedChannel(incomingChannel);
                            //establish a happens-before edge between closing and accepting a new connection
                            workerChannels.add(incomingChannel);

                            // this spawns a new thread immediately, so OK under lock
                            incomingChannel.loopRead(executor);
                            // the channel is properly registered and will be cleared by the close code.
                            incomingSocket = null;
                            incomingChannel = null;
                        }
                    }
                } finally {
                    // ensure we don't leak sockets and channels in the failure case. Note that we null both
                    // if there are no exceptions so this becomes a no op.
                    IOUtils.closeWhileHandlingException(incomingSocket, incomingChannel);
                }
            }
        }

        void loopRead(Executor executor) {
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    if (isOpen.get()) {
                        try {
                            onException(MockChannel.this, e);
                        } catch (Exception ex) {
                            logger.warn("failed on handling exception", ex);
                            IOUtils.closeWhileHandlingException(MockChannel.this); // pure paranoia
                        }
                    }
                }

                @Override
                protected void doRun() throws Exception {
                    StreamInput input = new InputStreamStreamInput(new BufferedInputStream(activeChannel.getInputStream()));
                    // There is a (slim) chance that we get interrupted right after a loop iteration, so check explicitly
                    while (isOpen.get() && !Thread.currentThread().isInterrupted()) {
                        cancellableThreads.executeIO(() -> readMessage(MockChannel.this, input));
                    }
                }
            });
        }

        synchronized void close0() throws IOException {
            // establish a happens-before edge between closing and accepting a new connection
            // we have to sync this entire block to ensure that our openChannels checks work correctly.
            // The close block below will close all worker channels but if one of the worker channels runs into an exception
            // for instance due to a disconnect the handling of this exception might be executed concurrently.
            // now if we are in-turn concurrently call close we might not wait for the actual close to happen and that will, down the road
            // make the assertion trip that not all channels are closed.
            if (isOpen.compareAndSet(true, false)) {
                final boolean removedChannel;
                synchronized (openChannels) {
                    removedChannel = openChannels.remove(this);
                }
                IOUtils.close(serverSocket, activeChannel, () -> IOUtils.close(workerChannels),
                    () -> cancellableThreads.cancel("channel closed"));
                assert removedChannel: "Channel was not removed or removed twice?";
            }
        }

        @Override
        public String toString() {
            return "MockChannel{" +
                "profile='" + profile + '\'' +
                ", isOpen=" + isOpen +
                ", localAddress=" + localAddress +
                ", isServerSocket=" + (serverSocket != null) +
                '}';
        }

        @Override
        public void close() {
            try {
                close0();
                closeFuture.complete(null);
            } catch (IOException e) {
                closeFuture.completeExceptionally(e);
            }
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
            closeFuture.addListener(ActionListener.toBiConsumer(listener));
        }

        @Override
        public void addConnectListener(ActionListener<Void> listener) {
            connectFuture.addListener(ActionListener.toBiConsumer(listener));
        }

        @Override
        public ChannelStats getChannelStats() {
            return stats;
        }

        @Override
        public boolean isOpen() {
            return isOpen.get();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return localAddress;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return (InetSocketAddress) activeChannel.getRemoteSocketAddress();
        }

        @Override
        public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
            try {
                synchronized (this) {
                    OutputStream outputStream = new BufferedOutputStream(activeChannel.getOutputStream());
                    reference.writeTo(outputStream);
                    outputStream.flush();
                }
                listener.onResponse(null);
            } catch (IOException e) {
                listener.onFailure(e);
                onException(this, e);
            }
        }
    }


    @Override
    protected void doStart() {
        boolean success = false;
        try {
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                // loop through all profiles and start them up, special handling for default one
                for (ProfileSettings profileSettings : profileSettings) {
                    bindServer(profileSettings);
                }
            }
            super.doStart();
            success = true;
        } finally {
            if (success == false) {
                doStop();
            }
        }
    }

    @Override
    protected void stopInternal() {
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        synchronized (openChannels) {
            assert openChannels.isEmpty() : "there are still open channels: " + openChannels;
        }
    }
}

