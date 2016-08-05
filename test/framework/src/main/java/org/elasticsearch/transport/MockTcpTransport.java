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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * This is a socket based blocking TcpTransport implementation that is used for tests
 * that need real networking. This implementation is a test only implementation that implements
 * the networking layer in the worst possible way since it blocks and uses a thread per request model.
 */
public class MockTcpTransport extends TcpTransport<MockTcpTransport.MockChannel> {

    private final ExecutorService executor;
    private final Version mockVersion;

    @Inject
    public MockTcpTransport(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                            CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                            NetworkService networkService) {
        this(settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService,
            Version.CURRENT);
    }

    public MockTcpTransport(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                            CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                            NetworkService networkService, Version mockVersion) {
        super("mock-tcp-transport", settings, threadPool, bigArrays, circuitBreakerService, namedWriteableRegistry, networkService);
        // we have our own crazy cached threadpool this one is not bounded at all...
        // using the ES thread factory here is crucial for tests otherwise disruption tests won't block that thread
        executor = Executors.newCachedThreadPool(EsExecutors.daemonThreadFactory(settings, Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX));
        this.mockVersion = mockVersion;
    }

    @Override
    protected InetSocketAddress getLocalAddress(MockChannel mockChannel) {
        return mockChannel.localAddress;
    }

    @Override
    protected MockChannel bind(final String name, InetSocketAddress address) throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.bind(address);
        socket.setReuseAddress(TCP_REUSE_ADDRESS.get(settings()));
        ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.bytes() > 0) {
            socket.setReceiveBufferSize(tcpReceiveBufferSize.bytesAsInt());
        }
        MockChannel serverMockChannel = new MockChannel(socket, name);
        CountDownLatch started = new CountDownLatch(1);
        executor.execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                try {
                    onException(serverMockChannel, e);
                } catch (IOException ex) {
                    logger.warn("failed on handling exception", ex);
                }
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
        byte[] minimalHeader = new byte[TcpHeader.MARKER_BYTES_SIZE];
        int firstByte = input.read();
        if (firstByte == -1) {
            throw new IOException("Connection reset by peer");
        }
        minimalHeader[0] = (byte) firstByte;
        minimalHeader[1] = (byte) input.read();
        int msgSize = input.readInt();
        if (msgSize == -1) {
            socket.getOutputStream().flush();
        } else {
            BytesStreamOutput output = new BytesStreamOutput();
            final byte[] buffer = new byte[msgSize];
            input.readFully(buffer);
            output.write(minimalHeader);
            output.writeInt(msgSize);
            output.write(buffer);
            final BytesReference bytes = output.bytes();
            if (TcpTransport.validateMessageHeader(bytes)) {
                InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
                messageReceived(bytes.slice(TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE, msgSize),
                    mockChannel, mockChannel.profile, remoteAddress, msgSize);
            } else {
                // ping message - we just drop all stuff
            }
        }
    }

    @Override
    protected NodeChannels connectToChannelsLight(DiscoveryNode node) throws IOException {
        return connectToChannels(node);
    }

    @Override
    protected NodeChannels connectToChannels(DiscoveryNode node) throws IOException {
        final NodeChannels nodeChannels = new NodeChannels(new MockChannel[1],
            new MockChannel[1],
            new MockChannel[1],
            new MockChannel[1],
            new MockChannel[1]);
        boolean success = false;
        final Socket socket = new Socket();
        try {
            Consumer<MockChannel> onClose = (channel) -> {
                final NodeChannels connected = connectedNodes.get(node);
                if (connected != null && connected.hasChannel(channel)) {
                    try {
                        executor.execute(() -> {
                            disconnectFromNode(node, channel, "channel closed event");
                        });
                    } catch (RejectedExecutionException ex) {
                        logger.debug("failed to run disconnectFromNode - node is shutting down");
                    }
                }
            };
            InetSocketAddress address = ((InetSocketTransportAddress) node.getAddress()).address();
            // we just use a single connections
            configureSocket(socket);
            socket.connect(address, (int) TCP_CONNECT_TIMEOUT.get(settings).millis());
            MockChannel channel = new MockChannel(socket, address, "none", onClose);
            channel.loopRead(executor);
            for (MockChannel[] channels : nodeChannels.getChannelArrays()) {
                for (int i = 0; i < channels.length; i++) {
                    channels[i] = channel;
                }
            }
            success = true;
        } finally {
            if (success == false) {
                IOUtils.close(nodeChannels, socket);
            }
        }

        return nodeChannels;
    }



    private void configureSocket(Socket socket) throws SocketException {
        socket.setTcpNoDelay(TCP_NO_DELAY.get(settings));
        ByteSizeValue tcpSendBufferSize = TCP_SEND_BUFFER_SIZE.get(settings);
        if (tcpSendBufferSize.bytes() > 0) {
            socket.setSendBufferSize(tcpSendBufferSize.bytesAsInt());
        }
        ByteSizeValue tcpReceiveBufferSize = TCP_RECEIVE_BUFFER_SIZE.get(settings);
        if (tcpReceiveBufferSize.bytes() > 0) {
            socket.setReceiveBufferSize(tcpReceiveBufferSize.bytesAsInt());
        }
        socket.setReuseAddress(TCP_REUSE_ADDRESS.get(settings()));
    }

    @Override
    protected boolean isOpen(MockChannel mockChannel) {
        return mockChannel.isOpen.get();
    }

    @Override
    protected void sendMessage(MockChannel mockChannel, BytesReference reference, Runnable sendListener, boolean close) throws IOException {
        synchronized (mockChannel) {
            final Socket socket = mockChannel.activeChannel;
            OutputStream outputStream = new BufferedOutputStream(socket.getOutputStream());
            reference.writeTo(outputStream);
            outputStream.flush();
        }
        if (sendListener != null) {
            sendListener.run();
        }
        if (close) {
            IOUtils.closeWhileHandlingException(mockChannel);
        }
    }

    @Override
    protected void closeChannels(List<MockChannel> channel) throws IOException {
        IOUtils.close(channel);
    }

    @Override
    public long serverOpen() {
        return 1;
    }

    public final class MockChannel implements Closeable {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final InetSocketAddress localAddress;
        private final ServerSocket serverSocket;
        private final ConcurrentHashMap<MockChannel, Boolean> workerChannels = new ConcurrentHashMap<>();
        private final Socket activeChannel;
        private final String profile;
        private final CancellableThreads cancellableThreads = new CancellableThreads();
        private final Closeable onClose;

        /**
         * Constructs a new MockChannel instance intended for handling the actual incoming / outgoing traffic.
         *
         * @param socket The client socket. Mut not be null.
         * @param localAddress Address associated with the corresponding local server socket. Must not be null.
         * @param profile The associated profile name.
         * @param onClose Callback to execute when this channel is closed.
         */
        public MockChannel(Socket socket, InetSocketAddress localAddress, String profile, Consumer<MockChannel> onClose) {
            this.localAddress = localAddress;
            this.activeChannel = socket;
            this.serverSocket = null;
            this.profile = profile;
            this.onClose = () -> onClose.accept(this);
        }

        /**
         * Constructs a new MockChannel instance intended for accepting requests.
         *
         * @param serverSocket The associated server socket. Must not be null.
         * @param profile The associated profile name.
         */
        public MockChannel(ServerSocket serverSocket, String profile) {
            this.localAddress = (InetSocketAddress) serverSocket.getLocalSocketAddress();
            this.serverSocket = serverSocket;
            this.profile = profile;
            this.activeChannel = null;
            this.onClose = null;
        }

        public void accept(Executor executor) throws IOException {
            while (isOpen.get()) {
                Socket incomingSocket = serverSocket.accept();
                MockChannel incomingChannel = null;
                try {
                    configureSocket(incomingSocket);
                    incomingChannel = new MockChannel(incomingSocket, localAddress, profile, workerChannels::remove);
                    //establish a happens-before edge between closing and accepting a new connection
                    synchronized (this) {
                        if (isOpen.get()) {
                            workerChannels.put(incomingChannel, Boolean.TRUE);
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

        public void loopRead(Executor executor) {
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    if (isOpen.get()) {
                        try {
                            onException(MockChannel.this, e);
                        } catch (IOException ex) {
                            logger.warn("failed on handling exception", ex);
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

        @Override
        public void close() throws IOException {
            if (isOpen.compareAndSet(true, false)) {
                //establish a happens-before edge between closing and accepting a new connection
                synchronized (this) {
                    IOUtils.close(serverSocket, activeChannel, () -> IOUtils.close(workerChannels.keySet()),
                        () -> cancellableThreads.cancel("channel closed"), onClose);
                }
            }
        }
    }


    @Override
    protected void doStart() {
        boolean success = false;
        try {
            if (NetworkService.NETWORK_SERVER.get(settings)) {
                // loop through all profiles and start them up, special handling for default one
                for (Map.Entry<String, Settings> entry : buildProfileSettings().entrySet()) {
                    final Settings settings = Settings.builder()
                        .put(entry.getValue()).build();
                    bindServer(entry.getKey(), settings);
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
    }

    @Override
    protected Version getCurrentVersion() {
        return mockVersion;
    }

}

