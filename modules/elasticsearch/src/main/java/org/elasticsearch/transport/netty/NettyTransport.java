/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.transport.netty;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.io.ByteArrayDataOutputStream;
import org.elasticsearch.util.io.Streamable;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.BoundTransportAddress;
import org.elasticsearch.util.transport.InetSocketTransportAddress;
import org.elasticsearch.util.transport.PortsRange;
import org.elasticsearch.util.transport.TransportAddress;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.transport.Transport.Helper.*;
import static org.elasticsearch.util.TimeValue.*;
import static org.elasticsearch.util.concurrent.ConcurrentMaps.*;
import static org.elasticsearch.util.concurrent.DynamicExecutors.*;
import static org.elasticsearch.util.io.HostResolver.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.util.transport.NetworkExceptionHelper.*;

/**
 * @author kimchy (Shay Banon)
 */
public class NettyTransport extends AbstractComponent implements Transport {

    static {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory() {
            @Override public InternalLogger newInstance(String name) {
                return super.newInstance(name.replace("org.jboss.netty.", "netty."));
            }
        });
    }

    private final Lifecycle lifecycle = new Lifecycle();

    final int workerCount;

    final String port;

    final String bindHost;

    final String publishHost;

    final TimeValue connectTimeout;

    final int connectionsPerNode;

    final int connectRetries;

    final Boolean tcpNoDelay;

    final Boolean tcpKeepAlive;

    final Boolean reuseAddress;

    final SizeValue tcpSendBufferSize;

    final SizeValue tcpReceiveBufferSize;

    private final ThreadPool threadPool;

    private volatile OpenChannelsHandler serverOpenChannels;

    private volatile ClientBootstrap clientBootstrap;

    private volatile ServerBootstrap serverBootstrap;

    // node id to actual channel
    final ConcurrentMap<String, NodeConnections> clientChannels = newConcurrentMap();


    private volatile Channel serverChannel;

    private volatile TransportServiceAdapter transportServiceAdapter;

    private volatile BoundTransportAddress boundAddress;

    public NettyTransport(ThreadPool threadPool) {
        this(EMPTY_SETTINGS, threadPool);
    }

    @Inject public NettyTransport(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;

        this.workerCount = componentSettings.getAsInt("workerCount", Runtime.getRuntime().availableProcessors());
        this.port = componentSettings.get("port", "9300-9400");
        this.bindHost = componentSettings.get("bindHost");
        this.connectionsPerNode = componentSettings.getAsInt("connectionsPerNode", 5);
        this.publishHost = componentSettings.get("publishHost");
        this.connectTimeout = componentSettings.getAsTime("connectTimeout", timeValueSeconds(1));
        this.connectRetries = componentSettings.getAsInt("connectRetries", 2);
        this.tcpNoDelay = componentSettings.getAsBoolean("tcpNoDelay", true);
        this.tcpKeepAlive = componentSettings.getAsBoolean("tcpKeepAlive", null);
        this.reuseAddress = componentSettings.getAsBoolean("reuseAddress", true);
        this.tcpSendBufferSize = componentSettings.getAsSize("tcpSendBufferSize", null);
        this.tcpReceiveBufferSize = componentSettings.getAsSize("tcpReceiveBufferSize", null);
    }

    @Override public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    public Settings settings() {
        return this.settings;
    }

    @Override public void transportServiceAdapter(TransportServiceAdapter service) {
        this.transportServiceAdapter = service;
    }

    TransportServiceAdapter transportServiceAdapter() {
        return transportServiceAdapter;
    }

    ThreadPool threadPool() {
        return threadPool;
    }

    @Override public Transport start() throws TransportException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        clientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "transportClientBoss")),
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "transportClientIoWorker")),
                workerCount));
        ChannelPipelineFactory clientPipelineFactory = new ChannelPipelineFactory() {
            @Override public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new SizeHeaderFrameDecoder());
                pipeline.addLast("dispatcher", new MessageChannelHandler(NettyTransport.this, logger));
                return pipeline;
            }
        };
        clientBootstrap.setPipelineFactory(clientPipelineFactory);
        clientBootstrap.setOption("connectTimeoutMillis", connectTimeout.millis());
        if (tcpNoDelay != null) {
            clientBootstrap.setOption("tcpNoDelay", tcpNoDelay);
        }
        if (tcpKeepAlive != null) {
            clientBootstrap.setOption("keepAlive", tcpKeepAlive);
        }
        if (tcpSendBufferSize != null) {
            clientBootstrap.setOption("sendBufferSize", tcpSendBufferSize.bytes());
        }
        if (tcpReceiveBufferSize != null) {
            clientBootstrap.setOption("receiveBufferSize", tcpReceiveBufferSize.bytes());
        }
        if (reuseAddress != null) {
            clientBootstrap.setOption("reuseAddress", reuseAddress);
        }

        if (!settings.getAsBoolean("network.server", true)) {
            return null;
        }

        serverOpenChannels = new OpenChannelsHandler();
        serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "transportServerBoss")),
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "transportServerIoWorker")),
                workerCount));
        ChannelPipelineFactory serverPipelineFactory = new ChannelPipelineFactory() {
            @Override public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("openChannels", serverOpenChannels);
                pipeline.addLast("decoder", new SizeHeaderFrameDecoder());
                pipeline.addLast("dispatcher", new MessageChannelHandler(NettyTransport.this, logger));
                return pipeline;
            }
        };
        serverBootstrap.setPipelineFactory(serverPipelineFactory);
        if (tcpNoDelay != null) {
            serverBootstrap.setOption("child.tcpNoDelay", tcpNoDelay);
        }
        if (tcpKeepAlive != null) {
            serverBootstrap.setOption("child.keepAlive", tcpKeepAlive);
        }
        if (tcpSendBufferSize != null) {
            serverBootstrap.setOption("child.sendBufferSize", tcpSendBufferSize.bytes());
        }
        if (tcpReceiveBufferSize != null) {
            serverBootstrap.setOption("child.receiveBufferSize", tcpReceiveBufferSize.bytes());
        }
        if (reuseAddress != null) {
            serverBootstrap.setOption("reuseAddress", reuseAddress);
            serverBootstrap.setOption("child.reuseAddress", reuseAddress);
        }

        // Bind and start to accept incoming connections.
        InetAddress hostAddressX;
        try {
            hostAddressX = resultBindHostAddress(bindHost, settings);
        } catch (IOException e) {
            throw new BindTransportException("Failed to resolve host [" + bindHost + "]", e);
        }
        final InetAddress hostAddress = hostAddressX;

        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override public boolean onPortNumber(int portNumber) {
                try {
                    serverChannel = serverBootstrap.bind(new InetSocketAddress(hostAddress, portNumber));
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            }
        });
        if (!success) {
            throw new BindTransportException("Failed to bind to [" + port + "]", lastException.get());
        }

        logger.debug("Bound to address [{}]", serverChannel.getLocalAddress());

        InetSocketAddress boundAddress = (InetSocketAddress) serverChannel.getLocalAddress();
        InetSocketAddress publishAddress;
        try {
            InetAddress publishAddressX = resultPublishHostAddress(publishHost, settings);
            if (publishAddressX == null) {
                // if its 0.0.0.0, we can't publish that.., default to the local ip address
                if (boundAddress.getAddress().isAnyLocalAddress()) {
                    publishAddress = new InetSocketAddress(resultPublishHostAddress(publishHost, settings, LOCAL_IP), boundAddress.getPort());
                } else {
                    publishAddress = boundAddress;
                }
            } else {
                publishAddress = new InetSocketAddress(publishAddressX, boundAddress.getPort());
            }
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }
        this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress));
        return this;
    }

    @Override public Transport stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }

        if (serverChannel != null) {
            try {
                serverChannel.close().awaitUninterruptibly();
            } finally {
                serverChannel = null;
            }
        }

        if (serverOpenChannels != null) {
            serverOpenChannels.close();
            serverOpenChannels = null;
        }

        if (serverBootstrap != null) {
            serverBootstrap.releaseExternalResources();
            serverBootstrap = null;
        }

        for (Iterator<NodeConnections> it = clientChannels.values().iterator(); it.hasNext();) {
            NodeConnections nodeConnections = it.next();
            it.remove();
            nodeConnections.close();
        }

        if (clientBootstrap != null) {
            // HACK, make sure we try and close open client channels also after
            // we releaseExternalResources, they seem to hang when there are open client channels
            ScheduledFuture<?> scheduledFuture = threadPool.schedule(new Runnable() {
                @Override public void run() {
                    try {
                        for (Iterator<NodeConnections> it = clientChannels.values().iterator(); it.hasNext();) {
                            NodeConnections nodeConnections = it.next();
                            it.remove();
                            nodeConnections.close();
                        }
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }, 500, TimeUnit.MILLISECONDS);
            clientBootstrap.releaseExternalResources();
            scheduledFuture.cancel(false);
            clientBootstrap = null;
        }
        return this;
    }

    @Override public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    @Override public BoundTransportAddress boundAddress() {
        return this.boundAddress;
    }

    void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (!lifecycle.started()) {
            // ignore
        }
        if (isCloseConnectionException(e.getCause()) || isConnectException(e.getCause())) {
            if (logger.isTraceEnabled()) {
                logger.trace("(Ignoring) Exception caught on netty layer [" + ctx.getChannel() + "]", e.getCause());
            }
        } else {
            logger.warn("Exception caught on netty layer [" + ctx.getChannel() + "]", e.getCause());
        }
    }

    TransportAddress wrapAddress(SocketAddress socketAddress) {
        return new InetSocketTransportAddress((InetSocketAddress) socketAddress);
    }

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    @Override public <T extends Streamable> void sendRequest(Node node, long requestId, String action,
                                                             Streamable streamable, final TransportResponseHandler<T> handler) throws IOException, TransportException {

        Channel targetChannel = nodeChannel(node);

        ByteArrayDataOutputStream stream = ByteArrayDataOutputStream.Cached.cached();
        stream.write(LENGTH_PLACEHOLDER); // fake size

        stream.writeLong(requestId);
        byte status = 0;
        status = setRequest(status);
        stream.writeByte(status); // 0 for request, 1 for response.

        stream.writeUTF(action);
        streamable.writeTo(stream);

        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(stream.copiedByteArray());

        int size = buffer.writerIndex() - 4;
        if (size == 0) {
            handler.handleException(new RemoteTransportException("", new FailedCommunicationException("Trying to send a stream with 0 size")));
        }
        buffer.setInt(0, size); // update real size.
        ChannelFuture channelFuture = targetChannel.write(buffer);
        // TODO do we need this listener?
//        channelFuture.addListener(new ChannelFutureListener() {
//            @Override public void operationComplete(ChannelFuture future) throws Exception {
//                if (!future.isSuccess()) {
//                    // maybe add back the retry?
//                    handler.handleException(new RemoteTransportException("", new FailedCommunicationException("Error sending request", future.getCause())));
//                }
//            }
//        });
    }

    @Override public void nodesAdded(Iterable<Node> nodes) {
        if (!lifecycle.started()) {
            throw new ElasticSearchIllegalStateException("Can't add nodes to a stopped transport");
        }
        for (Node node : nodes) {
            try {
                nodeChannel(node);
            } catch (Exception e) {
                logger.warn("Failed to connect to discovered node [" + node + "]", e);
            }
        }
    }

    @Override public void nodesRemoved(Iterable<Node> nodes) {
        for (Node node : nodes) {
            NodeConnections nodeConnections = clientChannels.remove(node.id());
            if (nodeConnections != null) {
                nodeConnections.close();
            }
        }
    }

    private Channel nodeChannel(Node node) throws ConnectTransportException {
        if (node == null) {
            throw new ConnectTransportException(node, "Can't connect to a null node");
        }
        NodeConnections nodeConnections = clientChannels.get(node.id());
        if (nodeConnections != null) {
            return nodeConnections.channel();
        }
        synchronized (this) {
            // recheck here, within the sync block (we cache connections, so we don't care about this single sync block)
            nodeConnections = clientChannels.get(node.id());
            if (nodeConnections != null) {
                return nodeConnections.channel();
            }
            // build connection(s) to the node
            ArrayList<Channel> channels = new ArrayList<Channel>();
            Throwable lastConnectException = null;
            for (int connectionIndex = 0; connectionIndex < connectionsPerNode; connectionIndex++) {
                for (int i = 1; i <= connectRetries; i++) {
                    if (!lifecycle.started()) {
                        for (Channel channel1 : channels) {
                            channel1.close().awaitUninterruptibly();
                        }
                        throw new ConnectTransportException(node, "Can't connect when the transport is stopped");
                    }
                    InetSocketAddress address = ((InetSocketTransportAddress) node.address()).address();
                    ChannelFuture channelFuture = clientBootstrap.connect(address);
                    channelFuture.awaitUninterruptibly((long) (connectTimeout.millis() * 1.25));
                    if (!channelFuture.isSuccess()) {
                        // we failed to connect, check if we need to bail or retry
                        if (i == connectRetries && connectionIndex == 0) {
                            lastConnectException = channelFuture.getCause();
                            if (connectionIndex == 0) {
                                throw new ConnectTransportException(node, "connectTimeout[" + connectTimeout + "], connectRetries[" + connectRetries + "]", lastConnectException);
                            } else {
                                // break out of the retry loop, try another connection
                                break;
                            }
                        } else {
                            logger.trace("Retry #[" + i + "], connect to [" + node + "]");
                            try {
                                channelFuture.getChannel().close();
                            } catch (Exception e) {
                                // ignore
                            }
                            continue;
                        }
                    }
                    // we got a connection, add it to our connections
                    Channel channel = channelFuture.getChannel();
                    if (!lifecycle.started()) {
                        channel.close();
                        for (Channel channel1 : channels) {
                            channel1.close().awaitUninterruptibly();
                        }
                        throw new ConnectTransportException(node, "Can't connect when the transport is stopped");
                    }
                    channel.getCloseFuture().addListener(new ChannelCloseListener(node.id()));
                    channels.add(channel);
                    break;
                }
            }
            if (channels.isEmpty()) {
                if (lastConnectException != null) {
                    throw new ConnectTransportException(node, "connectTimeout[" + connectTimeout + "], connectRetries[" + connectRetries + "]", lastConnectException);
                }
                throw new ConnectTransportException(node, "connectTimeout[" + connectTimeout + "], connectRetries[" + connectRetries + "], reason unknown");
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Connected to node [{}], numberOfConnections [{}]", node, channels.size());
            }
            clientChannels.put(node.id(), new NodeConnections(channels.toArray(new Channel[channels.size()])));
        }

        return clientChannels.get(node.id()).channel();
    }

    private static class NodeConnections {

        private final AtomicInteger counter = new AtomicInteger();

        private volatile Channel[] channels;

        private volatile boolean closed = false;

        private NodeConnections(Channel[] channels) {
            this.channels = channels;
        }

        private Channel channel() {
            return channels[Math.abs(counter.incrementAndGet()) % channels.length];
        }

        private void channelClosed(Channel closedChannel) {
            List<Channel> updated = Lists.newArrayList();
            for (Channel channel : channels) {
                if (!channel.getId().equals(closedChannel.getId())) {
                    updated.add(channel);
                }
            }
            this.channels = updated.toArray(new Channel[updated.size()]);
        }

        private int numberOfChannels() {
            return channels.length;
        }

        private synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            Channel[] channelsToClose = channels;
            channels = new Channel[0];
            for (Channel channel : channelsToClose) {
                if (channel.isOpen()) {
                    channel.close().awaitUninterruptibly();
                }
            }
        }
    }

    private class ChannelCloseListener implements ChannelFutureListener {

        private final String nodeId;

        private ChannelCloseListener(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override public void operationComplete(ChannelFuture future) throws Exception {
            final NodeConnections nodeConnections = clientChannels.get(nodeId);
            if (nodeConnections != null) {
                nodeConnections.channelClosed(future.getChannel());
                if (nodeConnections.numberOfChannels() == 0) {
                    // all the channels in the node connections are closed, remove it from
                    // our client channels
                    clientChannels.remove(nodeId);
                }
            }
        }
    }
}
