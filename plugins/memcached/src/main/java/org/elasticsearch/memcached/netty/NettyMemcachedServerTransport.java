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

package org.elasticsearch.memcached.netty;

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.memcached.MemcachedServerTransport;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.component.AbstractLifecycleComponent;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.transport.BoundTransportAddress;
import org.elasticsearch.util.transport.InetSocketTransportAddress;
import org.elasticsearch.util.transport.PortsRange;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.util.concurrent.DynamicExecutors.*;
import static org.elasticsearch.util.io.HostResolver.*;

/**
 * @author kimchy (shay.banon)
 */
public class NettyMemcachedServerTransport extends AbstractLifecycleComponent<MemcachedServerTransport> implements MemcachedServerTransport {

    static {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory() {
            @Override public InternalLogger newInstance(String name) {
                return super.newInstance(name.replace("org.jboss.netty.", "netty."));
            }
        });
    }

    private final RestController restController;

    private final int workerCount;

    private final String port;

    private final String bindHost;

    private final String publishHost;

    private final Boolean tcpNoDelay;

    private final Boolean tcpKeepAlive;

    private final Boolean reuseAddress;

    private final SizeValue tcpSendBufferSize;

    private final SizeValue tcpReceiveBufferSize;

    private volatile ServerBootstrap serverBootstrap;

    private volatile BoundTransportAddress boundAddress;

    private volatile Channel serverChannel;

    private volatile OpenChannelsHandler serverOpenChannels;

    @Inject public NettyMemcachedServerTransport(Settings settings, RestController restController) {
        super(settings);
        this.restController = restController;

        this.workerCount = componentSettings.getAsInt("worker_count", Runtime.getRuntime().availableProcessors());
        this.port = componentSettings.get("port", "11211-11311");
        this.bindHost = componentSettings.get("bind_host");
        this.publishHost = componentSettings.get("publish_host");
        this.tcpNoDelay = componentSettings.getAsBoolean("tcp_no_delay", true);
        this.tcpKeepAlive = componentSettings.getAsBoolean("tcp_keep_alive", null);
        this.reuseAddress = componentSettings.getAsBoolean("reuse_address", null);
        this.tcpSendBufferSize = componentSettings.getAsSize("tcp_send_buffer_size", null);
        this.tcpReceiveBufferSize = componentSettings.getAsSize("tcp_receive_buffer_size", null);
    }

    @Override public BoundTransportAddress boundAddress() {
        return boundAddress;
    }

    @Override protected void doStart() throws ElasticSearchException {
        this.serverOpenChannels = new OpenChannelsHandler();

        serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "memcachedBoss")),
                Executors.newCachedThreadPool(daemonThreadFactory(settings, "memcachedIoWorker")),
                workerCount));

        ChannelPipelineFactory pipelineFactory = new ChannelPipelineFactory() {
            @Override public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("openChannels", serverOpenChannels);
                pipeline.addLast("decoder", new TextMemcachedDecoder());
                pipeline.addLast("dispatcher", new MemcachedDispatcher(restController));
                return pipeline;
            }
        };

        serverBootstrap.setPipelineFactory(pipelineFactory);

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
            hostAddressX = resolveBindHostAddress(bindHost, settings);
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + bindHost + "]", e);
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
            throw new BindHttpException("Failed to bind to [" + port + "]", lastException.get());
        }

        InetSocketAddress boundAddress = (InetSocketAddress) serverChannel.getLocalAddress();
        InetSocketAddress publishAddress;
        try {
            InetAddress publishAddressX = resolvePublishHostAddress(publishHost, settings);
            if (publishAddressX == null) {
                // if its 0.0.0.0, we can't publish that.., default to the local ip address
                if (boundAddress.getAddress().isAnyLocalAddress()) {
                    publishAddress = new InetSocketAddress(resolvePublishHostAddress(publishHost, settings, LOCAL_IP), boundAddress.getPort());
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
    }

    @Override protected void doStop() throws ElasticSearchException {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }

        if (serverOpenChannels != null) {
            serverOpenChannels.close();
            serverOpenChannels = null;
        }

        if (serverBootstrap != null) {
            serverBootstrap.releaseExternalResources();
            serverBootstrap = null;
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }
}
