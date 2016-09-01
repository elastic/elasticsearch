/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty3;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty3.Netty3Transport;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isCloseDuringHandshakeException;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isNotSslRecordException;
import static org.elasticsearch.xpack.XPackSettings.TRANSPORT_SSL_ENABLED;

public class SecurityNetty3Transport extends Netty3Transport {

    public static final Setting<Boolean> PROFILE_SSL_SETTING = Setting.boolSetting(setting("ssl.enabled"), false);

    private final SSLService sslService;
    @Nullable private final IPFilter authenticator;
    private final Settings transportSSLSettings;
    private final boolean ssl;

    @Inject
    public SecurityNetty3Transport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                                   @Nullable IPFilter authenticator, SSLService sslService, NamedWriteableRegistry namedWriteableRegistry,
                                   CircuitBreakerService circuitBreakerService) {
        super(settings, threadPool, networkService, bigArrays, namedWriteableRegistry, circuitBreakerService);
        this.authenticator = authenticator;
        this.ssl = TRANSPORT_SSL_ENABLED.get(settings);
        this.sslService = sslService;
        this.transportSSLSettings = settings.getByPrefix(setting("transport.ssl."));

    }

    @Override
    protected void doStart() {
        super.doStart();
        if (authenticator != null) {
            authenticator.setBoundTransportAddress(boundAddress(), profileBoundAddresses());
        }
    }

    @Override
    public ChannelPipelineFactory configureClientChannelPipelineFactory() {
        return new SslClientChannelPipelineFactory(this);
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory(String name, Settings profileSettings) {
        return new SslServerChannelPipelineFactory(this, name, settings, profileSettings);
    }

    @Override
    protected void onException(Channel channel, Exception e) throws IOException {
        if (isNotSslRecordException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                        (Supplier<?>) () -> new ParameterizedMessage(
                                "received plaintext traffic on a encrypted channel, closing connection {}", channel), e);
            } else {
                logger.warn("received plaintext traffic on a encrypted channel, closing connection {}", channel);
            }
            disconnectFromNodeChannel(channel, e);
        } else if (isCloseDuringHandshakeException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("connection {} closed during handshake", channel), e);
            } else {
                logger.warn("connection {} closed during handshake", channel);
            }
            disconnectFromNodeChannel(channel, e);
        } else {
            super.onException(channel, e);
        }
    }

    public static Settings profileSslSettings(Settings profileSettings) {
        return profileSettings.getByPrefix(setting("ssl."));
    }

    private class SslServerChannelPipelineFactory extends ServerChannelPipelineFactory {

        private final boolean profileSsl;
        private final Settings profileSslSettings;

        SslServerChannelPipelineFactory(Netty3Transport nettyTransport, String name, Settings settings, Settings profileSettings) {
            super(nettyTransport, name, settings);
            this.profileSsl = PROFILE_SSL_SETTING.exists(profileSettings) ? PROFILE_SSL_SETTING.get(profileSettings) : ssl;
            this.profileSslSettings = profileSslSettings(profileSettings);
            if (profileSsl && sslService.isConfigurationValidForServerUsage(profileSslSettings, transportSSLSettings) == false) {
                if (TransportSettings.DEFAULT_PROFILE.equals(name)) {
                    throw new IllegalArgumentException("a key must be provided to run as a server. the key should be configured using the "
                            + "[xpack.security.transport.ssl.key] or [xpack.security.transport.ssl.keystore.path] setting");
                }
                throw new IllegalArgumentException("a key must be provided to run as a server. the key should be configured using the "
                        + "[transport.profiles." + name + ".xpack.security.ssl.key] or [transport.profiles." + name
                        + ".xpack.security.ssl.keystore.path] setting");
            }
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            if (profileSsl) {
                final SSLEngine serverEngine = sslService.createSSLEngine(profileSslSettings, transportSSLSettings);
                serverEngine.setUseClientMode(false);
                pipeline.addFirst("ssl", new SslHandler(serverEngine));
            }
            if (authenticator != null) {
                pipeline.addFirst("ipfilter", new IPFilterNetty3UpstreamHandler(authenticator, name));
            }
            return pipeline;
        }
    }

    private class SslClientChannelPipelineFactory extends ClientChannelPipelineFactory {

        private final boolean hostnameVerificationEnabled;

        SslClientChannelPipelineFactory(Netty3Transport transport) {
            super(transport);
            this.hostnameVerificationEnabled =
                    sslService.getVerificationMode(transportSSLSettings, Settings.EMPTY).isHostnameVerificationEnabled();
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            if (ssl) {
                pipeline.addFirst("sslInitializer", new ClientSslHandlerInitializer());
            }
            return pipeline;
        }

        /**
         * Handler that waits until connect is called to create a SSLEngine with the proper parameters in order to
         * perform hostname verification
         */
        private class ClientSslHandlerInitializer extends SimpleChannelHandler {

            @Override
            public void connectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) {
                SSLEngine sslEngine;
                if (hostnameVerificationEnabled) {
                    InetSocketAddress inetSocketAddress = (InetSocketAddress) e.getValue();
                    // we create the socket based on the name given. don't reverse DNS
                    sslEngine = sslService.createSSLEngine(transportSSLSettings, Settings.EMPTY, inetSocketAddress.getHostString(),
                            inetSocketAddress.getPort());
                } else {
                    sslEngine = sslService.createSSLEngine(transportSSLSettings, Settings.EMPTY);
                }

                sslEngine.setUseClientMode(true);
                ctx.getPipeline().replace(this, "ssl", new SslHandler(sslEngine));
                ctx.getPipeline().addAfter("ssl", "handshake", new Netty3HandshakeWaitingHandler(logger));

                ctx.sendDownstream(e);
            }
        }
    }
}
