/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;
import org.elasticsearch.xpack.security.ssl.ServerSSLService;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.security.Security.featureEnabledSetting;
import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.Security.settingPrefix;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isCloseDuringHandshakeException;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isNotSslRecordException;

/**
 *
 */
public class SecurityNettyTransport extends NettyTransport {

    public static final String CLIENT_AUTH_DEFAULT = SSLClientAuth.REQUIRED.name();
    public static final boolean SSL_DEFAULT = false;

    public static final Setting<Boolean> DEPRECATED_HOSTNAME_VERIFICATION_SETTING =
            Setting.boolSetting(setting("ssl.hostname_verification"), true, Property.NodeScope, Property.Filtered, Property.Deprecated);
    public static final Setting<Boolean> HOSTNAME_VERIFICATION_SETTING =
            Setting.boolSetting(featureEnabledSetting("ssl.hostname_verification"), DEPRECATED_HOSTNAME_VERIFICATION_SETTING,
                    Property.NodeScope, Property.Filtered);
    public static final Setting<Boolean> HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING =
            Setting.boolSetting(setting("ssl.hostname_verification.resolve_name"), true, Property.NodeScope, Property.Filtered);

    public static final Setting<Boolean> DEPRECATED_SSL_SETTING =
            Setting.boolSetting(setting("transport.ssl"), SSL_DEFAULT,
                    Property.Filtered, Property.NodeScope, Property.Deprecated);
    public static final Setting<Boolean> SSL_SETTING =
            Setting.boolSetting(setting("transport.ssl.enabled"), DEPRECATED_SSL_SETTING, Property.Filtered, Property.NodeScope);

    public static final Setting<SSLClientAuth> CLIENT_AUTH_SETTING =
            new Setting<>(setting("transport.ssl.client.auth"), CLIENT_AUTH_DEFAULT,
                    SSLClientAuth::parse, Property.NodeScope, Property.Filtered);

    public static final Setting<Boolean> DEPRECATED_PROFILE_SSL_SETTING =
            Setting.boolSetting(setting("ssl"), SSL_SETTING, Property.Filtered, Property.NodeScope, Property.Deprecated);
    public static final Setting<Boolean> PROFILE_SSL_SETTING =
            Setting.boolSetting(setting("ssl.enabled"), SSL_DEFAULT, Property.Filtered, Property.NodeScope);

    public static final Setting<SSLClientAuth> PROFILE_CLIENT_AUTH_SETTING =
            new Setting<>(setting("ssl.client.auth"), CLIENT_AUTH_SETTING, SSLClientAuth::parse,
                    Property.NodeScope, Property.Filtered);

    private final ServerSSLService serverSslService;
    private final ClientSSLService clientSSLService;
    @Nullable private final IPFilter authenticator;
    private final boolean ssl;

    @Inject
    public SecurityNettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                                @Nullable IPFilter authenticator, @Nullable ServerSSLService serverSSLService,
                                ClientSSLService clientSSLService, NamedWriteableRegistry namedWriteableRegistry,
                                CircuitBreakerService circuitBreakerService) {
        super(settings, threadPool, networkService, bigArrays, namedWriteableRegistry, circuitBreakerService);
        this.authenticator = authenticator;
        this.ssl = SSL_SETTING.get(settings);
        this.serverSslService = serverSSLService;
        this.clientSSLService = clientSSLService;
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
    protected void onException(Channel channel, Throwable e) {
        if (isNotSslRecordException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace("received plaintext traffic on a encrypted channel, closing connection {}", e, channel);
            } else {
                logger.warn("received plaintext traffic on a encrypted channel, closing connection {}", channel);
            }
            disconnectFromNodeChannel(channel, e);
        } else if (isCloseDuringHandshakeException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace("connection {} closed during handshake", e, channel);
            } else {
                logger.warn("connection {} closed during handshake", channel);
            }
            disconnectFromNodeChannel(channel, e);
        } else {
            super.onException(channel, e);
        }
    }

    public static boolean profileSsl(Settings profileSettings, Settings settings) {
        // we can't use the fallback mechanism here since it may not exist in the profile settings and we get the wrong value
        // for the profile if they use the old setting
        if (PROFILE_SSL_SETTING.exists(profileSettings)) {
            return PROFILE_SSL_SETTING.get(profileSettings);
        } else if (DEPRECATED_PROFILE_SSL_SETTING.exists(profileSettings)) {
            return DEPRECATED_PROFILE_SSL_SETTING.get(profileSettings);
        } else {
            return SSL_SETTING.get(settings);
        }
    }

    private class SslServerChannelPipelineFactory extends ServerChannelPipelineFactory {

        private final Settings profileSettings;

        public SslServerChannelPipelineFactory(NettyTransport nettyTransport, String name, Settings settings, Settings profileSettings) {
            super(nettyTransport, name, settings);
            this.profileSettings = profileSettings;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            final boolean profileSsl = profileSsl(profileSettings, settings);
            final SSLClientAuth clientAuth = PROFILE_CLIENT_AUTH_SETTING.get(profileSettings, settings);
            if (profileSsl) {
                SSLEngine serverEngine;
                Settings securityProfileSettings = profileSettings.getByPrefix(settingPrefix());
                if (securityProfileSettings.names().isEmpty() == false) {
                    serverEngine = serverSslService.createSSLEngine(securityProfileSettings);
                } else {
                    serverEngine = serverSslService.createSSLEngine();
                }
                serverEngine.setUseClientMode(false);
                clientAuth.configure(serverEngine);

                pipeline.addFirst("ssl", new SslHandler(serverEngine));
            }
            if (authenticator != null) {
                pipeline.addFirst("ipfilter", new IPFilterNettyUpstreamHandler(authenticator, name));
            }
            return pipeline;
        }
    }

    private class SslClientChannelPipelineFactory extends ClientChannelPipelineFactory {

        public SslClientChannelPipelineFactory(NettyTransport transport) {
            super(transport);
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
                if (HOSTNAME_VERIFICATION_SETTING.get(settings)) {
                    InetSocketAddress inetSocketAddress = (InetSocketAddress) e.getValue();
                    sslEngine = clientSSLService.createSSLEngine(Settings.EMPTY, getHostname(inetSocketAddress),
                            inetSocketAddress.getPort());

                    // By default, a SSLEngine will not perform hostname verification. In order to perform hostname verification
                    // we need to specify a EndpointIdentificationAlgorithm. We use the HTTPS algorithm to prevent against
                    // man in the middle attacks for transport connections
                    SSLParameters parameters = new SSLParameters();
                    parameters.setEndpointIdentificationAlgorithm("HTTPS");
                    sslEngine.setSSLParameters(parameters);
                } else {
                    sslEngine = clientSSLService.createSSLEngine();
                }

                sslEngine.setUseClientMode(true);
                ctx.getPipeline().replace(this, "ssl", new SslHandler(sslEngine));
                ctx.getPipeline().addAfter("ssl", "handshake", new HandshakeWaitingHandler(logger));

                ctx.sendDownstream(e);
            }

            @SuppressForbidden(reason = "need to use getHostName to resolve DNS name for SSL connections and hostname verification")
            private String getHostname(InetSocketAddress inetSocketAddress) {
                String hostname;
                if (HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING.get(settings)) {
                    hostname = inetSocketAddress.getHostName();
                } else {
                    hostname = inetSocketAddress.getHostString();
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("resolved hostname [{}] for address [{}] to be used in ssl hostname verification", hostname,
                            inetSocketAddress);
                }
                return hostname;
            }
        }
    }

    public static void addSettings(List<Setting<?>> settingsModule) {
        settingsModule.add(SSL_SETTING);
        settingsModule.add(HOSTNAME_VERIFICATION_SETTING);
        settingsModule.add(HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING);
        settingsModule.add(CLIENT_AUTH_SETTING);
        settingsModule.add(PROFILE_SSL_SETTING);
        settingsModule.add(PROFILE_CLIENT_AUTH_SETTING);

        // deprecated transport settings
        settingsModule.add(DEPRECATED_SSL_SETTING);
        settingsModule.add(DEPRECATED_PROFILE_SSL_SETTING);
        settingsModule.add(DEPRECATED_HOSTNAME_VERIFICATION_SETTING);
    }
}
