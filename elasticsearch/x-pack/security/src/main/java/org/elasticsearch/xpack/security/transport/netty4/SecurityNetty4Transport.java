/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.xpack.security.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.Security.settingPrefix;


/**
 * Implementation of a transport that extends the {@link Netty4Transport} to add SSL and IP Filtering
 */
public class SecurityNetty4Transport extends Netty4Transport {

    public static final String CLIENT_AUTH_DEFAULT = SSLClientAuth.REQUIRED.name();
    public static final boolean SSL_DEFAULT = false;

    public static final Setting<Boolean> DEPRECATED_HOSTNAME_VERIFICATION_SETTING =
            Setting.boolSetting(
                    setting("ssl.hostname_verification"),
                    true,
                    new Property[]{Property.NodeScope, Property.Filtered, Property.Deprecated, Property.Shared});

    public static final Setting<Boolean> HOSTNAME_VERIFICATION_SETTING =
            Setting.boolSetting(setting("ssl.hostname_verification.enabled"), DEPRECATED_HOSTNAME_VERIFICATION_SETTING,
                    Property.NodeScope, Property.Filtered, Property.Shared);

    public static final Setting<Boolean> HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING =
            Setting.boolSetting(
                    setting("ssl.hostname_verification.resolve_name"),
                    true,
                    new Property[]{Property.NodeScope, Property.Filtered, Property.Shared});

    public static final Setting<Boolean> DEPRECATED_SSL_SETTING =
            Setting.boolSetting(setting("transport.ssl"), SSL_DEFAULT,
                    Property.Filtered, Property.NodeScope, Property.Deprecated, Property.Shared);

    public static final Setting<Boolean> SSL_SETTING =
            Setting.boolSetting(
                    setting("transport.ssl.enabled"),
                    DEPRECATED_SSL_SETTING,
                    new Property[]{Property.Filtered, Property.NodeScope, Property.Shared});

    public static final Setting<SSLClientAuth> CLIENT_AUTH_SETTING =
            new Setting<>(
                    setting("transport.ssl.client.auth"),
                    CLIENT_AUTH_DEFAULT,
                    SSLClientAuth::parse,
                    new Property[]{Property.NodeScope, Property.Filtered, Property.Shared});

    public static final Setting<Boolean> DEPRECATED_PROFILE_SSL_SETTING =
            Setting.boolSetting(setting("ssl"), SSL_SETTING, Property.Filtered, Property.NodeScope, Property.Deprecated, Property.Shared);

    public static final Setting<Boolean> PROFILE_SSL_SETTING =
            Setting.boolSetting(setting("ssl.enabled"), SSL_DEFAULT, Property.Filtered, Property.NodeScope, Property.Shared);

    public static final Setting<SSLClientAuth> PROFILE_CLIENT_AUTH_SETTING =
            new Setting<>(
                    setting("ssl.client.auth"),
                    CLIENT_AUTH_SETTING,
                    SSLClientAuth::parse,
                    new Property[]{Property.NodeScope, Property.Filtered, Property.Shared});

    private final SSLService sslService;
    @Nullable private final IPFilter authenticator;
    private final SSLClientAuth clientAuth;
    private final boolean ssl;

    @Inject
    public SecurityNetty4Transport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                                   NamedWriteableRegistry namedWriteableRegistry, CircuitBreakerService circuitBreakerService,
                                   @Nullable IPFilter authenticator, SSLService sslService) {
        super(settings, threadPool, networkService, bigArrays, namedWriteableRegistry, circuitBreakerService);
        this.authenticator = authenticator;
        this.ssl = SSL_SETTING.get(settings);
        this.clientAuth = CLIENT_AUTH_SETTING.get(settings);
        this.sslService = sslService;
    }

    @Override
    protected void doStart() {
        super.doStart();
        if (authenticator != null) {
            authenticator.setBoundTransportAddress(boundAddress(), profileBoundAddresses());
        }
    }

    @Override
    protected ChannelHandler getServerChannelInitializer(String name, Settings settings) {
        return new SecurityServerChannelInitializer(name, settings);
    }

    @Override
    protected ChannelHandler getClientChannelInitializer() {
        return new SecurityClientChannelInitializer();
    }

    class SecurityServerChannelInitializer extends ServerChannelInitializer {

        private final boolean sslEnabled;
        private final Settings securityProfileSettings;

        protected SecurityServerChannelInitializer(String name, Settings settings) {
            super(name, settings);
            this.sslEnabled = profileSSL(settings, ssl);
            this.securityProfileSettings = settings.getByPrefix(settingPrefix());
            if (sslEnabled && sslService.isConfigurationValidForServerUsage(securityProfileSettings) == false) {
                throw new IllegalArgumentException("a key must be provided to run as a server");
            }
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            if (sslEnabled) {
                SSLEngine serverEngine = sslService.createSSLEngine(securityProfileSettings);
                serverEngine.setUseClientMode(false);
                final SSLClientAuth profileClientAuth = profileClientAuth(settings, clientAuth);
                profileClientAuth.configure(serverEngine);
                ch.pipeline().addFirst(new SslHandler(serverEngine));
            }
            if (authenticator != null) {
                ch.pipeline().addFirst(new IpFilterRemoteAddressFilter(authenticator, name));
            }
        }
    }

    class SecurityClientChannelInitializer extends ClientChannelInitializer {
        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            if (ssl) {
                ch.pipeline().addFirst(new ClientSslHandlerInitializer());
            }
        }
    }

    private class ClientSslHandlerInitializer extends ChannelOutboundHandlerAdapter {

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                            SocketAddress localAddress, ChannelPromise promise) throws Exception {
            final SSLEngine sslEngine;
            if (HOSTNAME_VERIFICATION_SETTING.get(settings)) {
                InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;
                sslEngine = sslService.createSSLEngine(Settings.EMPTY, getHostname(inetSocketAddress), inetSocketAddress.getPort());

                // By default, a SSLEngine will not perform hostname verification. In order to perform hostname verification
                // we need to specify a EndpointIdentificationAlgorithm. We use the HTTPS algorithm to prevent against
                // man in the middle attacks for transport connections
                SSLParameters parameters = new SSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                sslEngine.setSSLParameters(parameters);
            } else {
                sslEngine = sslService.createSSLEngine(Settings.EMPTY);
            }

            sslEngine.setUseClientMode(true);
            ctx.pipeline().replace(this, "ssl", new SslHandler(sslEngine));
            super.connect(ctx, remoteAddress, localAddress, promise);
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

    public static boolean profileSSL(Settings profileSettings, boolean defaultSSL) {
        if (PROFILE_SSL_SETTING.exists(profileSettings)) {
            return PROFILE_SSL_SETTING.get(profileSettings);
        } else if (DEPRECATED_PROFILE_SSL_SETTING.exists(profileSettings)) {
            return DEPRECATED_PROFILE_SSL_SETTING.get(profileSettings);
        } else {
            return defaultSSL;
        }
    }

    static SSLClientAuth profileClientAuth(Settings settings, SSLClientAuth clientAuth) {
        if (PROFILE_CLIENT_AUTH_SETTING.exists(settings)) {
            return PROFILE_CLIENT_AUTH_SETTING.get(settings);
        }
        return clientAuth;
    }
}
