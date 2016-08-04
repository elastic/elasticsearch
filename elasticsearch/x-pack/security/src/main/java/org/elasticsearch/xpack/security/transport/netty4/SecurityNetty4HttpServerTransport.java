/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import javax.net.ssl.SSLEngine;

import java.util.List;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isCloseDuringHandshakeException;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isNotSslRecordException;

public class SecurityNetty4HttpServerTransport extends Netty4HttpServerTransport {

    public static final boolean SSL_DEFAULT = false;
    public static final String CLIENT_AUTH_DEFAULT = SSLClientAuth.NO.name();

    public static final Setting<Boolean> DEPRECATED_SSL_SETTING =
            Setting.boolSetting(setting("http.ssl"), SSL_DEFAULT, Property.NodeScope, Property.Deprecated);
    public static final Setting<Boolean> SSL_SETTING =
            Setting.boolSetting(setting("http.ssl.enabled"), DEPRECATED_SSL_SETTING, Property.NodeScope);
    public static final Setting<SSLClientAuth> CLIENT_AUTH_SETTING =
            new Setting<>(setting("http.ssl.client.auth"), CLIENT_AUTH_DEFAULT, SSLClientAuth::parse, Property.NodeScope);

    private final IPFilter ipFilter;
    private final SSLService sslService;
    private final boolean ssl;
    private final Settings sslSettings;

    @Inject
    public SecurityNetty4HttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, IPFilter ipFilter,
                                             SSLService sslService, ThreadPool threadPool) {
        super(settings, networkService, bigArrays, threadPool);
        this.ipFilter = ipFilter;
        this.ssl = SSL_SETTING.get(settings);
        this.sslService =  sslService;
        if (ssl) {
            sslSettings = settings.getByPrefix(setting("http.ssl."));
        } else {
            sslSettings = Settings.EMPTY;
        }
    }

    @Override
    protected void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (!lifecycle.started()) {
            return;
        }

        if (isNotSslRecordException(cause)) {
            if (logger.isTraceEnabled()) {
                logger.trace("received plaintext http traffic on a https channel, closing connection {}", cause, ctx.channel());
            } else {
                logger.warn("received plaintext http traffic on a https channel, closing connection {}", ctx.channel());
            }
            ctx.channel().close();
        } else if (isCloseDuringHandshakeException(cause)) {
            if (logger.isTraceEnabled()) {
                logger.trace("connection {} closed during handshake", cause, ctx.channel());
            } else {
                logger.warn("connection {} closed during handshake", ctx.channel());
            }
            ctx.channel().close();
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }

    @Override
    protected void doStart() {
        super.doStart();
        ipFilter.setBoundHttpTransportAddress(this.boundAddress());
    }

    @Override
    public ChannelHandler configureServerChannelHandler() {
        return new HttpSslChannelHandler(this);
    }

    private class HttpSslChannelHandler extends HttpChannelHandler {

        private final SSLClientAuth clientAuth;

        HttpSslChannelHandler(Netty4HttpServerTransport transport) {
            super(transport, detailedErrorsEnabled, threadPool.getThreadContext());
            clientAuth = CLIENT_AUTH_SETTING.get(settings);
            if (ssl && sslService.isConfigurationValidForServerUsage(sslSettings) == false) {
                throw new IllegalArgumentException("a key must be provided to run as a server");
            }
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            if (ssl) {
                final SSLEngine engine = sslService.createSSLEngine(sslSettings);
                engine.setUseClientMode(false);
                clientAuth.configure(engine);
                ch.pipeline().addFirst("ssl", new SslHandler(engine));
            }
            ch.pipeline().addFirst("ip_filter", new IpFilterRemoteAddressFilter(ipFilter, IPFilter.HTTP_PROFILE_NAME));
        }

    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(SSL_SETTING);
        settings.add(CLIENT_AUTH_SETTING);
        settings.add(DEPRECATED_SSL_SETTING);
    }

    public static void overrideSettings(Settings.Builder settingsBuilder, Settings settings) {
        if (SSL_SETTING.get(settings) && SETTING_HTTP_COMPRESSION.exists(settings) == false) {
            settingsBuilder.put(SETTING_HTTP_COMPRESSION.getKey(), false);
        }
    }
}
