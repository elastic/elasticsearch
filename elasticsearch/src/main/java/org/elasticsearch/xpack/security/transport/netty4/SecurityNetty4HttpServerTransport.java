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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import javax.net.ssl.SSLEngine;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isCloseDuringHandshakeException;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isNotSslRecordException;
import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;

public class SecurityNetty4HttpServerTransport extends Netty4HttpServerTransport {

    private final IPFilter ipFilter;
    private final SSLService sslService;
    private final boolean ssl;

    public SecurityNetty4HttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, IPFilter ipFilter,
                                             SSLService sslService, ThreadPool threadPool, NamedXContentRegistry xContentRegistry,
                                             Dispatcher dispatcher) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher);
        this.ipFilter = ipFilter;
        this.ssl = HTTP_SSL_ENABLED.get(settings);
        this.sslService =  sslService;
    }

    @Override
    protected void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Netty4Utils.maybeDie(cause);
        if (!lifecycle.started()) {
            return;
        }

        if (isNotSslRecordException(cause)) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                        (Supplier<?>) () -> new ParameterizedMessage(
                                "received plaintext http traffic on a https channel, closing connection {}",
                                ctx.channel()),
                        cause);
            } else {
                logger.warn("received plaintext http traffic on a https channel, closing connection {}", ctx.channel());
            }
            ctx.channel().close();
        } else if (isCloseDuringHandshakeException(cause)) {
            if (logger.isTraceEnabled()) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("connection {} closed during handshake", ctx.channel()), cause);
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

        private final Settings sslSettings;

        HttpSslChannelHandler(Netty4HttpServerTransport transport) {
            super(transport, detailedErrorsEnabled, threadPool.getThreadContext());
            this.sslSettings = SSLService.getHttpTransportSSLSettings(settings);
            if (ssl && sslService.isConfigurationValidForServerUsage(sslSettings, false) == false) {
                throw new IllegalArgumentException("a key must be provided to run as a server. the key should be configured using the " +
                        "[xpack.security.http.ssl.key] or [xpack.security.http.ssl.keystore.path] setting");
            }
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            if (ssl) {
                final SSLEngine engine = sslService.createSSLEngine(sslSettings, Settings.EMPTY);
                engine.setUseClientMode(false);
                ch.pipeline().addFirst("ssl", new SslHandler(engine));
            }
            ch.pipeline().addFirst("ip_filter", new IpFilterRemoteAddressFilter(ipFilter, IPFilter.HTTP_PROFILE_NAME));
        }

    }

    public static void overrideSettings(Settings.Builder settingsBuilder, Settings settings) {
        if (HTTP_SSL_ENABLED.get(settings) && SETTING_HTTP_COMPRESSION.exists(settings) == false) {
            settingsBuilder.put(SETTING_HTTP_COMPRESSION.getKey(), false);
        }
    }
}
