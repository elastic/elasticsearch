/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import javax.net.ssl.SSLEngine;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.security.transport.SSLExceptionHelper.isCloseDuringHandshakeException;
import static org.elasticsearch.xpack.core.security.transport.SSLExceptionHelper.isNotSslRecordException;
import static org.elasticsearch.xpack.core.security.transport.SSLExceptionHelper.isReceivedCertificateUnknownException;

public class SecurityNetty4HttpServerTransport extends Netty4HttpServerTransport {

    private final IPFilter ipFilter;
    private final Settings sslSettings;
    private final SSLService sslService;
    private final SSLConfiguration sslConfiguration;

    public SecurityNetty4HttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, IPFilter ipFilter,
                                             SSLService sslService, ThreadPool threadPool, NamedXContentRegistry xContentRegistry,
                                             Dispatcher dispatcher) {
        super(settings, networkService, bigArrays, threadPool, xContentRegistry, dispatcher);
        this.ipFilter = ipFilter;
        final boolean ssl = HTTP_SSL_ENABLED.get(settings);
        this.sslSettings = SSLService.getHttpTransportSSLSettings(settings);
        this.sslService = sslService;
        if (ssl) {
            this.sslConfiguration = sslService.sslConfiguration(sslSettings, Settings.EMPTY);
            if (sslService.isConfigurationValidForServerUsage(sslConfiguration) == false) {
                throw new IllegalArgumentException("a key must be provided to run as a server. the key should be configured using the " +
                        "[xpack.security.http.ssl.key] or [xpack.security.http.ssl.keystore.path] setting");
            }
        } else {
            this.sslConfiguration = null;
        }

    }

    @Override
    protected void onException(HttpChannel channel, Exception e) {
        if (!lifecycle.started()) {
            return;
        }

        if (isNotSslRecordException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace(new ParameterizedMessage("received plaintext http traffic on a https channel, closing connection {}",
                    channel), e);
            } else {
                logger.warn("received plaintext http traffic on a https channel, closing connection {}", channel);
            }
            CloseableChannel.closeChannel(channel);
        } else if (isCloseDuringHandshakeException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace(new ParameterizedMessage("connection {} closed during ssl handshake", channel), e);
            } else {
                logger.warn("connection {} closed during ssl handshake", channel);
            }
            CloseableChannel.closeChannel(channel);
        } else if (isReceivedCertificateUnknownException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace(new ParameterizedMessage("http client did not trust server's certificate, closing connection {}",
                    channel), e);
            } else {
                logger.warn("http client did not trust this server's certificate, closing connection {}", channel);
            }
            CloseableChannel.closeChannel(channel);
        } else {
            super.onException(channel, e);
        }
    }

    @Override
    protected void doStart() {
        super.doStart();
        ipFilter.setBoundHttpTransportAddress(this.boundAddress());
    }

    @Override
    public ChannelHandler configureServerChannelHandler() {
        return new HttpSslChannelHandler();
    }

    private final class HttpSslChannelHandler extends HttpChannelHandler {
        HttpSslChannelHandler() {
            super(SecurityNetty4HttpServerTransport.this, handlingSettings);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            if (sslConfiguration != null) {
                SSLEngine sslEngine = sslService.createSSLEngine(sslConfiguration, null, -1);
                sslEngine.setUseClientMode(false);
                ch.pipeline().addFirst("ssl", new SslHandler(sslEngine));
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
