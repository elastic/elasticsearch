/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty3;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.netty3.Netty3HttpServerTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import java.util.List;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isCloseDuringHandshakeException;
import static org.elasticsearch.xpack.security.transport.SSLExceptionHelper.isNotSslRecordException;

/**
 *
 */
public class SecurityNetty3HttpServerTransport extends Netty3HttpServerTransport {

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
    public SecurityNetty3HttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, IPFilter ipFilter,
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
    protected void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (!lifecycle.started()) {
            return;
        }

        Throwable t = e.getCause();
        if (isNotSslRecordException(t)) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                        (Supplier<?>) () -> new ParameterizedMessage(
                                "received plaintext http traffic on a https channel, closing connection {}",
                                ctx.getChannel()),
                        t);
            } else {
                logger.warn("received plaintext http traffic on a https channel, closing connection {}", ctx.getChannel());
            }
            ctx.getChannel().close();
        } else if (isCloseDuringHandshakeException(t)) {
            if (logger.isTraceEnabled()) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("connection {} closed during handshake", ctx.getChannel()), t);
            } else {
                logger.warn("connection {} closed during handshake", ctx.getChannel());
            }
            ctx.getChannel().close();
        } else {
            super.exceptionCaught(ctx, e);
        }
    }

    @Override
    protected void doStart() {
        super.doStart();
        ipFilter.setBoundHttpTransportAddress(this.boundAddress());
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory() {
        return new HttpSslChannelPipelineFactory(this);
    }

    private class HttpSslChannelPipelineFactory extends HttpChannelPipelineFactory {

        private final SSLClientAuth clientAuth;

        public HttpSslChannelPipelineFactory(Netty3HttpServerTransport transport) {
            super(transport, detailedErrorsEnabled, threadPool.getThreadContext());
            clientAuth = CLIENT_AUTH_SETTING.get(settings);
            if (ssl && sslService.isConfigurationValidForServerUsage(sslSettings) == false) {
                throw new IllegalArgumentException("a key must be provided to run as a server");
            }
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            if (ssl) {
                SSLEngine engine = sslService.createSSLEngine(sslSettings);
                engine.setUseClientMode(false);
                clientAuth.configure(engine);

                pipeline.addFirst("ssl", new SslHandler(engine));
            }
            pipeline.addFirst("ipfilter", new IPFilterNetty3UpstreamHandler(ipFilter, IPFilter.HTTP_PROFILE_NAME));
            return pipeline;
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
