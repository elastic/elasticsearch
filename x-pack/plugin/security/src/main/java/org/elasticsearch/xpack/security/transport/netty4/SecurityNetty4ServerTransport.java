/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

public class SecurityNetty4ServerTransport extends SecurityNetty4Transport {

    @Nullable
    private final IPFilter authenticator;

    public SecurityNetty4ServerTransport(
        final Settings settings,
        final Version version,
        final ThreadPool threadPool,
        final NetworkService networkService,
        final PageCacheRecycler pageCacheRecycler,
        final NamedWriteableRegistry namedWriteableRegistry,
        final CircuitBreakerService circuitBreakerService,
        @Nullable final IPFilter authenticator,
        final SSLService sslService,
        final SharedGroupFactory sharedGroupFactory
    ) {
        super(
            settings,
            version,
            threadPool,
            networkService,
            pageCacheRecycler,
            namedWriteableRegistry,
            circuitBreakerService,
            sslService,
            sharedGroupFactory
        );
        this.authenticator = authenticator;
    }

    @Override
    protected void doStart() {
        super.doStart();
        if (authenticator != null) {
            authenticator.setBoundTransportAddress(boundAddress(), profileBoundAddresses());
        }
    }

    @Override
    protected ChannelHandler getNoSslChannelInitializer(final String name) {
        return new IPFilterServerChannelInitializer(name);
    }

    @Override
    protected ServerChannelInitializer getSslChannelInitializer(final String name, final SslConfiguration configuration) {
        return new SecurityServerChannelInitializer(name, configuration);
    }

    public class IPFilterServerChannelInitializer extends ServerChannelInitializer {

        IPFilterServerChannelInitializer(final String name) {
            super(name);
        }

        @Override
        protected void initChannel(final Channel ch) throws Exception {
            super.initChannel(ch);
            maybeAddIPFilter(ch, name);
        }
    }

    public class SecurityServerChannelInitializer extends SslChannelInitializer {

        SecurityServerChannelInitializer(final String name, final SslConfiguration configuration) {
            super(name, configuration);
        }

        @Override
        protected void initChannel(final Channel ch) throws Exception {
            super.initChannel(ch);
            maybeAddIPFilter(ch, name);
        }

    }

    private void maybeAddIPFilter(final Channel ch, final String name) {
        if (authenticator != null) {
            ch.pipeline().addFirst("ipfilter", new IpFilterRemoteAddressFilter(authenticator, name));
        }
    }

}
