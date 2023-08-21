/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Header;
import org.elasticsearch.transport.InboundAggregator;
import org.elasticsearch.transport.InboundDecoder;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.netty4.Netty4MessageInboundHandler;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;

public class SecurityNetty4ServerTransport extends SecurityNetty4Transport {

    private static final Logger logger = LogManager.getLogger(SecurityNetty4ServerTransport.class);
    @Nullable
    private final IPFilter authenticator;

    private final CrossClusterAccessAuthenticationService crossClusterAccessAuthenticationService;

    public SecurityNetty4ServerTransport(
        final Settings settings,
        final TransportVersion version,
        final ThreadPool threadPool,
        final NetworkService networkService,
        final PageCacheRecycler pageCacheRecycler,
        final NamedWriteableRegistry namedWriteableRegistry,
        final CircuitBreakerService circuitBreakerService,
        @Nullable final IPFilter authenticator,
        final SSLService sslService,
        final SharedGroupFactory sharedGroupFactory,
        final CrossClusterAccessAuthenticationService crossClusterAccessAuthenticationService
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
        this.crossClusterAccessAuthenticationService = crossClusterAccessAuthenticationService;
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

    @Override
    protected Netty4MessageInboundHandler getNetty4MessageInboundHandler(String name) {
        if (remoteClusterPortEnabled == false
            || REMOTE_CLUSTER_PROFILE.equals(name) == false
            || crossClusterAccessAuthenticationService == null) {
            return super.getNetty4MessageInboundHandler(name);
        }

        final ThreadPool threadPool = getThreadPool();
        final Transport.RequestHandlers requestHandlers = getRequestHandlers();
        final InboundAggregator aggregator = new InboundAggregator(
            getInflightBreaker(),
            requestHandlers::getHandler,
            ignoreDeserializationErrors()
        ) {
            @Override
            public void headerReceived(Header header) {
                super.headerReceived(header);

                if (header.isRequest() == false) {
                    throw new IllegalArgumentException("Remote cluster server cannot process response");
                }

                if (header.isHandshake()) {
                    return;
                }

                final ThreadContext threadContext = threadPool.getThreadContext();
                final var contextPreservingActionListener = new ContextPreservingActionListener<AuthenticationResult<User>>(
                    threadContext.wrapRestorable(threadContext.newStoredContext()),
                    ActionListener.wrap(authenticationResult -> {
                        if (false == authenticationResult.isAuthenticated()) {
                            logger.warn("authentication failed [{}]", authenticationResult.getMessage());
                            shortCircuit(
                                authenticationResult.getException() != null
                                    ? authenticationResult.getException()
                                    : new ElasticsearchSecurityException(authenticationResult.getMessage(), RestStatus.UNAUTHORIZED)
                            );
                        } else {
                            logger.info("authentication successful");
                        }
                    }, this::shortCircuit)
                );

                try (ThreadContext.StoredContext ignore = threadContext.newStoredContext()) {
                    crossClusterAccessAuthenticationService.tryAuthenticateCredentialsHeaderOnly(
                        threadContext,
                        header,
                        contextPreservingActionListener
                    );
                }
            }
        };

        return new Netty4MessageInboundHandler(
            this,
            new InboundPipeline(
                getStatsTracker(),
                threadPool::relativeTimeInMillis,
                new InboundDecoder(recycler, 16 * 1024 * 1024),
                aggregator,
                this::inboundMessage
            )
        );
    }

    private void maybeAddIPFilter(final Channel ch, final String name) {
        if (authenticator != null) {
            ch.pipeline().addFirst("ipfilter", new IpFilterRemoteAddressFilter(authenticator, name));
        }
    }

}
