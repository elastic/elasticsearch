/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4MockUtil;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.xpack.security.ssl.ServerSSLService;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Locale;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class SecurityNetty4TransportTests extends ESTestCase {
    private ServerSSLService serverSSLService;
    private ClientSSLService clientSSLService;

    @Before
    public void createSSLService() throws Exception {
        Path testnodeStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        Global globalSSLConfiguration = new Global(settings);
        serverSSLService = new ServerSSLService(settings, env, globalSSLConfiguration);
        clientSSLService = new ClientSSLService(settings, env, globalSSLConfiguration);
    }

    public void testThatSSLCanBeDisabledByProfile() throws Exception {
        Settings settings = Settings.builder().put(SecurityNetty4Transport.SSL_SETTING.getKey(), true).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client",
                Settings.builder().put("xpack.security.ssl", false).build());
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class), nullValue());
    }

    public void testThatSSLCanBeEnabledByProfile() throws Exception {
        Settings settings = Settings.builder().put(SecurityNetty4Transport.SSL_SETTING.getKey(), false).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client",
                Settings.builder().put("xpack.security.ssl", true).build());
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class), notNullValue());
    }

    public void testThatProfileTakesDefaultSSLSetting() throws Exception {
        Settings settings = Settings.builder().put(SecurityNetty4Transport.SSL_SETTING.getKey(), true).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client", Settings.EMPTY);
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine(), notNullValue());
    }

    public void testDefaultClientAuth() throws Exception {
        Settings settings = Settings.builder().put(SecurityNetty4Transport.SSL_SETTING.getKey(), true).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client", Settings.EMPTY);
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT), "true");
        Settings settings = Settings.builder()
                .put(SecurityNetty4Transport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty4Transport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client", Settings.EMPTY);
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NO.name(), "false", "FALSE", SSLClientAuth.NO.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(SecurityNetty4Transport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty4Transport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client", Settings.EMPTY);
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(SecurityNetty4Transport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty4Transport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client", Settings.EMPTY);
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(true));
    }

    public void testProfileRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT), "true", "TRUE");
        Settings settings = Settings.builder().put(SecurityNetty4Transport.SSL_SETTING.getKey(), true).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client",
                Settings.builder().put(SecurityNetty4Transport.PROFILE_CLIENT_AUTH_SETTING, value).build());
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testProfileNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NO.name(), "false", "FALSE", SSLClientAuth.NO.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder().put(SecurityNetty4Transport.SSL_SETTING.getKey(), true).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.getServerChannelInitializer("client",
                Settings.builder().put(SecurityNetty4Transport.PROFILE_CLIENT_AUTH_SETTING.getKey(), value).build());
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testProfileOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder().put(SecurityNetty4Transport.SSL_SETTING.getKey(), true).build();
        SecurityNetty4Transport transport =
                new SecurityNetty4Transport(
                        settings,
                        mock(ThreadPool.class),
                        mock(NetworkService.class),
                        mock(BigArrays.class),
                        mock(NamedWriteableRegistry.class),
                        mock(CircuitBreakerService.class),
                        null,
                        serverSSLService,
                        clientSSLService);
        Netty4MockUtil.setOpenChannelsHandlerToMock(transport);
        final ChannelHandler handler = transport.getServerChannelInitializer("client",
                Settings.builder().put(SecurityNetty4Transport.PROFILE_CLIENT_AUTH_SETTING.getKey(), value).build());
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(true));
    }

}
