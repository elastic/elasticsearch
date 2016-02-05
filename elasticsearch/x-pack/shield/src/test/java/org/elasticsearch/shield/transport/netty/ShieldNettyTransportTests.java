/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.ssl.ServerSSLService;
import org.elasticsearch.shield.transport.SSLClientAuth;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyMockUtil;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Locale;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class ShieldNettyTransportTests extends ESTestCase {
    private ServerSSLService serverSSLService;
    private ClientSSLService clientSSLService;

    @Before
    public void createSSLService() throws Exception {
        Path testnodeStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");
        Settings settings = settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build();
        Environment env = new Environment(settingsBuilder().put("path.home", createTempDir()).build());
        serverSSLService = new ServerSSLService(settings, env);
        clientSSLService = new ClientSSLService(settings);
        clientSSLService.setEnvironment(env);
    }

    public void testThatSSLCanBeDisabledByProfile() throws Exception {
        Settings settings = settingsBuilder().put("shield.transport.ssl", true).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                settingsBuilder().put("shield.ssl", false).build());
        assertThat(factory.getPipeline().get(SslHandler.class), nullValue());
    }

    public void testThatSSLCanBeEnabledByProfile() throws Exception {
        Settings settings = settingsBuilder().put("shield.transport.ssl", false).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                settingsBuilder().put("shield.ssl", true).build());
        assertThat(factory.getPipeline().get(SslHandler.class), notNullValue());
    }

    public void testThatProfileTakesDefaultSSLSetting() throws Exception {
        Settings settings = settingsBuilder().put("shield.transport.ssl", true).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine(), notNullValue());
    }

    public void testDefaultClientAuth() throws Exception {
        Settings settings = settingsBuilder().put("shield.transport.ssl", true).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT), "true");
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", true)
                .put(ShieldNettyTransport.TRANSPORT_CLIENT_AUTH_SETTING, value).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NO.name(), "false", "FALSE", SSLClientAuth.NO.name().toLowerCase(Locale.ROOT));
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", true)
                .put(ShieldNettyTransport.TRANSPORT_CLIENT_AUTH_SETTING, value).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", true)
                .put(ShieldNettyTransport.TRANSPORT_CLIENT_AUTH_SETTING, value).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(true));
    }

    public void testProfileRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT), "true", "TRUE");
        Settings settings = settingsBuilder().put("shield.transport.ssl", true).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                Settings.builder().put(ShieldNettyTransport.TRANSPORT_PROFILE_CLIENT_AUTH_SETTING, value).build());
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testProfileNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NO.name(), "false", "FALSE", SSLClientAuth.NO.name().toLowerCase(Locale.ROOT));
        Settings settings = settingsBuilder().put("shield.transport.ssl", true).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                Settings.builder().put(ShieldNettyTransport.TRANSPORT_PROFILE_CLIENT_AUTH_SETTING, value).build());
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testProfileOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = settingsBuilder().put("shield.transport.ssl", true).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class),
                mock(NetworkService.class), mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService,
                mock(NamedWriteableRegistry.class));
        NettyMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                Settings.builder().put(ShieldNettyTransport.TRANSPORT_PROFILE_CLIENT_AUTH_SETTING, value).build());
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(true));
    }
}
