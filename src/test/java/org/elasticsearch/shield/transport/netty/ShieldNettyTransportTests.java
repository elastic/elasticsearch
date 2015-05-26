/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.netty.channel.ChannelPipelineFactory;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.ssl.ServerSSLService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Path;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.mockito.Mockito.mock;
import static org.hamcrest.Matchers.*;

public class ShieldNettyTransportTests extends ElasticsearchTestCase {

    private ServerSSLService serverSSLService;
    private ClientSSLService clientSSLService;
    private ShieldSettingsFilter settingsFilter;

    @Before
    public void createSSLService() throws Exception {
        Path testnodeStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");
        Settings settings = settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build();
        Environment env = new Environment(settingsBuilder().put("path.home", createTempDir()).build());
        settingsFilter = new ShieldSettingsFilter(settings, new SettingsFilter(settings));
        serverSSLService = new ServerSSLService(settings, settingsFilter, env);
        clientSSLService = new ClientSSLService(settings, env);
    }

    @Test
    public void testThatSSLCanBeDisabledByProfile() throws Exception {
        Settings settings = settingsBuilder().put("shield.transport.ssl", true).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class), mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, settingsFilter);
        setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", settingsBuilder().put("shield.ssl", false).build());
        assertThat(factory.getPipeline().get(SslHandler.class), nullValue());
    }

    @Test
    public void testThatSSLCanBeEnabledByProfile() throws Exception {
        Settings settings = settingsBuilder().put("shield.transport.ssl", false).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class), mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, settingsFilter);
        setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", settingsBuilder().put("shield.ssl", true).build());
        assertThat(factory.getPipeline().get(SslHandler.class), notNullValue());
    }

    @Test
    public void testThatProfileTakesDefaultSSLSetting() throws Exception {
        Settings settings = settingsBuilder().put("shield.transport.ssl", true).build();
        ShieldNettyTransport transport = new ShieldNettyTransport(settings, mock(ThreadPool.class), mock(NetworkService.class), mock(BigArrays.class), Version.CURRENT, null, serverSSLService, clientSSLService, settingsFilter);
        setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class), notNullValue());
    }

    /*
     * We don't really need to start Netty for these tests, but we can't create a pipeline
     * with a null handler. So we set it to a mock for this test using reflection.
     */
    private void setOpenChannelsHandlerToMock(NettyTransport transport) throws Exception {
        Field serverOpenChannels = NettyTransport.class.getDeclaredField("serverOpenChannels");
        serverOpenChannels.setAccessible(true);
        serverOpenChannels.set(transport, mock(OpenChannelsHandler.class));
    }
}
