/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.netty.OpenChannelsHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.ssl.ServerSSLService;
import org.elasticsearch.shield.transport.SSLClientAuth;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Locale;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ShieldNettyHttpServerTransportTests extends ElasticsearchTestCase {

    private ServerSSLService serverSSLService;

    @Before
    public void createSSLService() throws Exception {
        Path testnodeStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");
        Settings settings = settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build();
        Environment env = new Environment(settingsBuilder().put("path.home", createTempDir()).build());
        ShieldSettingsFilter settingsFilter = new ShieldSettingsFilter(settings, new SettingsFilter(settings));
        serverSSLService = new ServerSSLService(settings, settingsFilter, env);
    }

    @Test
    public void testDefaultClientAuth() throws Exception {
        Settings settings = Settings.builder().put(ShieldNettyHttpServerTransport.HTTP_SSL_SETTING, true).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class), mock(BigArrays.class), mock(IPFilter.class), serverSSLService);
        setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    @Test
    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(ShieldNettyHttpServerTransport.HTTP_SSL_SETTING, true)
                .put(ShieldNettyHttpServerTransport.HTTP_CLIENT_AUTH_SETTING, value).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class), mock(BigArrays.class), mock(IPFilter.class), serverSSLService);
        setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(true));
    }

    @Test
    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT), "true", "TRUE");
        Settings settings = Settings.builder()
                .put(ShieldNettyHttpServerTransport.HTTP_SSL_SETTING, true)
                .put(ShieldNettyHttpServerTransport.HTTP_CLIENT_AUTH_SETTING, value).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class), mock(BigArrays.class), mock(IPFilter.class), serverSSLService);
        setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    @Test
    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NO.name(), SSLClientAuth.NO.name().toLowerCase(Locale.ROOT), "false", "FALSE");
        Settings settings = Settings.builder()
                .put(ShieldNettyHttpServerTransport.HTTP_SSL_SETTING, true)
                .put(ShieldNettyHttpServerTransport.HTTP_CLIENT_AUTH_SETTING, value).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class), mock(BigArrays.class), mock(IPFilter.class), serverSSLService);
        setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    /*
     * We don't really need to start Netty for these tests, but we can't create a pipeline
     * with a null handler. So we set it to a mock for this test using reflection.
     */
    private void setOpenChannelsHandlerToMock(NettyHttpServerTransport transport) throws Exception {
        Field serverOpenChannels = NettyHttpServerTransport.class.getDeclaredField("serverOpenChannels");
        serverOpenChannels.setAccessible(true);
        serverOpenChannels.set(transport, mock(OpenChannelsHandler.class));
    }
}
