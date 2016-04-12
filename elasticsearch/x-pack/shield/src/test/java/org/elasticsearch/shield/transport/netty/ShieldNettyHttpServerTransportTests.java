/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.netty.NettyHttpMockUtil;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.shield.ssl.ServerSSLService;
import org.elasticsearch.shield.transport.SSLClientAuth;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.junit.Before;

import javax.net.ssl.SSLEngine;
import java.nio.file.Path;
import java.util.Locale;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class ShieldNettyHttpServerTransportTests extends ESTestCase {

    private ServerSSLService serverSSLService;

    @Before
    public void createSSLService() throws Exception {
        Path testnodeStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        serverSSLService = new ServerSSLService(settings, env, new Global(settings), null);
    }

    public void testDefaultClientAuth() throws Exception {
        Settings settings = Settings.builder().put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class), mock(Global.class));
        NettyHttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put(ShieldNettyHttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class), mock(Global.class));
        NettyHttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(true));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT), "true", "TRUE");
        Settings settings = Settings.builder()
                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put(ShieldNettyHttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class), mock(Global.class));
        NettyHttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NO.name(), SSLClientAuth.NO.name().toLowerCase(Locale.ROOT), "false", "FALSE");
        Settings settings = Settings.builder()
                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put(ShieldNettyHttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class), mock(Global.class));
        NettyHttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testCustomSSLConfiguration() throws Exception {
        Settings settings = Settings.builder()
                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true).build();
        ShieldNettyHttpServerTransport transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class), mock(Global.class));
        NettyHttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        SSLEngine defaultEngine = factory.getPipeline().get(SslHandler.class).getEngine();

        settings = Settings.builder()
                .put(ShieldNettyHttpServerTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.http.ssl.supported_protocols", "TLSv1.2")
                .build();
        transport = new ShieldNettyHttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class), mock(Global.class));
        NettyHttpMockUtil.setOpenChannelsHandlerToMock(transport);
        factory = transport.configureServerChannelPipelineFactory();
        SSLEngine customEngine = factory.getPipeline().get(SslHandler.class).getEngine();
        assertThat(customEngine.getEnabledProtocols(), arrayContaining("TLSv1.2"));
        assertThat(customEngine.getEnabledProtocols(), not(equalTo(defaultEngine.getEnabledProtocols())));
    }
}
