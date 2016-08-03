/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty3;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.netty3.Netty3HttpMockUtil;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.xpack.security.ssl.ServerSSLService;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
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

public class SecurityNetty3HttpServerTransportTests extends ESTestCase {

    private ServerSSLService serverSSLService;

    @Before
    public void createSSLService() throws Exception {
        Path testnodeStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        serverSSLService = new ServerSSLService(settings, env, new Global(settings));
    }

    public void testDefaultClientAuth() throws Exception {
        Settings settings = Settings.builder().put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true).build();
        SecurityNetty3HttpServerTransport transport = new SecurityNetty3HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class));
        Netty3HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty3HttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty3HttpServerTransport transport = new SecurityNetty3HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class));
        Netty3HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(true));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT), "true", "TRUE");
        Settings settings = Settings.builder()
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty3HttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty3HttpServerTransport transport = new SecurityNetty3HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class));
        Netty3HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NO.name(), SSLClientAuth.NO.name().toLowerCase(Locale.ROOT), "false", "FALSE");
        Settings settings = Settings.builder()
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty3HttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty3HttpServerTransport transport = new SecurityNetty3HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class));
        Netty3HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testCustomSSLConfiguration() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true).build();
        SecurityNetty3HttpServerTransport transport = new SecurityNetty3HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class));
        Netty3HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory();
        SSLEngine defaultEngine = factory.getPipeline().get(SslHandler.class).getEngine();

        settings = Settings.builder()
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.http.ssl.supported_protocols", "TLSv1.2")
                .build();
        transport = new SecurityNetty3HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), serverSSLService, mock(ThreadPool.class));
        Netty3HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        factory = transport.configureServerChannelPipelineFactory();
        SSLEngine customEngine = factory.getPipeline().get(SslHandler.class).getEngine();
        assertThat(customEngine.getEnabledProtocols(), arrayContaining("TLSv1.2"));
        assertThat(customEngine.getEnabledProtocols(), not(equalTo(defaultEngine.getEnabledProtocols())));
    }

    public void testDisablesCompressionByDefaultForSsl() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true).build();

        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityNetty3HttpServerTransport.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(HttpTransportSettings.SETTING_HTTP_COMPRESSION.get(pluginSettingsBuilder.build()), is(false));
    }

    public void testLeavesCompressionOnIfNotSsl() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), false).build();
        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityNetty3HttpServerTransport.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(pluginSettingsBuilder.build().isEmpty(), is(true));
    }

    public void testDoesNotChangeExplicitlySetCompression() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityNetty3HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(HttpTransportSettings.SETTING_HTTP_COMPRESSION.getKey(), true)
                .build();

        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityNetty3HttpServerTransport.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(pluginSettingsBuilder.build().isEmpty(), is(true));
    }
}
