/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.http.netty4.Netty4HttpMockUtil;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.Before;

import javax.net.ssl.SSLEngine;

import java.nio.file.Path;
import java.util.Locale;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class SecurityNetty4HttpServerTransportTests extends ESTestCase {

    private SSLService sslService;
    private Environment env;

    @Before
    public void createSSLService() throws Exception {
        Path testNodeStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testNodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("path.home", createTempDir())
                .build();
        env = new Environment(settings);
        sslService = new SSLService(settings, env);
    }

    public void testDefaultClientAuth() throws Exception {
        Settings settings = Settings.builder().put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true).build();
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), sslService, mock(ThreadPool.class));
        Netty4HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty4HttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), sslService, mock(ThreadPool.class));
        Netty4HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(true));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT), "true", "TRUE");
        Settings settings = Settings.builder()
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty4HttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), sslService, mock(ThreadPool.class));
        Netty4HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NO.name(), SSLClientAuth.NO.name().toLowerCase(Locale.ROOT), "false", "FALSE");
        Settings settings = Settings.builder()
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(SecurityNetty4HttpServerTransport.CLIENT_AUTH_SETTING.getKey(), value).build();
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), sslService, mock(ThreadPool.class));
        Netty4HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testCustomSSLConfiguration() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true).build();
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), sslService, mock(ThreadPool.class));
        Netty4HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelHandler handler = transport.configureServerChannelHandler();
        EmbeddedChannel ch = new EmbeddedChannel(handler);
        SSLEngine defaultEngine = ch.pipeline().get(SslHandler.class).engine();

        settings = Settings.builder()
                .put(env.settings())
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.http.ssl.supported_protocols", "TLSv1.2")
                .build();
        sslService = new SSLService(settings, new Environment(settings));
        transport = new SecurityNetty4HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), sslService, mock(ThreadPool.class));
        Netty4HttpMockUtil.setOpenChannelsHandlerToMock(transport);
        handler = transport.configureServerChannelHandler();
        ch = new EmbeddedChannel(handler);
        SSLEngine customEngine = ch.pipeline().get(SslHandler.class).engine();
        assertThat(customEngine.getEnabledProtocols(), arrayContaining("TLSv1.2"));
        assertThat(customEngine.getEnabledProtocols(), not(equalTo(defaultEngine.getEnabledProtocols())));
    }

    public void testDisablesCompressionByDefaultForSsl() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true).build();

        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityNetty4HttpServerTransport.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(HttpTransportSettings.SETTING_HTTP_COMPRESSION.get(pluginSettingsBuilder.build()), is(false));
    }

    public void testLeavesCompressionOnIfNotSsl() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), false).build();
        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityNetty4HttpServerTransport.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(pluginSettingsBuilder.build().isEmpty(), is(true));
    }

    public void testDoesNotChangeExplicitlySetCompression() throws Exception {
        Settings settings = Settings.builder()
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true)
                .put(HttpTransportSettings.SETTING_HTTP_COMPRESSION.getKey(), true)
                .build();

        Settings.Builder pluginSettingsBuilder = Settings.builder();
        SecurityNetty4HttpServerTransport.overrideSettings(pluginSettingsBuilder, settings);
        assertThat(pluginSettingsBuilder.build().isEmpty(), is(true));
    }

    public void testThatExceptionIsThrownWhenConfiguredWithoutSslKey() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .put("xpack.security.ssl.truststore.password", "testnode")
                .put(SecurityNetty4HttpServerTransport.SSL_SETTING.getKey(), true)
                .put("path.home", createTempDir())
                .build();
        env = new Environment(settings);
        sslService = new SSLService(settings, env);
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), sslService, mock(ThreadPool.class));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, transport::configureServerChannelHandler);
        assertThat(e.getMessage(), containsString("key must be provided"));
    }

    public void testNoExceptionWhenConfiguredWithoutSslKeySSLDisabled() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .put("xpack.security.ssl.truststore.password", "testnode")
                .put("path.home", createTempDir())
                .build();
        env = new Environment(settings);
        sslService = new SSLService(settings, env);
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(settings, mock(NetworkService.class),
                mock(BigArrays.class), mock(IPFilter.class), sslService, mock(ThreadPool.class));
        assertNotNull(transport.configureServerChannelHandler());
    }
}
