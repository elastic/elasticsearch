/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.http.nio.NioHttpChannel;
import org.elasticsearch.nio.Config;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.nio.NioGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.SSLEngineUtils;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.Before;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityNioHttpServerTransportTests extends ESTestCase {

    private SSLService sslService;
    private Environment env;
    private InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    private NioGroupFactory nioGroupFactory;

    @Before
    public void createSSLService() {
        Path testNodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");
        Path testNodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.key", testNodeKey)
            .put("xpack.security.http.ssl.certificate", testNodeCert)
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings)
            .build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(settings, env);
    }

    public void testDefaultClientAuth() throws IOException {
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        nioGroupFactory = new NioGroupFactory(settings, logger);
        sslService = new SSLService(settings, env);
        SecurityNioHttpServerTransport transport = new SecurityNioHttpServerTransport(settings,
            new NetworkService(Collections.emptyList()), mock(BigArrays.class), mock(PageCacheRecycler.class), mock(ThreadPool.class),
            xContentRegistry(), new NullDispatcher(), mock(IPFilter.class), sslService, nioGroupFactory);
        SecurityNioHttpServerTransport.SecurityHttpChannelFactory factory = transport.channelFactory();
        SocketChannel socketChannel = mock(SocketChannel.class);
        when(socketChannel.getRemoteAddress()).thenReturn(address);
        NioHttpChannel channel = factory.createChannel(mock(NioSelector.class), socketChannel, mock(Config.Socket.class));
        SSLEngine engine = SSLEngineUtils.getSSLEngine(channel);

        assertThat(engine.getNeedClientAuth(), is(false));
        assertThat(engine.getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws IOException {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value).build();
        sslService = new SSLService(settings, env);
        nioGroupFactory = new NioGroupFactory(settings, logger);
        SecurityNioHttpServerTransport transport = new SecurityNioHttpServerTransport(settings,
            new NetworkService(Collections.emptyList()), mock(BigArrays.class), mock(PageCacheRecycler.class), mock(ThreadPool.class),
            xContentRegistry(), new NullDispatcher(), mock(IPFilter.class), sslService, nioGroupFactory);

        SecurityNioHttpServerTransport.SecurityHttpChannelFactory factory = transport.channelFactory();
        SocketChannel socketChannel = mock(SocketChannel.class);
        when(socketChannel.getRemoteAddress()).thenReturn(address);
        NioHttpChannel channel = factory.createChannel(mock(NioSelector.class), socketChannel, mock(Config.Socket.class));
        SSLEngine engine = SSLEngineUtils.getSSLEngine(channel);
        assertThat(engine.getNeedClientAuth(), is(false));
        assertThat(engine.getWantClientAuth(), is(true));
    }

    public void testRequiredClientAuth() throws IOException {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value).build();
        nioGroupFactory = new NioGroupFactory(settings, logger);
        sslService = new SSLService(settings, env);
        SecurityNioHttpServerTransport transport = new SecurityNioHttpServerTransport(settings,
            new NetworkService(Collections.emptyList()), mock(BigArrays.class), mock(PageCacheRecycler.class), mock(ThreadPool.class),
            xContentRegistry(), new NullDispatcher(), mock(IPFilter.class), sslService, nioGroupFactory);

        SecurityNioHttpServerTransport.SecurityHttpChannelFactory factory = transport.channelFactory();
        SocketChannel socketChannel = mock(SocketChannel.class);
        when(socketChannel.getRemoteAddress()).thenReturn(address);
        NioHttpChannel channel = factory.createChannel(mock(NioSelector.class), socketChannel, mock(Config.Socket.class));
        SSLEngine engine = SSLEngineUtils.getSSLEngine(channel);
        assertThat(engine.getNeedClientAuth(), is(true));
        assertThat(engine.getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws IOException {
        String value = randomFrom(SSLClientAuth.NONE.name(), SSLClientAuth.NONE.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value).build();
        sslService = new SSLService(settings, env);
        nioGroupFactory = new NioGroupFactory(settings, logger);
        SecurityNioHttpServerTransport transport = new SecurityNioHttpServerTransport(settings,
            new NetworkService(Collections.emptyList()), mock(BigArrays.class), mock(PageCacheRecycler.class), mock(ThreadPool.class),
            xContentRegistry(), new NullDispatcher(), mock(IPFilter.class), sslService, nioGroupFactory);

        SecurityNioHttpServerTransport.SecurityHttpChannelFactory factory = transport.channelFactory();
        SocketChannel socketChannel = mock(SocketChannel.class);
        when(socketChannel.getRemoteAddress()).thenReturn(address);
        NioHttpChannel channel = factory.createChannel(mock(NioSelector.class), socketChannel, mock(Config.Socket.class));
        SSLEngine engine = SSLEngineUtils.getSSLEngine(channel);
        assertThat(engine.getNeedClientAuth(), is(false));
        assertThat(engine.getWantClientAuth(), is(false));
    }

    public void testCustomSSLConfiguration() throws IOException {
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        sslService = new SSLService(settings, env);
        nioGroupFactory = new NioGroupFactory(settings, logger);
        SecurityNioHttpServerTransport transport = new SecurityNioHttpServerTransport(settings,
            new NetworkService(Collections.emptyList()), mock(BigArrays.class), mock(PageCacheRecycler.class), mock(ThreadPool.class),
            xContentRegistry(), new NullDispatcher(), mock(IPFilter.class), sslService, nioGroupFactory);
        SecurityNioHttpServerTransport.SecurityHttpChannelFactory factory = transport.channelFactory();
        SocketChannel socketChannel = mock(SocketChannel.class);
        when(socketChannel.getRemoteAddress()).thenReturn(address);
        NioHttpChannel channel = factory.createChannel(mock(NioSelector.class), socketChannel, mock(Config.Socket.class));
        SSLEngine defaultEngine = SSLEngineUtils.getSSLEngine(channel);

        settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.supported_protocols", "TLSv1.2")
            .build();
        sslService = new SSLService(settings, TestEnvironment.newEnvironment(settings));
        nioGroupFactory = new NioGroupFactory(settings, logger);
        transport = new SecurityNioHttpServerTransport(settings,
            new NetworkService(Collections.emptyList()), mock(BigArrays.class), mock(PageCacheRecycler.class), mock(ThreadPool.class),
            xContentRegistry(), new NullDispatcher(), mock(IPFilter.class), sslService, nioGroupFactory);
        factory = transport.channelFactory();
        channel = factory.createChannel(mock(NioSelector.class), socketChannel, mock(Config.Socket.class));
        SSLEngine customEngine = SSLEngineUtils.getSSLEngine(channel);
        assertThat(customEngine.getEnabledProtocols(), arrayContaining("TLSv1.2"));
        assertThat(customEngine.getEnabledProtocols(), not(equalTo(defaultEngine.getEnabledProtocols())));
    }

    public void testThatExceptionIsThrownWhenConfiguredWithoutSslKey() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
            .setSecureSettings(secureSettings)
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("path.home", createTempDir())
            .build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(settings, env);
        nioGroupFactory = new NioGroupFactory(settings, logger);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new SecurityNioHttpServerTransport(settings,
                new NetworkService(Collections.emptyList()), mock(BigArrays.class), mock(PageCacheRecycler.class), mock(ThreadPool.class),
                xContentRegistry(), new NullDispatcher(), mock(IPFilter.class), sslService, nioGroupFactory));
        assertThat(e.getMessage(), containsString("key must be provided"));
    }

    public void testNoExceptionWhenConfiguredWithoutSslKeySSLDisabled() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
            .setSecureSettings(secureSettings)
            .put("path.home", createTempDir())
            .build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(settings, env);
        nioGroupFactory = new NioGroupFactory(settings, logger);
        SecurityNioHttpServerTransport transport = new SecurityNioHttpServerTransport(settings,
            new NetworkService(Collections.emptyList()), mock(BigArrays.class), mock(PageCacheRecycler.class), mock(ThreadPool.class),
            xContentRegistry(), new NullDispatcher(), mock(IPFilter.class), sslService, nioGroupFactory);
    }
}
