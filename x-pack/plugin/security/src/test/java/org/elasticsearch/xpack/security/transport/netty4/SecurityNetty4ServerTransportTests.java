/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.junit.Before;

import javax.net.ssl.SSLEngine;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;

public class SecurityNetty4ServerTransportTests extends ESTestCase {

    private Environment env;
    private SSLService sslService;

    @Before
    public void createSSLService() throws Exception {
        Path testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        Path testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.ssl.key", testnodeKey)
            .put("xpack.ssl.certificate", testnodeCert)
            .setSecureSettings(secureSettings)
            .put("path.home", createTempDir())
            .build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(settings, env);
    }

    private SecurityNetty4Transport createTransport() {
        return createTransport(Settings.builder().put("xpack.security.transport.ssl.enabled", true).build());
    }

    private SecurityNetty4Transport createTransport(Settings additionalSettings) {
        final Settings settings =
                Settings.builder()
                        .put("xpack.security.transport.ssl.enabled", true)
                        .put(additionalSettings)
                        .build();
        return new SecurityNetty4ServerTransport(
                settings,
                Version.CURRENT,
                mock(ThreadPool.class),
                new NetworkService(Collections.emptyList()),
                mock(BigArrays.class),
                mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class),
                null,
                sslService);
    }

    public void testThatProfileTakesDefaultSSLSetting() throws Exception {
        SecurityNetty4Transport transport = createTransport();
        ChannelHandler handler = transport.getServerChannelInitializer("default");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine(), notNullValue());
    }

    public void testDefaultClientAuth() throws Exception {
        SecurityNetty4Transport transport = createTransport();
        ChannelHandler handler = transport.getServerChannelInitializer("default");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4Transport transport = createTransport(settings);
        ChannelHandler handler = transport.getServerChannelInitializer("default");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NONE.name(), SSLClientAuth.NONE.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4Transport transport = createTransport(settings);
        ChannelHandler handler = transport.getServerChannelInitializer("default");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4Transport transport = createTransport(settings);
        ChannelHandler handler = transport.getServerChannelInitializer("default");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(true));
    }

    public void testProfileRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("transport.profiles.client.port", "8000-9000")
                .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4Transport transport = createTransport(settings);
        ChannelHandler handler = transport.getServerChannelInitializer("client");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testProfileNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NONE.name(), SSLClientAuth.NONE.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("transport.profiles.client.port", "8000-9000")
                .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4Transport transport = createTransport(settings);
        ChannelHandler handler = transport.getServerChannelInitializer("client");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testProfileOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("transport.profiles.client.port", "8000-9000")
                .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4Transport transport = createTransport(settings);
        final ChannelHandler handler = transport.getServerChannelInitializer("client");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(true));
    }

    public void testTransportSSLOverridesGlobalSSL() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode");
        Settings.Builder builder = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.key",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("xpack.security.transport.ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .put("xpack.security.transport.ssl.client_authentication", "none")
            .put("xpack.ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .setSecureSettings(secureSettings)
            .put("path.home", createTempDir());
        Settings settings = builder.build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(settings, env);
        SecurityNetty4Transport transport = createTransport(settings);
        final ChannelHandler handler = transport.getServerChannelInitializer("default");
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        final SSLEngine engine = ch.pipeline().get(SslHandler.class).engine();
        assertFalse(engine.getNeedClientAuth());
        assertFalse(engine.getWantClientAuth());

        // get the global and verify that it is different in that it requires client auth
        SSLConfiguration configuration = sslService.getSSLConfiguration("xpack.ssl");
        assertNotNull(configuration);
        final SSLEngine globalEngine = sslService.createSSLEngine(configuration, null, -1);
        assertTrue(globalEngine.getNeedClientAuth());
        assertFalse(globalEngine.getWantClientAuth());
    }
}
