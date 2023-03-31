/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;

import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.AbstractHttpServerTransportTestCase;
import org.elasticsearch.http.NullDispatcher;
import org.elasticsearch.http.netty4.Netty4HttpHeaderValidator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLClientAuth;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;

import javax.net.ssl.SSLEngine;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class SecurityNetty4HttpServerTransportTests extends AbstractHttpServerTransportTestCase {

    private SSLService sslService;
    private Environment env;
    private Path testnodeCert;
    private Path testnodeKey;

    @Before
    public void createSSLService() {
        testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        Settings.Builder builder = Settings.builder();
        if (inFipsJvm()) {
            builder.put(XPackSettings.DIAGNOSE_TRUST_EXCEPTIONS_SETTING.getKey(), false);
        }
        Settings settings = builder.put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.key", testnodeKey)
            .put("xpack.security.http.ssl.certificate", testnodeCert)
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings)
            .build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(settings, env);
    }

    public void testDefaultClientAuth() throws Exception {
        Settings settings = Settings.builder().put(env.settings()).put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        sslService = new SSLService(settings, env);
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(BigArrays.class),
            mock(IPFilter.class),
            sslService,
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            randomFrom(Netty4HttpHeaderValidator.NOOP_VALIDATOR, null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value)
            .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(BigArrays.class),
            mock(IPFilter.class),
            sslService,
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            randomFrom(Netty4HttpHeaderValidator.NOOP_VALIDATOR, null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(true));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value)
            .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(BigArrays.class),
            mock(IPFilter.class),
            sslService,
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            randomFrom(Netty4HttpHeaderValidator.NOOP_VALIDATOR, null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(true));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NONE.name(), SSLClientAuth.NONE.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.client_authentication", value)
            .build();
        sslService = new SSLService(settings, env);
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(BigArrays.class),
            mock(IPFilter.class),
            sslService,
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            randomFrom(Netty4HttpHeaderValidator.NOOP_VALIDATOR, null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        final EmbeddedChannel ch = new EmbeddedChannel(handler);
        assertThat(ch.pipeline().get(SslHandler.class).engine().getNeedClientAuth(), is(false));
        assertThat(ch.pipeline().get(SslHandler.class).engine().getWantClientAuth(), is(false));
    }

    public void testCustomSSLConfiguration() throws Exception {
        Settings settings = Settings.builder().put(env.settings()).put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true).build();
        sslService = new SSLService(settings, env);
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(BigArrays.class),
            mock(IPFilter.class),
            sslService,
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            randomFrom(Netty4HttpHeaderValidator.NOOP_VALIDATOR, null)
        );
        ChannelHandler handler = transport.configureServerChannelHandler();
        EmbeddedChannel ch = new EmbeddedChannel(handler);
        SSLEngine defaultEngine = ch.pipeline().get(SslHandler.class).engine();

        settings = Settings.builder()
            .put(env.settings())
            .put(XPackSettings.HTTP_SSL_ENABLED.getKey(), true)
            .put("xpack.security.http.ssl.supported_protocols", "TLSv1.2")
            .build();
        sslService = new SSLService(settings, TestEnvironment.newEnvironment(settings));
        transport = new SecurityNetty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(BigArrays.class),
            mock(IPFilter.class),
            sslService,
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            randomFrom(Netty4HttpHeaderValidator.NOOP_VALIDATOR, null)
        );
        handler = transport.configureServerChannelHandler();
        ch = new EmbeddedChannel(handler);
        SSLEngine customEngine = ch.pipeline().get(SslHandler.class).engine();
        assertThat(customEngine.getEnabledProtocols(), arrayContaining("TLSv1.2"));
        assertThat(customEngine.getEnabledProtocols(), not(equalTo(defaultEngine.getEnabledProtocols())));
    }

    public void testNoExceptionWhenConfiguredWithoutSslKeySSLDisabled() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        Settings.Builder builder = Settings.builder();
        if (inFipsJvm()) {
            builder.put(XPackSettings.DIAGNOSE_TRUST_EXCEPTIONS_SETTING.getKey(), false);
        }
        Settings settings = builder.put("xpack.security.http.ssl.enabled", false)
            .put("xpack.security.http.ssl.key", testnodeKey)
            .put("xpack.security.http.ssl.certificate", testnodeCert)
            .setSecureSettings(secureSettings)
            .put("path.home", createTempDir())
            .build();
        env = TestEnvironment.newEnvironment(settings);
        sslService = new SSLService(settings, env);
        SecurityNetty4HttpServerTransport transport = new SecurityNetty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            mock(BigArrays.class),
            mock(IPFilter.class),
            sslService,
            mock(ThreadPool.class),
            xContentRegistry(),
            new NullDispatcher(),
            randomClusterSettings(),
            new SharedGroupFactory(settings),
            randomFrom(Netty4HttpHeaderValidator.NOOP_VALIDATOR, null)
        );
        assertNotNull(transport.configureServerChannelHandler());
    }
}
