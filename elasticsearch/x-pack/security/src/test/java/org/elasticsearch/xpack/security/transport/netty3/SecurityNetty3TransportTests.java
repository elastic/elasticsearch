/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty3;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.SSLClientAuth;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty3.Netty3MockUtil;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.junit.Before;

import javax.net.ssl.SSLEngine;
import java.nio.file.Path;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class SecurityNetty3TransportTests extends ESTestCase {

    private Environment env;
    private SSLService sslService;

    @Before
    public void createSSLService() throws Exception {
        Path testnodeStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .build();
        env = new Environment(settings);
        sslService = new SSLService(settings, env);
    }

    public void testThatSSLCanBeDisabledByProfile() throws Exception {
        Settings settings = Settings.builder().put("xpack.security.transport.ssl.enabled", true).build();
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                Settings.builder().put("xpack.security.ssl.enabled", false).build());
        assertThat(factory.getPipeline().get(SslHandler.class), nullValue());
    }

    public void testThatSSLCanBeEnabledByProfile() throws Exception {
        Settings settings = Settings.builder().put("xpack.security.transport.ssl.enabled", false).build();
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                Settings.builder().put("xpack.security.ssl.enabled", true).build());
        assertThat(factory.getPipeline().get(SslHandler.class), notNullValue());
    }

    public void testThatProfileTakesDefaultSSLSetting() throws Exception {
        Settings settings = Settings.builder().put("xpack.security.transport.ssl.enabled", true).build();
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine(), notNullValue());
    }

    public void testDefaultClientAuth() throws Exception {
        Settings settings = Settings.builder().put("xpack.security.transport.ssl.enabled", true).build();
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.security.transport.ssl.enabled", true)
                .put("xpack.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NONE.name(), SSLClientAuth.NONE.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.security.transport.ssl.enabled", true)
                .put("xpack.ssl.client_authentication", value).build();
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.security.transport.ssl.enabled", true)
                .put("xpack.ssl.client_authentication", value).build();
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
               mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client", Settings.EMPTY);
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(true));
    }

    public void testProfileRequiredClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.REQUIRED.name(), SSLClientAuth.REQUIRED.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.security.transport.ssl.enabled", true)
                .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                Settings.builder().put("xpack.security.ssl.client_authentication", value).build());
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(true));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testProfileNoClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.NONE.name(), SSLClientAuth.NONE.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.security.transport.ssl.enabled", true)
                .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class), mock(NetworkService.class),
                mock(BigArrays.class), null, sslService, mock(NamedWriteableRegistry.class),
                mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                Settings.builder().put("xpack.security.ssl.client_authentication", value).build());
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(false));
    }

    public void testProfileOptionalClientAuth() throws Exception {
        String value = randomFrom(SSLClientAuth.OPTIONAL.name(), SSLClientAuth.OPTIONAL.name().toLowerCase(Locale.ROOT));
        Settings settings = Settings.builder()
                .put(env.settings())
                .put("xpack.security.transport.ssl.enabled", true)
                .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
                .build();
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class),
                mock(NetworkService.class), mock(BigArrays.class), null, sslService,
                mock(NamedWriteableRegistry.class), mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("client",
                Settings.builder().put("xpack.security.ssl.client_authentication", value).build());
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getNeedClientAuth(), is(false));
        assertThat(factory.getPipeline().get(SslHandler.class).getEngine().getWantClientAuth(), is(true));
    }

    public void testThatExceptionIsThrownWhenConfiguredWithoutSslKey() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .put("xpack.ssl.truststore.password", "testnode")
                .put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), true)
                .put("path.home", createTempDir())
                .build();
        env = new Environment(settings);
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class),
                mock(NetworkService.class), mock(BigArrays.class), null, sslService,
                mock(NamedWriteableRegistry.class), mock(CircuitBreakerService.class));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> transport.configureServerChannelPipelineFactory(randomAsciiOfLength(6), Settings.EMPTY));
        assertThat(e.getMessage(), containsString("key must be provided"));
    }

    public void testNoExceptionWhenConfiguredWithoutSslKeySSLDisabled() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .put("xpack.ssl.truststore.password", "testnode")
                .put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), false)
                .put("path.home", createTempDir())
                .build();
        env = new Environment(settings);
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class),
                mock(NetworkService.class), mock(BigArrays.class), null, sslService,
                mock(NamedWriteableRegistry.class), mock(CircuitBreakerService.class));
        assertNotNull(transport.configureServerChannelPipelineFactory(randomAsciiOfLength(6), Settings.EMPTY));
    }

    public void testTransportSSLOverridesGlobalSSL() throws Exception {
        final boolean useGlobalKeystoreWithoutKey = randomBoolean();
        Settings.Builder builder = Settings.builder()
                .put("xpack.security.transport.ssl.keystore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .put("xpack.security.transport.ssl.keystore.password", "testnode")
                .put("xpack.security.transport.ssl.client_authentication", "none")
                .put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), true)
                .put("path.home", createTempDir());
        if (useGlobalKeystoreWithoutKey) {
            builder.put("xpack.ssl.keystore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/truststore-testnode-only.jks"))
                    .put("xpack.ssl.keystore.password", "truststore-testnode-only");
        }
        Settings settings = builder.build();
        env = new Environment(settings);
        sslService = new SSLService(settings, env);
        SecurityNetty3Transport transport = new SecurityNetty3Transport(settings, mock(ThreadPool.class),
                mock(NetworkService.class), mock(BigArrays.class), null, sslService,
                mock(NamedWriteableRegistry.class), mock(CircuitBreakerService.class));
        Netty3MockUtil.setOpenChannelsHandlerToMock(transport);
        ChannelPipelineFactory factory = transport.configureServerChannelPipelineFactory("default", Settings.EMPTY);
        final SSLEngine engine = factory.getPipeline().get(SslHandler.class).getEngine();
        assertFalse(engine.getNeedClientAuth());
        assertFalse(engine.getWantClientAuth());

        // get the global and verify that it is different in that it requires client auth
        final SSLEngine globalEngine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertTrue(globalEngine.getNeedClientAuth());
        assertFalse(globalEngine.getWantClientAuth());
    }
}
