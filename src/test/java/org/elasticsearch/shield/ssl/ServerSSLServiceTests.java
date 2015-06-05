/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

public class ServerSSLServiceTests extends ElasticsearchTestCase {

    Path testnodeStore;
    ShieldSettingsFilter settingsFilter;
    Environment env;

    @Before
    public void setup() throws Exception {
        testnodeStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");
        settingsFilter = mock(ShieldSettingsFilter.class);
        env = new Environment(settingsBuilder().put("path.home", createTempDir()).build());
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testThatInvalidProtocolThrowsException() throws Exception {
        Settings settings = settingsBuilder()
                .put("shield.ssl.protocol", "non-existing")
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .put("shield.ssl.truststore.path", testnodeStore)
                .put("shield.ssl.truststore.password", "testnode")
                .build();
        new ServerSSLService(settings, settingsFilter, env).createSSLEngine();
    }

    @Test
    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        Path testClientStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks");

        Settings settings = settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build();
        ServerSSLService sslService = new ServerSSLService(settings, settingsFilter, env);

        Settings.Builder settingsBuilder = settingsBuilder()
                .put("truststore.path", testClientStore)
                .put("truststore.password", "testclient");

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(settingsBuilder.build());
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    @Test
    public void testThatSslContextCachingWorks() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
            .put("shield.ssl.keystore.path", testnodeStore)
            .put("shield.ssl.keystore.password", "testnode")
            .build(), settingsFilter, env);

        SSLContext sslContext = sslService.sslContext();
        SSLContext cachedSslContext = sslService.sslContext();

        assertThat(sslContext, is(sameInstance(cachedSslContext)));
    }

    @Test
    public void testThatKeyStoreAndKeyCanHaveDifferentPasswords() throws Exception {
        Path differentPasswordsStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks");
        new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", differentPasswordsStore)
                .put("shield.ssl.keystore.password", "testnode")
                .put("shield.ssl.keystore.key_password", "testnode1")
                .build(), settingsFilter, env).createSSLEngine();
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks");
        new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", differentPasswordsStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build(), settingsFilter, env).createSSLEngine();
    }

    @Test
    public void testThatSSLv3IsNotEnabled() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build(), settingsFilter, env);
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(Arrays.asList(engine.getEnabledProtocols()), not(hasItem("SSLv3")));
    }

    @Test
    public void testThatSSLSessionCacheHasDefaultLimits() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build(), settingsFilter, env);
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(1000));
        assertThat(context.getSessionTimeout(), equalTo((int) TimeValue.timeValueHours(24).seconds()));
    }

    @Test
    public void testThatSettingSSLSessionCacheLimitsWorks() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .put("shield.ssl.session.cache_size", "300")
                .put("shield.ssl.session.cache_timeout", "600s")
                .build(), settingsFilter, env);
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(300));
        assertThat(context.getSessionTimeout(), equalTo(600));
    }

    @Test(expected = ShieldSettingsException.class)
    public void testThatCreateSSLEngineWithoutAnySettingsDoesNotWork() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.EMPTY, settingsFilter, env);
        sslService.createSSLEngine();
    }

    @Test(expected = ShieldSettingsException.class)
    public void testThatCreateSSLEngineWithOnlyTruststoreDoesNotWork() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.truststore.path", testnodeStore)
                .put("shield.ssl.truststore.password", "testnode")
                .build(), settingsFilter, env);
        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngine, notNullValue());
    }

    @Test(expected = ShieldSettingsException.class)
    public void testThatTruststorePasswordIsRequired() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .put("shield.ssl.truststore.path", testnodeStore)
                .build(), settingsFilter, env);
        sslService.sslContext();
    }

    @Test(expected = ShieldSettingsException.class)
    public void testThatKeystorePasswordIsRequired() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .build(), settingsFilter, env);
        sslService.sslContext();
    }

    @Test
    public void validCiphersAndInvalidCiphersWork() throws Exception {
        List<String> ciphers = new ArrayList<>(Arrays.asList(AbstractSSLService.DEFAULT_CIPHERS));
        ciphers.add("foo");
        ciphers.add("bar");
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .putArray("shield.ssl.ciphers", ciphers.toArray(new String[ciphers.size()]))
                .build(), settingsFilter, env);
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(engine, is(notNullValue()));
        String[] enabledCiphers = engine.getEnabledCipherSuites();
        assertThat(Arrays.asList(enabledCiphers), not(contains("foo", "bar")));
    }

    @Test(expected = ShieldSettingsException.class)
    public void invalidCiphersOnlyThrowsException() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .putArray("shield.ssl.ciphers", new String[] { "foo", "bar" })
                .build(), settingsFilter, env);
        sslService.createSSLEngine();
    }

    @Test
    public void testThatSSLSocketFactoryHasProperCiphersAndProtocols() throws Exception {
        ServerSSLService sslService = new ServerSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testnodeStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build(), settingsFilter, env);
        SSLSocketFactory factory = sslService.sslSocketFactory();
        assertThat(factory.getDefaultCipherSuites(), is(sslService.ciphers()));

        try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
            assertThat(socket.getEnabledCipherSuites(), is(sslService.ciphers()));
            assertThat(socket.getEnabledProtocols(), is(sslService.supportedProtocols()));
        }
    }
}
