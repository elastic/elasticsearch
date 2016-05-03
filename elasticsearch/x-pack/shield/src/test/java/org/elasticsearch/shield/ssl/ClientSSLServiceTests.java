/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Before;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ClientSSLServiceTests extends ESTestCase {

    Environment env;
    Path testclientStore;

    @Before
    public void setup() throws Exception {
        testclientStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks");
        env = randomBoolean() ? new Environment(Settings.builder().put("path.home", createTempDir()).build()) : null;
    }

    public void testThatInvalidProtocolThrowsException() throws Exception {
        try {
            Settings settings = Settings.builder()
                    .put("xpack.security.ssl.protocol", "non-existing")
                    .put("xpack.security.ssl.keystore.path", testclientStore)
                    .put("xpack.security.ssl.keystore.password", "testclient")
                    .put("xpack.security.ssl.truststore.path", testclientStore)
                    .put("xpack.security.ssl.truststore.password", "testclient")
                    .build();
            ClientSSLService clientSSLService = new ClientSSLService(settings, new Global(settings));
            clientSSLService.createSSLEngine();
            fail("expected an exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to initialize the SSLContext"));
        }
    }

    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        Path testnodeStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");

        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .build());

        Settings.Builder settingsBuilder = Settings.builder()
                .put("truststore.path", testnodeStore)
                .put("truststore.password", "testnode");

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(settingsBuilder.build());
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    public void testThatSslContextCachingWorks() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .build());

        SSLContext sslContext = sslService.sslContext();
        SSLContext cachedSslContext = sslService.sslContext();

        assertThat(sslContext, is(sameInstance(cachedSslContext)));
    }

    public void testThatKeyStoreAndKeyCanHaveDifferentPasswords() throws Exception {
        Path differentPasswordsStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks");
        createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", differentPasswordsStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.keystore.key_password", "testnode1")
                .build()).createSSLEngine();
    }

    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks");
        try {
            createClientSSLService(Settings.builder()
                    .put("xpack.security.ssl.keystore.path", differentPasswordsStore)
                    .put("xpack.security.ssl.keystore.password", "testnode")
                    .build()).createSSLEngine();
            fail("expected an exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to initialize a KeyManagerFactory"));
        }
    }

    public void testThatSSLv3IsNotEnabled() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .build());
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(Arrays.asList(engine.getEnabledProtocols()), not(hasItem("SSLv3")));
    }

    public void testThatSSLSessionCacheHasDefaultLimits() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .build());
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(1000));
        assertThat(context.getSessionTimeout(), equalTo((int) TimeValue.timeValueHours(24).seconds()));
    }

    public void testThatSettingSSLSessionCacheLimitsWorks() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .put("xpack.security.ssl.session.cache_size", "300")
                .put("xpack.security.ssl.session.cache_timeout", "600s")
                .build());
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(300));
        assertThat(context.getSessionTimeout(), equalTo(600));
    }

    public void testThatCreateClientSSLEngineWithoutAnySettingsWorks() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put(Global.AUTO_GENERATE_SSL_SETTING.getKey(), false)
                .build());
        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngine, notNullValue());
    }

    public void testThatCreateSSLEngineWithOnlyTruststoreWorks() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.truststore.path", testclientStore)
                .put("xpack.security.ssl.truststore.password", "testclient")
                .build());
        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngine, notNullValue());
    }

    public void testThatCreateSSLEngineWithOnlyKeystoreWorks() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .build());
        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngine, notNullValue());
    }

    @Network
    public void testThatSSLContextWithoutSettingsWorks() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put(Global.AUTO_GENERATE_SSL_SETTING.getKey(), false).build());
        SSLContext sslContext = sslService.sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSslcontext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA
            // This will result in a SSLHandshakeException if the SSLContext does not trust the CA, but the default
            // truststore trusts all common public CAs so the handshake will succeed
            client.execute(new HttpGet("https://www.elastic.co/"));
        }
    }

    @Network
    public void testThatSSLContextTrustsJDKTrustedCAs() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .build());
        SSLContext sslContext = sslService.sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSslcontext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA which will succeed because the JDK
            // certs are trusted by default
            client.execute(new HttpGet("https://www.elastic.co/")).close();
        }

        sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .put(Global.INCLUDE_JDK_CERTS_SETTING.getKey(), "false")
                .build());
        sslContext = sslService.sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSslcontext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA
            // This will result in a SSLHandshakeException because the truststore is the testnodestore, which doesn't
            // trust any public CAs
            client.execute(new HttpGet("https://www.elastic.co/"));
            fail("A SSLHandshakeException should have been thrown here");
        } catch (Exception e) {
            assertThat(e, instanceOf(SSLHandshakeException.class));
        }
    }

    public void testThatTruststorePasswordIsRequired() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.truststore.path", testclientStore)
                .build());
        try {
            sslService.sslContext();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no truststore password configured"));
        }
    }

    public void testThatKeystorePasswordIsRequired() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .build());
        try {
            sslService.sslContext();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no keystore password configured"));
        }
    }

    public void testValidCiphersAndInvalidCiphersWork() throws Exception {
        List<String> ciphers = new ArrayList<>(Global.DEFAULT_CIPHERS);
        ciphers.add("foo");
        ciphers.add("bar");
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .putArray("xpack.security.ssl.ciphers", ciphers.toArray(new String[ciphers.size()]))
                .build());
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(engine, is(notNullValue()));
        String[] enabledCiphers = engine.getEnabledCipherSuites();
        assertThat(Arrays.asList(enabledCiphers), not(contains("foo", "bar")));
    }

    public void testInvalidCiphersOnlyThrowsException() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .putArray("xpack.security.ssl.ciphers", new String[] { "foo", "bar" })
                .build());
        try {
            sslService.createSSLEngine();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("none of the ciphers [foo, bar] are supported by this JVM"));
        }
    }

    public void testThatSSLSocketFactoryHasProperCiphersAndProtocols() throws Exception {
        ClientSSLService sslService = createClientSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .build());
        SSLSocketFactory factory = sslService.sslSocketFactory(Settings.EMPTY);
        final String[] ciphers = sslService.supportedCiphers(factory.getSupportedCipherSuites(), sslService.ciphers(), false);
        assertThat(factory.getDefaultCipherSuites(), is(ciphers));

        try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
            assertThat(socket.getEnabledCipherSuites(), is(ciphers));
            assertThat(socket.getEnabledProtocols(), is(sslService.supportedProtocols()));
        }
    }

    ClientSSLService createClientSSLService(Settings settings) {
        ClientSSLService clientSSLService = new ClientSSLService(settings, new Global(settings));
        clientSSLService.setEnvironment(env);
        return clientSSLService;
    }
}
