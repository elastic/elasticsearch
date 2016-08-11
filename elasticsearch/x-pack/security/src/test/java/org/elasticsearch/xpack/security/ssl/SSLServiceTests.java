/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.test.ESTestCase;
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

public class SSLServiceTests extends ESTestCase {

    private Path testnodeStore;
    private Path testclientStore;
    private Environment env;

    @Before
    public void setup() throws Exception {
        testnodeStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        testclientStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks");
        env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
    }

    public void testThatInvalidProtocolThrowsException() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.protocol", "non-existing")
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.truststore.path", testnodeStore)
                .put("xpack.security.ssl.truststore.password", "testnode")
                .build();
        try {
            new SSLService(settings, env);
            fail("expected an exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to initialize the SSLContext"));
        }
    }

    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        Path testClientStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks");

        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("transport.profiles.foo.xpack.security.ssl.truststore.path", testClientStore)
                .put("transport.profiles.foo.xpack.security.ssl.truststore.password", "testclient")
                .build();
        SSLService sslService = new SSLService(settings, env);

        Settings customTruststoreSettings = Settings.builder()
                .put("ssl.truststore.path", testClientStore)
                .put("ssl.truststore.password", "testclient")
                .build();

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(customTruststoreSettings);
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine(Settings.EMPTY);
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    public void testThatSslContextCachingWorks() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);

        SSLContext sslContext = sslService.sslContext();
        SSLContext cachedSslContext = sslService.sslContext();

        assertThat(sslContext, is(sameInstance(cachedSslContext)));
    }

    public void testThatKeyStoreAndKeyCanHaveDifferentPasswords() throws Exception {
        Path differentPasswordsStore =
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-different-passwords.jks");
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", differentPasswordsStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.keystore.key_password", "testnode1")
                .build();
        new SSLService(settings, env).createSSLEngine(Settings.EMPTY);
    }

    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore =
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-different-passwords.jks");
        try {
            Settings settings = Settings.builder()
                    .put("xpack.security.ssl.keystore.path", differentPasswordsStore)
                    .put("xpack.security.ssl.keystore.password", "testnode")
                    .build();
            new SSLService(settings, env).createSSLEngine(Settings.EMPTY);
            fail("expected an exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to initialize a KeyManagerFactory"));
        }
    }

    public void testThatSSLv3IsNotEnabled() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine engine = sslService.createSSLEngine(Settings.EMPTY);
        assertThat(Arrays.asList(engine.getEnabledProtocols()), not(hasItem("SSLv3")));
    }

    public void testThatSSLSessionCacheHasDefaultLimits() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(1000));
        assertThat(context.getSessionTimeout(), equalTo((int) TimeValue.timeValueHours(24).seconds()));
    }

    public void testThatSettingSSLSessionCacheLimitsWorks() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.session.cache_size", "300")
                .put("xpack.security.ssl.session.cache_timeout", "600s")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(300));
        assertThat(context.getSessionTimeout(), equalTo(600));
    }

    public void testCreateWithoutAnySettingsNotValidForServer() throws Exception {
        SSLService sslService = new SSLService(Settings.EMPTY, env);
        assertFalse(sslService.isConfigurationValidForServerUsage(Settings.EMPTY));
    }

    public void testCreateWithOnlyTruststoreNotValidForServer() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.truststore.path", testnodeStore)
                .put("xpack.security.ssl.truststore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        assertFalse(sslService.isConfigurationValidForServerUsage(Settings.EMPTY));
    }

    public void testCreateWithKeystoreIsValidForServer() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        assertTrue(sslService.isConfigurationValidForServerUsage(Settings.EMPTY));
    }

    public void testThatCreateClientSSLEngineWithoutAnySettingsWorks() throws Exception {
        SSLService sslService = new SSLService(Settings.EMPTY, env);
        SSLEngine sslEngine = sslService.createSSLEngine(Settings.EMPTY);
        assertThat(sslEngine, notNullValue());
    }

    public void testThatCreateSSLEngineWithOnlyTruststoreWorks() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.truststore.path", testclientStore)
                .put("xpack.security.ssl.truststore.password", "testclient")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine sslEngine = sslService.createSSLEngine(Settings.EMPTY);
        assertThat(sslEngine, notNullValue());
    }

    public void testThatTruststorePasswordIsRequired() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.truststore.path", testnodeStore)
                .build();
        IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("no truststore password configured"));
    }

    public void testThatKeystorePasswordIsRequired() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .build();
        IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("no keystore password configured"));
    }

    public void testCiphersAndInvalidCiphersWork() throws Exception {
        List<String> ciphers = new ArrayList<>(Global.DEFAULT_CIPHERS);
        ciphers.add("foo");
        ciphers.add("bar");
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .putArray("xpack.security.ssl.ciphers", ciphers.toArray(new String[ciphers.size()]))
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine engine = sslService.createSSLEngine(Settings.EMPTY);
        assertThat(engine, is(notNullValue()));
        String[] enabledCiphers = engine.getEnabledCipherSuites();
        assertThat(Arrays.asList(enabledCiphers), not(contains("foo", "bar")));
    }

    public void testInvalidCiphersOnlyThrowsException() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .putArray("xpack.security.ssl.ciphers", new String[] { "foo", "bar" })
                .build();
        IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("none of the ciphers [foo, bar] are supported by this JVM"));
    }

    public void testThatSSLSocketFactoryHasProperCiphersAndProtocols() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLSocketFactory factory = sslService.sslSocketFactory(Settings.EMPTY);
        SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        final String[] ciphers = sslService.supportedCiphers(factory.getSupportedCipherSuites(), config.ciphers(), false);
        assertThat(factory.getDefaultCipherSuites(), is(ciphers));

        try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
            assertThat(socket.getEnabledCipherSuites(), is(ciphers));
            assertThat(socket.getEnabledProtocols(), is(config.supportedProtocols().toArray(Strings.EMPTY_ARRAY)));
        }
    }

    @Network
    public void testThatSSLContextWithoutSettingsWorks() throws Exception {
        SSLService sslService = new SSLService(Settings.EMPTY, env);
        SSLContext sslContext = sslService.sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA
            // This will result in a SSLHandshakeException if the SSLContext does not trust the CA, but the default
            // truststore trusts all common public CAs so the handshake will succeed
            client.execute(new HttpGet("https://www.elastic.co/"));
        }
    }

    @Network
    public void testThatSSLContextTrustsJDKTrustedCAs() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .build();
        SSLContext sslContext = new SSLService(settings, env).sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA which will succeed because the JDK
            // certs are trusted by default
            client.execute(new HttpGet("https://www.elastic.co/")).close();
        }

        settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testclientStore)
                .put("xpack.security.ssl.keystore.password", "testclient")
                .put(Global.INCLUDE_JDK_CERTS_SETTING.getKey(), "false")
                .build();
        sslContext = new SSLService(settings, env).sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA
            // This will result in a SSLHandshakeException because the truststore is the testnodestore, which doesn't
            // trust any public CAs
            client.execute(new HttpGet("https://www.elastic.co/"));
            fail("A SSLHandshakeException should have been thrown here");
        } catch (Exception e) {
            assertThat(e, instanceOf(SSLHandshakeException.class));
        }
    }
}
