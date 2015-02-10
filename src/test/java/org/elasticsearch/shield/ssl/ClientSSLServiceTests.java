/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSessionContext;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class ClientSSLServiceTests extends ElasticsearchTestCase {

    Path testclientStore;

    @Before
    public void setup() throws Exception {
        testclientStore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks").toURI());
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testThatInvalidProtocolThrowsException() throws Exception {
        new ClientSSLService(settingsBuilder()
                .put("shield.ssl.protocol", "non-existing")
                .put("shield.ssl.keystore.path", testclientStore)
                .put("shield.ssl.keystore.password", "testclient")
                .put("shield.ssl.truststore.path", testclientStore)
                .put("shield.ssl.truststore.password", "testclient")
                .build()).createSSLEngine();
    }

    @Test
    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        Path testnodeStore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI());

        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testclientStore)
                .put("shield.ssl.keystore.password", "testclient")
                .build());

        ImmutableSettings.Builder settingsBuilder = settingsBuilder()
                .put("truststore.path", testnodeStore)
                .put("truststore.password", "testnode");

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(settingsBuilder.build());
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    @Test
    public void testThatSslContextCachingWorks() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testclientStore)
                .put("shield.ssl.keystore.password", "testclient")
                .build());

        SSLContext sslContext = sslService.sslContext();
        SSLContext cachedSslContext = sslService.sslContext();

        assertThat(sslContext, is(sameInstance(cachedSslContext)));
    }

    @Test
    public void testThatKeyStoreAndKeyCanHaveDifferentPasswords() throws Exception {
        Path differentPasswordsStore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks").toURI());
        new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", differentPasswordsStore)
                .put("shield.ssl.keystore.password", "testnode")
                .put("shield.ssl.keystore.key_password", "testnode1")
                .build()).createSSLEngine();
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore = Paths.get(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks").toURI());
        new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", differentPasswordsStore)
                .put("shield.ssl.keystore.password", "testnode")
                .build()).createSSLEngine();
    }

    @Test
    public void testThatSSLv3IsNotEnabled() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testclientStore)
                .put("shield.ssl.keystore.password", "testclient")
                .build());
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(Arrays.asList(engine.getEnabledProtocols()), not(hasItem("SSLv3")));
    }

    @Test
    public void testThatSSLSessionCacheHasDefaultLimits() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testclientStore)
                .put("shield.ssl.keystore.password", "testclient")
                .build());
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(1000));
        assertThat(context.getSessionTimeout(), equalTo((int) TimeValue.timeValueHours(24).seconds()));
    }

    @Test
    public void testThatSettingSSLSessionCacheLimitsWorks() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testclientStore)
                .put("shield.ssl.keystore.password", "testclient")
                .put("shield.ssl.session.cache_size", "300")
                .put("shield.ssl.session.cache_timeout", "600s")
                .build());
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(300));
        assertThat(context.getSessionTimeout(), equalTo(600));
    }

    @Test
    public void testThatCreateClientSSLEngineWithoutAnySettingsWorks() throws Exception {
        ClientSSLService sslService = new ClientSSLService(ImmutableSettings.EMPTY);
        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngine, notNullValue());
    }

    @Test
    public void testThatCreateSSLEngineWithOnlyTruststoreWorks() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.truststore.path", testclientStore)
                .put("shield.ssl.truststore.password", "testclient")
                .build());
        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngine, notNullValue());
    }

    @Test
    public void testThatCreateSSLEngineWithOnlyKeystoreWorks() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testclientStore)
                .put("shield.ssl.keystore.password", "testclient")
                .build());
        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngine, notNullValue());
    }

    @Test
    @Network
    public void testThatSSLContextWithoutSettingsWorks() throws Exception {
        ClientSSLService sslService = new ClientSSLService(ImmutableSettings.EMPTY);
        SSLContext sslContext = sslService.sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSslcontext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA
            // This will result in a SSLHandshakeException if the SSLContext does not trust the CA, but the default
            // truststore trusts all common public CAs so the handshake will succeed
            client.execute(new HttpGet("https://www.elasticsearch.com/"));
        }
    }

    @Test
    @Network
    public void testThatSSLContextWithKeystoreDoesNotTrustAllPublicCAs() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testclientStore)
                .put("shield.ssl.keystore.password", "testclient")
                .build());
        SSLContext sslContext = sslService.sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSslcontext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA
            // This will result in a SSLHandshakeException because the truststore is the testnodestore, which doesn't
            // trust any public CAs
            client.execute(new HttpGet("https://www.elasticsearch.com/"));
            fail("A SSLHandshakeException should have been thrown here");
        } catch (Exception e) {
            assertThat(e, instanceOf(SSLHandshakeException.class));
        }
    }

    @Test(expected = ShieldSettingsException.class)
    public void testThatTruststorePasswordIsRequired() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.truststore.path", testclientStore)
                .build());
        sslService.sslContext();
    }

    @Test(expected = ShieldSettingsException.class)
    public void testThatKeystorePasswordIsRequired() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .put("shield.ssl.keystore.path", testclientStore)
                .build());
        sslService.sslContext();
    }

    @Test
    public void validCiphersAndInvalidCiphersWork() throws Exception {
        List<String> ciphers = new ArrayList<>(Arrays.asList(AbstractSSLService.DEFAULT_CIPHERS));
        ciphers.add("foo");
        ciphers.add("bar");
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .putArray("shield.ssl.ciphers", ciphers.toArray(new String[ciphers.size()]))
                .build());
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(engine, is(notNullValue()));
        String[] enabledCiphers = engine.getEnabledCipherSuites();
        assertThat(Arrays.asList(enabledCiphers), not(contains("foo", "bar")));
    }

    @Test(expected = ShieldSettingsException.class)
    public void invalidCiphersOnlyThrowsException() throws Exception {
        ClientSSLService sslService = new ClientSSLService(settingsBuilder()
                .putArray("shield.ssl.ciphers", new String[] { "foo", "bar" })
                .build());
        sslService.createSSLEngine();
    }
}
