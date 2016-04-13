/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ServerSSLServiceTests extends ESTestCase {
    Path testnodeStore;
    Environment env;

    @Before
    public void setup() throws Exception {
        testnodeStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks");
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
            new ServerSSLService(settings, env).createSSLEngine();
            fail("expected an exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to initialize the SSLContext"));
        }
    }

    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        Path testClientStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks");

        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        ServerSSLService sslService = new ServerSSLService(settings, env);

        Settings.Builder settingsBuilder = Settings.builder()
                .put("truststore.path", testClientStore)
                .put("truststore.password", "testclient");

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(settingsBuilder.build());
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    public void testThatSslContextCachingWorks() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
            .put("xpack.security.ssl.keystore.path", testnodeStore)
            .put("xpack.security.ssl.keystore.password", "testnode")
            .build(), env);

        SSLContext sslContext = sslService.sslContext();
        SSLContext cachedSslContext = sslService.sslContext();

        assertThat(sslContext, is(sameInstance(cachedSslContext)));
    }

    public void testThatKeyStoreAndKeyCanHaveDifferentPasswords() throws Exception {
        Path differentPasswordsStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks");
        new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", differentPasswordsStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.keystore.key_password", "testnode1")
                .build(), env).createSSLEngine();
    }

    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks");
        try {
            new ServerSSLService(Settings.builder()
                    .put("xpack.security.ssl.keystore.path", differentPasswordsStore)
                    .put("xpack.security.ssl.keystore.password", "testnode")
                    .build(), env).createSSLEngine();
            fail("expected an exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to initialize a KeyManagerFactory"));
        }
    }

    public void testThatSSLv3IsNotEnabled() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build(), env);
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(Arrays.asList(engine.getEnabledProtocols()), not(hasItem("SSLv3")));
    }

    public void testThatSSLSessionCacheHasDefaultLimits() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build(), env);
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(1000));
        assertThat(context.getSessionTimeout(), equalTo((int) TimeValue.timeValueHours(24).seconds()));
    }

    public void testThatSettingSSLSessionCacheLimitsWorks() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.session.cache_size", "300")
                .put("xpack.security.ssl.session.cache_timeout", "600s")
                .build(), env);
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(300));
        assertThat(context.getSessionTimeout(), equalTo(600));
    }

    public void testThatCreateSSLEngineWithoutAnySettingsDoesNotWork() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.EMPTY, env);
        try {
            sslService.createSSLEngine();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no keystore configured"));
        }
    }

    public void testThatCreateSSLEngineWithOnlyTruststoreDoesNotWork() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.truststore.path", testnodeStore)
                .put("xpack.security.ssl.truststore.password", "testnode")
                .build(), env);
        try {
            sslService.createSSLEngine();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no keystore configured"));
        }
    }

    public void testThatTruststorePasswordIsRequired() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.truststore.path", testnodeStore)
                .build(), env);
        try {
            sslService.sslContext();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no truststore password configured"));
        }
    }

    public void testThatKeystorePasswordIsRequired() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .build(), env);
        try {
            sslService.sslContext();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no keystore password configured"));
        }
    }

    public void testCiphersAndInvalidCiphersWork() throws Exception {
        List<String> ciphers = new ArrayList<>(SSLSettings.Globals.DEFAULT_CIPHERS);
        ciphers.add("foo");
        ciphers.add("bar");
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .putArray("xpack.security.ssl.ciphers", ciphers.toArray(new String[ciphers.size()]))
                .build(), env);
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(engine, is(notNullValue()));
        String[] enabledCiphers = engine.getEnabledCipherSuites();
        assertThat(Arrays.asList(enabledCiphers), not(contains("foo", "bar")));
    }

    public void testInvalidCiphersOnlyThrowsException() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .putArray("xpack.security.ssl.ciphers", new String[] { "foo", "bar" })
                .build(), env);
        try {
            sslService.createSSLEngine();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("failed loading cipher suites [[foo, bar]]"));
        }
    }

    public void testThatSSLSocketFactoryHasProperCiphersAndProtocols() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build(), env);
        SSLSocketFactory factory = sslService.sslSocketFactory();
        final String[] ciphers = sslService.supportedCiphers(factory.getSupportedCipherSuites(), sslService.ciphers());
        assertThat(factory.getDefaultCipherSuites(), is(ciphers));

        try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
            assertThat(socket.getEnabledCipherSuites(), is(ciphers));
            assertThat(socket.getEnabledProtocols(), is(sslService.supportedProtocols()));
        }
    }
}
