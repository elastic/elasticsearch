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
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
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
            new ServerSSLService(settings, env, new Global(settings), null).createSSLEngine();
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
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);

        Settings.Builder settingsBuilder = Settings.builder()
                .put("truststore.path", testClientStore)
                .put("truststore.password", "testclient");

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(settingsBuilder.build());
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine();
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    public void testThatSslContextCachingWorks() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);

        SSLContext sslContext = sslService.sslContext();
        SSLContext cachedSslContext = sslService.sslContext();

        assertThat(sslContext, is(sameInstance(cachedSslContext)));
    }

    public void testThatKeyStoreAndKeyCanHaveDifferentPasswords() throws Exception {
        Path differentPasswordsStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks");
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", differentPasswordsStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.keystore.key_password", "testnode1")
                .build();
        new ServerSSLService(settings, env, new Global(settings), null).createSSLEngine();
    }

    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore = getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-different-passwords.jks");
        try {
            Settings settings = Settings.builder()
                    .put("xpack.security.ssl.keystore.path", differentPasswordsStore)
                    .put("xpack.security.ssl.keystore.password", "testnode")
                    .build();
            new ServerSSLService(settings, env, new Global(settings), null).createSSLEngine();
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
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
        SSLEngine engine = sslService.createSSLEngine();
        assertThat(Arrays.asList(engine.getEnabledProtocols()), not(hasItem("SSLv3")));
    }

    public void testThatSSLSessionCacheHasDefaultLimits() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
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
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
        SSLSessionContext context = sslService.sslContext().getServerSessionContext();
        assertThat(context.getSessionCacheSize(), equalTo(300));
        assertThat(context.getSessionTimeout(), equalTo(600));
    }

    public void testThatCreateSSLEngineWithoutAnySettingsDoesNotWork() throws Exception {
        ServerSSLService sslService = new ServerSSLService(Settings.EMPTY, env, new Global(Settings.builder()
                .put(Global.AUTO_GENERATE_SSL_SETTING.getKey(), false).build()), null);
        try {
            sslService.createSSLEngine();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("a key must be configured to act as a server"));
        }
    }

    public void testThatCreateSSLEngineWithOnlyTruststoreDoesNotWork() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.truststore.path", testnodeStore)
                .put("xpack.security.ssl.truststore.password", "testnode")
                .build();
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
        try {
            sslService.createSSLEngine();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("a key must be configured to act as a server"));
        }
    }

    public void testThatTruststorePasswordIsRequired() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .put("xpack.security.ssl.truststore.path", testnodeStore)
                .build();
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
        try {
            sslService.sslContext();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no truststore password configured"));
        }
    }

    public void testThatKeystorePasswordIsRequired() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .build();
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
        try {
            sslService.sslContext();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no keystore password configured"));
        }
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
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
        SSLEngine engine = sslService.createSSLEngine();
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
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
        try {
            sslService.createSSLEngine();
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("none of the ciphers [foo, bar] are supported by this JVM"));
        }
    }

    public void testThatSSLSocketFactoryHasProperCiphersAndProtocols() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", testnodeStore)
                .put("xpack.security.ssl.keystore.password", "testnode")
                .build();
        ServerSSLService sslService = new ServerSSLService(settings, env, new Global(settings), null);
        SSLSocketFactory factory = sslService.sslSocketFactory(Settings.EMPTY);
        final String[] ciphers = sslService.supportedCiphers(factory.getSupportedCipherSuites(), sslService.ciphers(), false);
        assertThat(factory.getDefaultCipherSuites(), is(ciphers));

        try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
            assertThat(socket.getEnabledCipherSuites(), is(ciphers));
            assertThat(socket.getEnabledProtocols(), is(sslService.supportedProtocols()));
        }
    }
}
