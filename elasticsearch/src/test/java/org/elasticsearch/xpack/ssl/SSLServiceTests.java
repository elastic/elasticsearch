/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.XPackSettings;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        Path testClientStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks");

        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testnodeStore)
                .put("xpack.ssl.truststore.password", "testnode")
                .put("transport.profiles.foo.xpack.security.ssl.truststore.path", testClientStore)
                .put("transport.profiles.foo.xpack.security.ssl.truststore.password", "testclient")
                .build();
        SSLService sslService = new SSLService(settings, env);

        Settings customTruststoreSettings = Settings.builder()
                .put("truststore.path", testClientStore)
                .put("truststore.password", "testclient")
                .build();

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(customTruststoreSettings, Settings.EMPTY);
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    public void testThatSslContextCachingWorks() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
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
                .put("xpack.ssl.keystore.path", differentPasswordsStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .put("xpack.ssl.keystore.key_password", "testnode1")
                .build();
        new SSLService(settings, env).createSSLEngine(Settings.EMPTY, Settings.EMPTY);
    }

    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore =
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-different-passwords.jks");
        try {
            Settings settings = Settings.builder()
                    .put("xpack.ssl.keystore.path", differentPasswordsStore)
                    .put("xpack.ssl.keystore.password", "testnode")
                    .build();
            new SSLService(settings, env).createSSLEngine(Settings.EMPTY, Settings.EMPTY);
            fail("expected an exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to initialize a KeyManagerFactory"));
        }
    }

    public void testThatSSLv3IsNotEnabled() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine engine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(Arrays.asList(engine.getEnabledProtocols()), not(hasItem("SSLv3")));
    }

    public void testThatCreateClientSSLEngineWithoutAnySettingsWorks() throws Exception {
        SSLService sslService = new SSLService(Settings.EMPTY, env);
        SSLEngine sslEngine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(sslEngine, notNullValue());
    }

    public void testThatCreateSSLEngineWithOnlyTruststoreWorks() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testclientStore)
                .put("xpack.ssl.truststore.password", "testclient")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine sslEngine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(sslEngine, notNullValue());
    }

    public void testCreateWithoutAnySettingsNotValidForServer() throws Exception {
        SSLService sslService = new SSLService(Settings.EMPTY, env);
        assertFalse(sslService.isConfigurationValidForServerUsage(Settings.EMPTY, Settings.EMPTY));
    }

    public void testCreateWithOnlyTruststoreNotValidForServer() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testnodeStore)
                .put("xpack.ssl.truststore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        assertFalse(sslService.isConfigurationValidForServerUsage(Settings.EMPTY, Settings.EMPTY));
    }

    public void testCreateWithKeystoreIsValidForServer() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        assertTrue(sslService.isConfigurationValidForServerUsage(Settings.EMPTY, Settings.EMPTY));
    }

    public void testValidForServerWithFallback() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testnodeStore)
                .put("xpack.ssl.truststore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        assertFalse(sslService.isConfigurationValidForServerUsage(Settings.EMPTY, settings.getByPrefix("xpack.ssl.")));

        settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testnodeStore)
                .put("xpack.ssl.truststore.password", "testnode")
                .put("xpack.security.transport.ssl.keystore.path", testnodeStore)
                .put("xpack.security.transport.ssl.keystore.password", "testnode")
                .build();
        sslService = new SSLService(settings, env);
        assertFalse(sslService.isConfigurationValidForServerUsage(Settings.EMPTY, settings.getByPrefix("xpack.ssl.")));
        assertTrue(sslService.isConfigurationValidForServerUsage(
                settings.getByPrefix("xpack.security.transport.ssl."), settings.getByPrefix("xpack.ssl.")));
        assertTrue(sslService.isConfigurationValidForServerUsage(Settings.EMPTY, settings.getByPrefix("xpack.security.transport.ssl.")));
    }

    public void testGetVerificationMode() {
        SSLService sslService = new SSLService(Settings.EMPTY, env);
        assertThat(sslService.getVerificationMode(Settings.EMPTY, Settings.EMPTY), is(XPackSettings.VERIFICATION_MODE_DEFAULT));

        Settings settings = Settings.builder()
                .put("xpack.ssl.verification_mode", "none")
                .put("xpack.security.transport.ssl.verification_mode", "certificate")
                .put("transport.profiles.foo.xpack.security.ssl.verification_mode", "full")
                .build();
        sslService = new SSLService(settings, env);
        assertThat(sslService.getVerificationMode(Settings.EMPTY, Settings.EMPTY), is(VerificationMode.NONE));
        assertThat(sslService.getVerificationMode(settings.getByPrefix("xpack.security.transport.ssl."), Settings.EMPTY),
                is(VerificationMode.CERTIFICATE));
        assertThat(sslService.getVerificationMode(settings.getByPrefix("transport.profiles.foo.xpack.security.ssl."),
                settings.getByPrefix("xpack.security.transport.ssl.")), is(VerificationMode.FULL));
        assertThat(sslService.getVerificationMode(Settings.EMPTY, settings.getByPrefix("xpack.security.transport.ssl.")),
                is(VerificationMode.CERTIFICATE));
    }

    public void testIsSSLClientAuthEnabled() {
        SSLService sslService = new SSLService(Settings.EMPTY, env);
        assertTrue(sslService.isSSLClientAuthEnabled(Settings.EMPTY));
        assertTrue(sslService.isSSLClientAuthEnabled(Settings.EMPTY, Settings.EMPTY));

        Settings settings = Settings.builder()
                .put("xpack.ssl.client_authentication", "none")
                .put("xpack.security.transport.ssl.client_authentication", "optional")
                .build();
        sslService = new SSLService(settings, env);
        assertFalse(sslService.isSSLClientAuthEnabled(Settings.EMPTY));
        assertFalse(sslService.isSSLClientAuthEnabled(Settings.EMPTY, Settings.EMPTY));
        assertTrue(sslService.isSSLClientAuthEnabled(settings.getByPrefix("xpack.security.transport.ssl.")));
        assertTrue(sslService.isSSLClientAuthEnabled(settings.getByPrefix("xpack.security.transport.ssl."), Settings.EMPTY));
        assertTrue(sslService.isSSLClientAuthEnabled(settings.getByPrefix("transport.profiles.foo.xpack.security.ssl."),
                settings.getByPrefix("xpack.security.transport.ssl.")));
    }

    public void testThatHttpClientAuthDefaultsToNone() {
        final Settings globalSettings = Settings.builder()
                .put("xpack.security.http.ssl.enabled", true)
                .put("xpack.ssl.client_authentication", SSLClientAuth.OPTIONAL.name())
                .build();
        final SSLService sslService = new SSLService(globalSettings, env);

        final SSLConfiguration globalConfig = sslService.sslConfiguration(Settings.EMPTY);
        assertThat(globalConfig.sslClientAuth(), is(SSLClientAuth.OPTIONAL));

        final Settings httpSettings = SSLService.getHttpTransportSSLSettings(globalSettings);
        final SSLConfiguration httpConfig = sslService.sslConfiguration(httpSettings);
        assertThat(httpConfig.sslClientAuth(), is(SSLClientAuth.NONE));
    }

    public void testThatTruststorePasswordIsRequired() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .put("xpack.ssl.truststore.path", testnodeStore)
                .build();
        NullPointerException e =
                expectThrows(NullPointerException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("truststore password must be specified"));
    }

    public void testThatKeystorePasswordIsRequired() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .build();
        NullPointerException e =
                expectThrows(NullPointerException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("keystore password must be specified"));
    }

    public void testCiphersAndInvalidCiphersWork() throws Exception {
        List<String> ciphers = new ArrayList<>(XPackSettings.DEFAULT_CIPHERS);
        ciphers.add("foo");
        ciphers.add("bar");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .putArray("xpack.ssl.ciphers", ciphers.toArray(new String[ciphers.size()]))
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine engine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(engine, is(notNullValue()));
        String[] enabledCiphers = engine.getEnabledCipherSuites();
        assertThat(Arrays.asList(enabledCiphers), not(contains("foo", "bar")));
    }

    public void testInvalidCiphersOnlyThrowsException() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .putArray("xpack.ssl.cipher_suites", new String[]{"foo", "bar"})
                .build();
        IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("none of the ciphers [foo, bar] are supported by this JVM"));
    }

    public void testThatSSLEngineHasCipherSuitesOrderSet() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine engine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(engine, is(notNullValue()));
        assertTrue(engine.getSSLParameters().getUseCipherSuitesOrder());
    }

    public void testThatSSLSocketFactoryHasProperCiphersAndProtocols() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLSocketFactory factory = sslService.sslSocketFactory(Settings.EMPTY);
        SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        final String[] ciphers = sslService.supportedCiphers(factory.getSupportedCipherSuites(), config.cipherSuites(), false);
        assertThat(factory.getDefaultCipherSuites(), is(ciphers));

        final String[] supportedProtocols = config.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
            assertThat(socket.getEnabledCipherSuites(), is(ciphers));
            // the order we set the protocols in is not going to be what is returned as internally the JDK may sort the versions
            assertThat(socket.getEnabledProtocols(), arrayContainingInAnyOrder(supportedProtocols));
            assertArrayEquals(ciphers, socket.getSSLParameters().getCipherSuites());
            assertThat(socket.getSSLParameters().getProtocols(), arrayContainingInAnyOrder(supportedProtocols));
            assertTrue(socket.getSSLParameters().getUseCipherSuitesOrder());
        }
    }

    public void testThatSSLEngineHasProperCiphersAndProtocols() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.password", "testnode")
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine engine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        SSLConfiguration config = sslService.sslConfiguration(Settings.EMPTY);
        final String[] ciphers = sslService.supportedCiphers(engine.getSupportedCipherSuites(), config.cipherSuites(), false);
        final String[] supportedProtocols = config.supportedProtocols().toArray(Strings.EMPTY_ARRAY);
        assertThat(engine.getEnabledCipherSuites(), is(ciphers));
        assertArrayEquals(ciphers, engine.getSSLParameters().getCipherSuites());
        // the order we set the protocols in is not going to be what is returned as internally the JDK may sort the versions
        assertThat(engine.getEnabledProtocols(), arrayContainingInAnyOrder(supportedProtocols));
        assertThat(engine.getSSLParameters().getProtocols(), arrayContainingInAnyOrder(supportedProtocols));
    }

    public void testSSLStrategy() {
        // this just exhaustively verifies that the right things are called and that it uses the right parameters
        Settings settings = Settings.builder().build();
        SSLService sslService = mock(SSLService.class);
        SSLConfiguration sslConfig = mock(SSLConfiguration.class);
        SSLParameters sslParameters = mock(SSLParameters.class);
        SSLContext sslContext = mock(SSLContext.class);
        String[] protocols = new String[] { "protocols" };
        String[] ciphers = new String[] { "ciphers!!!" };
        String[] supportedCiphers = new String[] { "supported ciphers" };
        List<String> requestedCiphers = new ArrayList<>(0);
        VerificationMode mode = randomFrom(VerificationMode.values());
        ArgumentCaptor<HostnameVerifier> verifier = ArgumentCaptor.forClass(HostnameVerifier.class);
        SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);

        when(sslService.sslConfiguration(settings)).thenReturn(sslConfig);
        when(sslService.sslContext(sslConfig)).thenReturn(sslContext);
        when(sslService.supportedCiphers(supportedCiphers, requestedCiphers, false)).thenReturn(ciphers);
        when(sslService.sslParameters(sslContext)).thenReturn(sslParameters);
        when(sslParameters.getCipherSuites()).thenReturn(supportedCiphers);
        when(sslConfig.supportedProtocols()).thenReturn(Arrays.asList(protocols));
        when(sslConfig.cipherSuites()).thenReturn(requestedCiphers);
        when(sslConfig.verificationMode()).thenReturn(mode);
        when(sslService.sslIOSessionStrategy(eq(sslContext), eq(protocols), eq(ciphers), verifier.capture())).thenReturn(sslStrategy);

        // ensure it actually goes through and calls the real method
        when(sslService.sslIOSessionStrategy(settings)).thenCallRealMethod();

        assertThat(sslService.sslIOSessionStrategy(settings), sameInstance(sslStrategy));

        if (mode.isHostnameVerificationEnabled()) {
            assertThat(verifier.getValue(), instanceOf(DefaultHostnameVerifier.class));
        } else {
            assertThat(verifier.getValue(), sameInstance(NoopHostnameVerifier.INSTANCE));
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
            privilegedConnect(() -> client.execute(new HttpGet("https://www.elastic.co/")).close());
        }
    }

    @Network
    public void testThatSSLContextTrustsJDKTrustedCAs() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testclientStore)
                .put("xpack.ssl.keystore.password", "testclient")
                .build();
        SSLContext sslContext = new SSLService(settings, env).sslContext();
        try (CloseableHttpClient client = HttpClients.custom().setSSLContext(sslContext).build()) {
            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA which will succeed because the JDK
            // certs are trusted by default
            privilegedConnect(() -> client.execute(new HttpGet("https://www.elastic.co/")).close());
        }
    }

    @Network
    public void testThatSSLIOSessionStrategyWithoutSettingsWorks() throws Exception {
        SSLService sslService = new SSLService(Settings.EMPTY, env);
        SSLIOSessionStrategy sslStrategy = sslService.sslIOSessionStrategy(Settings.EMPTY);
        try (CloseableHttpAsyncClient client = getAsyncHttpClient(sslStrategy)) {
            client.start();

            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA
            // This will result in a SSLHandshakeException if the SSLContext does not trust the CA, but the default
            // truststore trusts all common public CAs so the handshake will succeed
            client.execute(new HttpHost("elastic.co", 443, "https"), new HttpGet("/"), new AssertionCallback()).get();
        }
    }

    @Network
    public void testThatSSLIOSessionStrategytTrustsJDKTrustedCAs() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testclientStore)
                .put("xpack.ssl.keystore.password", "testclient")
                .build();
        SSLIOSessionStrategy sslStrategy = new SSLService(settings, env).sslIOSessionStrategy(Settings.EMPTY);
        try (CloseableHttpAsyncClient client = getAsyncHttpClient(sslStrategy)) {
            client.start();

            // Execute a GET on a site known to have a valid certificate signed by a trusted public CA which will succeed because the JDK
            // certs are trusted by default
            client.execute(new HttpHost("elastic.co", 443, "https"), new HttpGet("/"), new AssertionCallback()).get();
        }
    }

    class AssertionCallback implements FutureCallback<HttpResponse> {

        @Override
        public void completed(HttpResponse result) {
            assertThat(result.getStatusLine().getStatusCode(), lessThan(300));
        }

        @Override
        public void failed(Exception ex) {
            logger.error(ex);

            fail(ex.toString());
        }

        @Override
        public void cancelled() {
            fail("The request was cancelled for some reason");
        }
    }

    private CloseableHttpAsyncClient getAsyncHttpClient(SSLIOSessionStrategy sslStrategy) throws Exception {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<CloseableHttpAsyncClient>)
                    () -> HttpAsyncClientBuilder.create().setSSLStrategy(sslStrategy).build());
        } catch (PrivilegedActionException e) {
            throw (Exception) e.getCause();
        }
    }

    private static void privilegedConnect(CheckedRunnable<Exception> runnable) throws Exception {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                runnable.run();
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw (Exception) e.getCause();
        }
    }

}
