/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

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
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;
import org.joda.time.DateTime;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import java.net.Socket;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
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
    private String testnodeStoreType;
    private Path testclientStore;
    private Environment env;

    @Before
    public void setup() throws Exception {
        // Randomise the keystore type (jks/PKCS#12)
        if (randomBoolean()) {
            testnodeStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
            // The default is to use JKS. Randomly test with explicit and with the default value.
            testnodeStoreType = randomBoolean() ? "jks" : null;
        } else {
            testnodeStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.p12");
            testnodeStoreType = randomBoolean() ? "PKCS12" : null;
        }
        logger.info("Using [{}] key/truststore [{}]", testnodeStoreType, testnodeStore);
        testclientStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks");
        env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
    }

    public void testThatCustomTruststoreCanBeSpecified() throws Exception {
        Path testClientStore = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.jks");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
        secureSettings.setString("transport.profiles.foo.xpack.security.ssl.truststore.secure_password", "testclient");
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testnodeStore)
                .put("xpack.ssl.truststore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .put("transport.profiles.foo.xpack.security.ssl.truststore.path", testClientStore)
                .build();
        SSLService sslService = new SSLService(settings, env);

        MockSecureSettings secureCustomSettings = new MockSecureSettings();
        secureCustomSettings.setString("truststore.secure_password", "testclient");
        Settings customTruststoreSettings = Settings.builder()
                .put("truststore.path", testClientStore)
                .setSecureSettings(secureCustomSettings)
                .build();

        SSLEngine sslEngineWithTruststore = sslService.createSSLEngine(customTruststoreSettings, Settings.EMPTY);
        assertThat(sslEngineWithTruststore, is(not(nullValue())));

        SSLEngine sslEngine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(sslEngineWithTruststore, is(not(sameInstance(sslEngine))));
    }

    public void testThatSslContextCachingWorks() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .build();
        SSLService sslService = new SSLService(settings, env);

        SSLContext sslContext = sslService.sslContext();
        SSLContext cachedSslContext = sslService.sslContext();

        assertThat(sslContext, is(sameInstance(cachedSslContext)));
    }

    public void testThatKeyStoreAndKeyCanHaveDifferentPasswords() throws Exception {
        Path differentPasswordsStore =
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-different-passwords.jks");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        secureSettings.setString("xpack.ssl.keystore.secure_key_password", "testnode1");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", differentPasswordsStore)
                .setSecureSettings(secureSettings)
                .build();
        new SSLService(settings, env).createSSLEngine(Settings.EMPTY, Settings.EMPTY);
    }

    public void testIncorrectKeyPasswordThrowsException() throws Exception {
        Path differentPasswordsStore =
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-different-passwords.jks");
        try {
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
            Settings settings = Settings.builder()
                    .put("xpack.ssl.keystore.path", differentPasswordsStore)
                    .setSecureSettings(secureSettings)
                    .build();
            new SSLService(settings, env).createSSLEngine(Settings.EMPTY, Settings.EMPTY);
            fail("expected an exception");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to initialize a KeyManagerFactory"));
        }
    }

    public void testThatSSLv3IsNotEnabled() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
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
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testclient");
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testclientStore)
                .setSecureSettings(secureSettings)
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine sslEngine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(sslEngine, notNullValue());
    }


    public void testCreateWithKeystoreIsValidForServer() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .build();
        SSLService sslService = new SSLService(settings, env);

        assertTrue(sslService.isConfigurationValidForServerUsage(sslService.sslConfiguration(Settings.EMPTY)));
    }

    public void testValidForServerWithFallback() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testnodeStore)
                .put("xpack.ssl.truststore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .build();
        SSLService sslService = new SSLService(settings, env);
        assertFalse(sslService.isConfigurationValidForServerUsage(sslService.sslConfiguration(Settings.EMPTY)));

        secureSettings.setString("xpack.security.transport.ssl.keystore.secure_password", "testnode");
        settings = Settings.builder()
                .put("xpack.ssl.truststore.path", testnodeStore)
                .put("xpack.ssl.truststore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .put("xpack.security.transport.ssl.keystore.path", testnodeStore)
                .put("xpack.security.transport.ssl.keystore.type", testnodeStoreType)
                .build();
        sslService = new SSLService(settings, env);
        assertFalse(sslService.isConfigurationValidForServerUsage(sslService.sslConfiguration(Settings.EMPTY)));
        assertTrue(sslService.isConfigurationValidForServerUsage(sslService.sslConfiguration(
                settings.getByPrefix("xpack.security.transport.ssl."))));
        assertFalse(sslService.isConfigurationValidForServerUsage(sslService.sslConfiguration(Settings.EMPTY)));
    }

    public void testGetVerificationMode() throws Exception {
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

    public void testIsSSLClientAuthEnabled() throws Exception {
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

    public void testThatHttpClientAuthDefaultsToNone() throws Exception {
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
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .put("xpack.ssl.truststore.path", testnodeStore)
                .put("xpack.ssl.truststore.type", testnodeStoreType)
                .build();
        ElasticsearchException e =
                expectThrows(ElasticsearchException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("failed to initialize a TrustManagerFactory"));
    }

    public void testThatKeystorePasswordIsRequired() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .build();
        ElasticsearchException e =
                expectThrows(ElasticsearchException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("failed to create trust manager"));
    }

    public void testCiphersAndInvalidCiphersWork() throws Exception {
        List<String> ciphers = new ArrayList<>(XPackSettings.DEFAULT_CIPHERS);
        ciphers.add("foo");
        ciphers.add("bar");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .putList("xpack.ssl.ciphers", ciphers.toArray(new String[ciphers.size()]))
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine engine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(engine, is(notNullValue()));
        String[] enabledCiphers = engine.getEnabledCipherSuites();
        assertThat(Arrays.asList(enabledCiphers), not(contains("foo", "bar")));
    }

    public void testInvalidCiphersOnlyThrowsException() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");

        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .putList("xpack.ssl.cipher_suites", new String[] { "foo", "bar" })
                .build();
        IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> new SSLService(settings, env));
        assertThat(e.getMessage(), is("none of the ciphers [foo, bar] are supported by this JVM"));
    }

    public void testThatSSLEngineHasCipherSuitesOrderSet() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
                .build();
        SSLService sslService = new SSLService(settings, env);
        SSLEngine engine = sslService.createSSLEngine(Settings.EMPTY, Settings.EMPTY);
        assertThat(engine, is(notNullValue()));
        assertTrue(engine.getSSLParameters().getUseCipherSuitesOrder());
    }

    public void testThatSSLSocketFactoryHasProperCiphersAndProtocols() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
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
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testnodeStore)
                .put("xpack.ssl.keystore.type", testnodeStoreType)
                .setSecureSettings(secureSettings)
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
        VerificationMode mode = randomFrom(VerificationMode.values());
        Settings settings = Settings.builder()
                .put("supported_protocols", "protocols")
                .put("cipher_suites", "")
                .put("verification_mode", mode.name())
                .build();
        SSLService sslService = mock(SSLService.class);
        SSLConfiguration sslConfig = new SSLConfiguration(settings);
        SSLParameters sslParameters = mock(SSLParameters.class);
        SSLContext sslContext = mock(SSLContext.class);
        String[] protocols = new String[] { "protocols" };
        String[] ciphers = new String[] { "ciphers!!!" };
        String[] supportedCiphers = new String[] { "supported ciphers" };
        List<String> requestedCiphers = new ArrayList<>(0);
        ArgumentCaptor<HostnameVerifier> verifier = ArgumentCaptor.forClass(HostnameVerifier.class);
        SSLIOSessionStrategy sslStrategy = mock(SSLIOSessionStrategy.class);

        when(sslService.sslConfiguration(settings)).thenReturn(sslConfig);
        when(sslService.sslContext(sslConfig)).thenReturn(sslContext);
        when(sslService.supportedCiphers(supportedCiphers, requestedCiphers, false)).thenReturn(ciphers);
        when(sslService.sslParameters(sslContext)).thenReturn(sslParameters);
        when(sslParameters.getCipherSuites()).thenReturn(supportedCiphers);
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

    public void testEmptyTrustManager() throws Exception {
        Settings settings = Settings.builder().build();
        final SSLService sslService = new SSLService(settings, env);
        SSLConfiguration sslConfig = new SSLConfiguration(settings);
        X509ExtendedTrustManager trustManager = sslService.sslContextHolder(sslConfig).getEmptyTrustManager();
        assertThat(trustManager.getAcceptedIssuers(), emptyArray());
    }

    public void testReadCertificateInformation() throws Exception {
        final Path jksPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        final Path p12Path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.p12");
        final Path pemPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/active-directory-ca.crt");

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        secureSettings.setString("xpack.ssl.truststore.secure_password", "testnode");
        secureSettings.setString("xpack.http.ssl.keystore.secure_password", "testnode");

        final Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", jksPath)
                .put("xpack.ssl.truststore.path", jksPath)
                .put("xpack.http.ssl.keystore.path", p12Path)
                .put("xpack.security.authc.realms.ad.type", "ad")
                .put("xpack.security.authc.realms.ad.ssl.certificate_authorities", pemPath)
                .setSecureSettings(secureSettings)
                .build();

        final SSLService sslService = new SSLService(settings, env);
        final List<CertificateInfo> certificates = new ArrayList<>(sslService.getLoadedCertificates());
        assertThat(certificates, iterableWithSize(8));
        Collections.sort(certificates,
                Comparator.comparing((CertificateInfo c) -> c.alias() == null ? "" : c.alias()).thenComparing(CertificateInfo::path));

        final Iterator<CertificateInfo> iterator = certificates.iterator();
        CertificateInfo cert = iterator.next();
        assertThat(cert.alias(), nullValue());
        assertThat(cert.path(), equalTo(pemPath.toString()));
        assertThat(cert.format(), equalTo("PEM"));
        assertThat(cert.serialNumber(), equalTo("580db8ad52bb168a4080e1df122a3f56"));
        assertThat(cert.subjectDn(), equalTo("CN=ad-ELASTICSEARCHAD-CA, DC=ad, DC=test, DC=elasticsearch, DC=com"));
        assertThat(cert.expiry(), equalTo(DateTime.parse("2029-08-27T16:32:42Z")));
        assertThat(cert.hasPrivateKey(), equalTo(false));

        cert = iterator.next();
        assertThat(cert.alias(), equalTo("activedir"));
        assertThat(cert.path(), equalTo(jksPath.toString()));
        assertThat(cert.format(), equalTo("jks"));
        assertThat(cert.serialNumber(), equalTo("580db8ad52bb168a4080e1df122a3f56"));
        assertThat(cert.subjectDn(), equalTo("CN=ad-ELASTICSEARCHAD-CA, DC=ad, DC=test, DC=elasticsearch, DC=com"));
        assertThat(cert.expiry(), equalTo(DateTime.parse("2029-08-27T16:32:42Z")));
        assertThat(cert.hasPrivateKey(), equalTo(false));

        cert = iterator.next();
        assertThat(cert.alias(), equalTo("mykey"));
        assertThat(cert.path(), equalTo(jksPath.toString()));
        assertThat(cert.format(), equalTo("jks"));
        assertThat(cert.serialNumber(), equalTo("3151a81eec8d4e34c56a8466a8510bcfbe63cc31"));
        assertThat(cert.subjectDn(), equalTo("CN=samba4"));
        assertThat(cert.expiry(), equalTo(DateTime.parse("2021-02-14T17:49:11.000Z")));
        assertThat(cert.hasPrivateKey(), equalTo(false));

        cert = iterator.next();
        assertThat(cert.alias(), equalTo("openldap"));
        assertThat(cert.path(), equalTo(jksPath.toString()));
        assertThat(cert.format(), equalTo("jks"));
        assertThat(cert.serialNumber(), equalTo("d3850b2b1995ad5f"));
        assertThat(cert.subjectDn(), equalTo("CN=OpenLDAP, OU=Elasticsearch, O=Elastic, L=Mountain View, ST=CA, C=US"));
        assertThat(cert.expiry(), equalTo(DateTime.parse("2027-07-23T16:41:14Z")));
        assertThat(cert.hasPrivateKey(), equalTo(false));

        cert = iterator.next();
        assertThat(cert.alias(), equalTo("testclient"));
        assertThat(cert.path(), equalTo(jksPath.toString()));
        assertThat(cert.format(), equalTo("jks"));
        assertThat(cert.serialNumber(), equalTo("b9d497f2924bbe29"));
        assertThat(cert.subjectDn(), equalTo("CN=Elasticsearch Test Client, OU=elasticsearch, O=org"));
        assertThat(cert.expiry(), equalTo(DateTime.parse("2019-09-22T18:52:55Z")));
        assertThat(cert.hasPrivateKey(), equalTo(false));

        cert = iterator.next();
        assertThat(cert.alias(), equalTo("testnode"));
        assertThat(cert.path(), equalTo(jksPath.toString()));
        assertThat(cert.format(), equalTo("jks"));
        assertThat(cert.serialNumber(), equalTo("b8b96c37e332cccb"));
        assertThat(cert.subjectDn(), equalTo("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
        assertThat(cert.expiry(), equalTo(DateTime.parse("2019-09-22T18:52:57Z")));
        assertThat(cert.hasPrivateKey(), equalTo(true));

        cert = iterator.next();
        assertThat(cert.alias(), equalTo("testnode"));
        assertThat(cert.path(), equalTo(p12Path.toString()));
        assertThat(cert.format(), equalTo("PKCS12"));
        assertThat(cert.serialNumber(), equalTo("b8b96c37e332cccb"));
        assertThat(cert.subjectDn(), equalTo("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
        assertThat(cert.expiry(), equalTo(DateTime.parse("2019-09-22T18:52:57Z")));
        assertThat(cert.hasPrivateKey(), equalTo(true));

        cert = iterator.next();
        assertThat(cert.alias(), equalTo("testnode-client-profile"));
        assertThat(cert.path(), equalTo(jksPath.toString()));
        assertThat(cert.format(), equalTo("jks"));
        assertThat(cert.serialNumber(), equalTo("c0ea4216e8ff0fd8"));
        assertThat(cert.subjectDn(), equalTo("CN=testnode-client-profile"));
        assertThat(cert.expiry(), equalTo(DateTime.parse("2019-09-22T18:52:56Z")));
        assertThat(cert.hasPrivateKey(), equalTo(false));

        assertFalse(iterator.hasNext());
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
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testclient");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testclientStore)
                .setSecureSettings(secureSettings)
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
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.keystore.secure_password", "testclient");
        Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path", testclientStore)
                .setSecureSettings(secureSettings)
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
