/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.metadata.resolver.impl.AbstractReloadingMetadataResolver;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.NameIDType;
import org.opensaml.saml.saml2.metadata.EntityDescriptor;
import org.opensaml.saml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.saml.saml2.metadata.SingleLogoutService;
import org.opensaml.saml.saml2.metadata.SingleSignOnService;
import org.opensaml.security.credential.Credential;
import org.opensaml.security.x509.X509Credential;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PrivilegedActionException;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

/**
 * Basic unit tests for the SAMLRealm
 */
public class SamlRealmTests extends SamlTestCase {

    public static final String TEST_IDP_ENTITY_ID = "http://demo_josso_1.josso.dev.docker:8081/IDBUS/JOSSO-TUTORIAL/IDP1/SAML2/MD";
    private static final int METADATA_REFRESH = 3000;

    private static final String REALM_NAME = "my-saml";
    private static final String REALM_SETTINGS_PREFIX = "xpack.security.authc.realms." + REALM_NAME;

    @Before
    public void initRealm() throws PrivilegedActionException {
        SamlUtils.initialize(logger);
    }

    public void testReadIdpMetadataFromFile() throws Exception {
        final Path path = getDataPath("idp1.xml");
        Tuple<RealmConfig, SSLService> config = buildConfig(path.toString());
        final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
        Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> tuple
                = SamlRealm.initializeResolver(logger, config.v1(), config.v2(), watcherService);
        try {
            assertIdp1MetadataParsedCorrectly(tuple.v2().get());
        } finally {
            tuple.v1().destroy();
        }
    }

    public void testReadIdpMetadataFromHttps() throws Exception {
        final Path path = getDataPath("idp1.xml");
        final String body = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        final MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("xpack.ssl.keystore.secure_password", "testnode");
        final Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .put("path.home", createTempDir())
                .setSecureSettings(mockSecureSettings)
                .build();
        TestsSSLService sslService = new TestsSSLService(settings, TestEnvironment.newEnvironment(settings));
        try (MockWebServer proxyServer = new MockWebServer(sslService.sslContext(Settings.EMPTY), false)) {
            proxyServer.start();
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));
            assertEquals(0, proxyServer.requests().size());

            Tuple<RealmConfig, SSLService> config = buildConfig("https://localhost:" + proxyServer.getPort());
            final ResourceWatcherService watcherService = mock(ResourceWatcherService.class);
            Tuple<AbstractReloadingMetadataResolver, Supplier<EntityDescriptor>> tuple
                    = SamlRealm.initializeResolver(logger, config.v1(), config.v2(), watcherService);

            try {
                final int firstRequestCount = proxyServer.requests().size();
                assertThat(firstRequestCount, greaterThanOrEqualTo(1));
                assertIdp1MetadataParsedCorrectly(tuple.v2().get());
                assertBusy(() -> assertThat(proxyServer.requests().size(), greaterThan(firstRequestCount)));
            } finally {
                tuple.v1().destroy();
            }
        }
    }

    public void testAuthenticateWithRoleMapping() throws Exception {
        final boolean useNameId = randomBoolean();
        final boolean principalIsEmailAddress = randomBoolean();
        final Boolean populateUserMetadata = randomFrom(Boolean.TRUE, Boolean.FALSE, null);
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);

        final Settings.Builder settingsBuilder = Settings.builder()
                .put(SamlRealmSettings.PRINCIPAL_ATTRIBUTE.name(), useNameId ? "nameid" : "uid")
                .put(SamlRealmSettings.GROUPS_ATTRIBUTE.name(), "groups")
                .put(SamlRealmSettings.MAIL_ATTRIBUTE.name(), "mail");
        if (principalIsEmailAddress) {
            final boolean anchoredMatch = randomBoolean();
            settingsBuilder.put(SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getPattern().getKey(),
                    anchoredMatch ? "^([^@]+)@shield.gov$" : "^([^@]+)@");
        }
        if (populateUserMetadata != null) {
            settingsBuilder.put(SamlRealmSettings.POPULATE_USER_METADATA.getKey(), populateUserMetadata.booleanValue());
        }
        final Settings realmSettings = settingsBuilder.build();

        final RealmConfig config = realmConfigFromRealmSettings(realmSettings);

        final SamlRealm realm = new SamlRealm(config, roleMapper, authenticator, logoutHandler, () -> idp, sp);
        final SamlToken token = new SamlToken(new byte[0], Collections.singletonList("<id>"));

        final String nameIdValue = principalIsEmailAddress ? "clint.barton@shield.gov" : "clint.barton";
        final String uidValue = principalIsEmailAddress ? "cbarton@shield.gov" : "cbarton";
        final SamlAttributes attributes = new SamlAttributes(
                new SamlNameId(NameIDType.PERSISTENT, nameIdValue, idp.getEntityID(), sp.getEntityId(), null),
                randomAlphaOfLength(16),
                Arrays.asList(
                        new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.1", "uid", Collections.singletonList(uidValue)),
                        new SamlAttributes.SamlAttribute("urn:oid:1.3.6.1.4.1.5923.1.5.1.1", "groups", Arrays.asList("avengers", "shield")),
                        new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.3", "mail", Arrays.asList("cbarton@shield.gov"))
                ));
        Mockito.when(authenticator.authenticate(token)).thenReturn(attributes);

        AtomicReference<UserRoleMapper.UserData> userData = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            assert invocation.getArguments().length == 2;
            userData.set((UserRoleMapper.UserData) invocation.getArguments()[0]);
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.singleton("superuser"));
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        final AuthenticationResult result = future.get();
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser().principal(), equalTo(useNameId ? "clint.barton" : "cbarton"));
        assertThat(result.getUser().email(), equalTo("cbarton@shield.gov"));
        assertThat(result.getUser().roles(), arrayContainingInAnyOrder("superuser"));
        if (populateUserMetadata == Boolean.FALSE) {
            // TODO : "saml_nameid" should be null too, but the logout code requires it for now.
            assertThat(result.getUser().metadata().get("saml_uid"), nullValue());
        } else {
            assertThat(result.getUser().metadata().get("saml_nameid"), equalTo(nameIdValue));
            assertThat(result.getUser().metadata().get("saml_uid"), instanceOf(Iterable.class));
            assertThat((Iterable<?>) result.getUser().metadata().get("saml_uid"), contains(uidValue));
        }

        assertThat(userData.get().getUsername(), equalTo(useNameId ? "clint.barton" : "cbarton"));
        assertThat(userData.get().getGroups(), containsInAnyOrder("avengers", "shield"));
    }

    public void testAttributeSelectionWithRegex() throws Exception {
        final boolean useFriendlyName = randomBoolean();
        final Settings settings = Settings.builder()
                .put("attributes.principal", useFriendlyName ? "mail" : "urn:oid:0.9.2342.19200300.100.1.3")
                .put("attribute_patterns.principal", "^(.+)@\\w+.example.com$")
                .build();

        final RealmConfig config = realmConfigFromRealmSettings(settings);

        final SamlRealmSettings.AttributeSetting principalSetting = new SamlRealmSettings.AttributeSetting("principal");
        final SamlRealm.AttributeParser parser = SamlRealm.AttributeParser.forSetting(logger, principalSetting, config, false);

        final SamlAttributes attributes = new SamlAttributes(
                new SamlNameId(NameIDType.TRANSIENT, randomAlphaOfLength(24), null, null, null),
                randomAlphaOfLength(16),
                Collections.singletonList(new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.3", "mail",
                        Arrays.asList("john.smith@personal.example.net", "john.smith@corporate.example.com", "jsmith@corporate.example.com")
                )));

        final List<String> strings = parser.getAttribute(attributes);
        assertThat(strings, contains("john.smith", "jsmith"));
    }

    public void testSettingPatternWithoutAttributeThrowsSettingsException() throws Exception {
        final Settings realmSettings = Settings.builder()
                .put(SamlRealmSettings.PRINCIPAL_ATTRIBUTE.name(), "nameid")
                .put(SamlRealmSettings.NAME_ATTRIBUTE.getPattern().getKey(), "^\\s*(\\S.*\\S)\\s*$")
                .build();
        final RealmConfig config = realmConfigFromRealmSettings(realmSettings);

        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null);

        final SettingsException settingsException = expectThrows(SettingsException.class,
                () -> new SamlRealm(config, roleMapper, authenticator, logoutHandler, () -> idp, sp));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attribute_patterns.name"));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attributes.name"));
    }

    public void testMissingPrincipalSettingThrowsSettingsException() throws Exception {
        final Settings realmSettings = Settings.EMPTY;
        final RealmConfig config = realmConfigFromRealmSettings(realmSettings);

        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null);

        final SettingsException settingsException = expectThrows(SettingsException.class,
                () -> new SamlRealm(config, roleMapper, authenticator, logoutHandler, () -> idp, sp));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attributes.principal"));
    }

    public void testNonMatchingPrincipalPatternThrowsSamlException() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);

        final Settings realmSettings = Settings.builder()
                .put(SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getAttribute().getKey(), "mail")
                .put(SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getPattern().getKey(), "^([^@]+)@mycorp\\.example\\.com$")
                .build();

        final RealmConfig config = realmConfigFromRealmSettings(realmSettings);

        final SamlRealm realm = new SamlRealm(config, roleMapper, authenticator, logoutHandler, () -> idp, sp);
        final SamlToken token = new SamlToken(new byte[0], Collections.singletonList("<id>"));

        for (String mail : Arrays.asList("john@your-corp.example.com", "john@mycorp.example.com.example.net", "john")) {
            final SamlAttributes attributes = new SamlAttributes(
                    new SamlNameId(NameIDType.TRANSIENT, randomAlphaOfLength(12), null, null, null),
                    randomAlphaOfLength(16),
                    Collections.singletonList(
                            new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.3", "mail", Collections.singletonList(mail))
                    ));
            Mockito.when(authenticator.authenticate(token)).thenReturn(attributes);

            final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            final AuthenticationResult result = future.actionGet();
            assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
            assertThat(result.getMessage(), containsString("attributes.principal"));
            assertThat(result.getMessage(), containsString("mail"));
            assertThat(result.getMessage(), containsString("@mycorp\\.example\\.com"));
        }
    }

    public void testCreateCredentialFromPemFiles() throws Exception {
        final Settings.Builder builder = buildSettings("http://example.com");
        final Path dir = createTempDir("encryption");
        final Path encryptionKeyPath = getDataPath("encryption.key");
        final Path destEncryptionKeyPath = dir.resolve("encryption.key");
        final PrivateKey encryptionKey = PemUtils.readPrivateKey(encryptionKeyPath, "encryption"::toCharArray);
        final Path encryptionCertPath = getDataPath("encryption.crt");
        final Path destEncryptionCertPath = dir.resolve("encryption.crt");
        final X509Certificate encryptionCert = CertParsingUtils.readX509Certificates(Collections.singletonList(encryptionCertPath))[0];
        Files.copy(encryptionKeyPath, destEncryptionKeyPath);
        Files.copy(encryptionCertPath, destEncryptionCertPath);
        builder.put(REALM_SETTINGS_PREFIX + ".encryption.key", destEncryptionKeyPath);
        builder.put(REALM_SETTINGS_PREFIX + ".encryption.certificate", destEncryptionCertPath);
        final Settings settings = builder.build();
        final RealmConfig realmConfig = realmConfigFromGlobalSettings(settings);
        final Credential credential = SamlRealm.buildEncryptionCredential(realmConfig).get(0);

        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(encryptionKey));
        assertThat(credential.getPublicKey(), equalTo(encryptionCert.getPublicKey()));
    }

    public void testCreateEncryptionCredentialFromKeyStore() throws Exception {
        final Path dir = createTempDir();
        final Settings.Builder builder = Settings.builder()
                .put(REALM_SETTINGS_PREFIX + ".type", "saml")
                .put("path.home", dir);
        final Path ksFile = dir.resolve("cred.p12");
        final boolean testMultipleEncryptionKeyPair = randomBoolean();
        final Tuple<X509Certificate, PrivateKey> certKeyPair1 = readKeyPair("RSA_4096");
        final Tuple<X509Certificate, PrivateKey> certKeyPair2 = readKeyPair("RSA_2048");
        final KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null);
        ks.setKeyEntry(getAliasName(certKeyPair1), certKeyPair1.v2(), "key-password".toCharArray(),
                new Certificate[] { certKeyPair1.v1() });
        if (testMultipleEncryptionKeyPair) {
            ks.setKeyEntry(getAliasName(certKeyPair2), certKeyPair2.v2(), "key-password".toCharArray(),
                    new Certificate[] { certKeyPair2.v1() });
        }
        try (OutputStream out = Files.newOutputStream(ksFile)) {
            ks.store(out, "ks-password".toCharArray());
        }
        builder.put(REALM_SETTINGS_PREFIX + ".encryption.keystore.path", ksFile.toString());
        builder.put(REALM_SETTINGS_PREFIX + ".encryption.keystore.type", "PKCS12");
        final boolean isEncryptionKeyStoreAliasSet = randomBoolean();
        if (isEncryptionKeyStoreAliasSet) {
            builder.put(REALM_SETTINGS_PREFIX + ".encryption.keystore.alias", getAliasName(certKeyPair1));
        }

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".encryption.keystore.secure_password", "ks-password");
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".encryption.keystore.secure_key_password", "key-password");
        builder.setSecureSettings(secureSettings);

        final Settings settings = builder.build();
        final RealmConfig realmConfig = realmConfigFromGlobalSettings(settings);
        final List<X509Credential> credentials = SamlRealm.buildEncryptionCredential(realmConfig);

        assertThat("Encryption Credentials should not be null", credentials, notNullValue());
        final int expectedCredentials = (isEncryptionKeyStoreAliasSet) ? 1 : (testMultipleEncryptionKeyPair) ? 2 : 1;
        assertEquals("Expected encryption credentials size does not match", expectedCredentials, credentials.size());
        credentials.stream().forEach((credential) -> {
            assertTrue("Unexpected private key in the list of encryption credentials",
                    Arrays.asList(new PrivateKey[] { certKeyPair1.v2(), certKeyPair2.v2() }).contains(credential.getPrivateKey()));
            assertTrue("Unexpected public key in the list of encryption credentials",
                    Arrays.asList(new PublicKey[] { (certKeyPair1.v1()).getPublicKey(), certKeyPair2.v1().getPublicKey() })
                            .contains(credential.getPublicKey()));
        });
    }

    public void testCreateSigningCredentialFromKeyStoreSuccessScenarios() throws Exception {
        final Path dir = createTempDir();
        final Settings.Builder builder = Settings.builder().put(REALM_SETTINGS_PREFIX + ".type", "saml").put("path.home", dir);
        final Path ksFile = dir.resolve("cred.p12");
        final Tuple<X509Certificate, PrivateKey> certKeyPair1 = readRandomKeyPair("RSA");
        final Tuple<X509Certificate, PrivateKey> certKeyPair2 = readRandomKeyPair("EC");

        final KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null);
        ks.setKeyEntry(getAliasName(certKeyPair1), certKeyPair1.v2(), "key-password".toCharArray(),
                new Certificate[] { certKeyPair1.v1() });
        ks.setKeyEntry(getAliasName(certKeyPair2), certKeyPair2.v2(), "key-password".toCharArray(),
                new Certificate[] { certKeyPair2.v1() });
        try (OutputStream out = Files.newOutputStream(ksFile)) {
            ks.store(out, "ks-password".toCharArray());
        }
        builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.path", ksFile.toString());
        builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.type", "PKCS12");
        final boolean isSigningKeyStoreAliasSet = randomBoolean();
        if (isSigningKeyStoreAliasSet) {
            builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.alias", getAliasName(certKeyPair1));
        }

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".signing.keystore.secure_password", "ks-password");
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".signing.keystore.secure_key_password", "key-password");
        builder.setSecureSettings(secureSettings);

        final Settings settings = builder.build();
        final RealmConfig realmConfig = realmConfigFromGlobalSettings(settings);

        // Should build signing credential and use the key from KS.
        final SigningConfiguration signingConfig = SamlRealm.buildSigningConfiguration(realmConfig);
        final Credential credential = signingConfig.getCredential();
        assertThat(credential, notNullValue());
        assertThat(credential.getPrivateKey(), equalTo(certKeyPair1.v2()));
        assertThat(credential.getPublicKey(), equalTo(certKeyPair1.v1().getPublicKey()));
    }

    public void testCreateSigningCredentialFromKeyStoreFailureScenarios() throws Exception {
        final Path dir = createTempDir();
        final Settings.Builder builder = Settings.builder().put(REALM_SETTINGS_PREFIX + ".type", "saml").put("path.home", dir);
        final Path ksFile = dir.resolve("cred.p12");
        final Tuple<X509Certificate, PrivateKey> certKeyPair1 = readKeyPair("RSA_4096");
        final Tuple<X509Certificate, PrivateKey> certKeyPair2 = readKeyPair("RSA_2048");
        final Tuple<X509Certificate, PrivateKey> certKeyPair3 = readRandomKeyPair("EC");

        final KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null);
        final boolean noRSAKeysInKS = randomBoolean();
        if (noRSAKeysInKS == false) {
            ks.setKeyEntry(getAliasName(certKeyPair1), certKeyPair1.v2(), "key-password".toCharArray(),
                    new Certificate[] { certKeyPair1.v1() });
            ks.setKeyEntry(getAliasName(certKeyPair2), certKeyPair2.v2(), "key-password".toCharArray(),
                    new Certificate[] { certKeyPair2.v1() });
        }
        ks.setKeyEntry(getAliasName(certKeyPair3), certKeyPair3.v2(), "key-password".toCharArray(),
                new Certificate[] { certKeyPair3.v1() });
        try (OutputStream out = Files.newOutputStream(ksFile)) {
            ks.store(out, "ks-password".toCharArray());
        }

        builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.path", ksFile.toString());
        builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.type", "PKCS12");
        final boolean isSigningKeyStoreAliasSet = randomBoolean();
        final Tuple<X509Certificate, PrivateKey> chosenAliasCertKeyPair;
        final String unknownAlias = randomAlphaOfLength(5);
        if (isSigningKeyStoreAliasSet) {
            chosenAliasCertKeyPair = randomFrom(Arrays.asList(certKeyPair3, null));
            if (chosenAliasCertKeyPair == null) {
                // Unknown alias
                builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.alias", unknownAlias);
            } else {
                builder.put(REALM_SETTINGS_PREFIX + ".signing.keystore.alias", getAliasName(chosenAliasCertKeyPair));
            }
        } else {
            chosenAliasCertKeyPair = null;
        }

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".signing.keystore.secure_password", "ks-password");
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".signing.keystore.secure_key_password", "key-password");
        builder.setSecureSettings(secureSettings);

        final Settings settings = builder.build();
        final RealmConfig realmConfig = realmConfigFromGlobalSettings(settings);

        if (isSigningKeyStoreAliasSet) {
            if (chosenAliasCertKeyPair == null) {
                // Unknown alias, this must throw exception
                final IllegalArgumentException illegalArgumentException =
                        expectThrows(IllegalArgumentException.class, () -> SamlRealm.buildSigningConfiguration(realmConfig));
                final String expectedErrorMessage = "The configured key store for "
                        + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_SETTINGS.getPrefix())
                        + " does not have a key associated with alias [" + unknownAlias + "] " + "(from setting "
                        + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_KEY_ALIAS) + ")";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            } else {
                final String chosenAliasName = getAliasName(chosenAliasCertKeyPair);
                // Since this is unsupported key type, this must throw exception
                final IllegalArgumentException illegalArgumentException =
                        expectThrows(IllegalArgumentException.class, () -> SamlRealm.buildSigningConfiguration(realmConfig));
                final String expectedErrorMessage = "The key associated with alias [" + chosenAliasName + "] " + "(from setting "
                        + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_KEY_ALIAS)
                        + ") uses unsupported key algorithm type [" + chosenAliasCertKeyPair.v2().getAlgorithm()
                        + "], only RSA is supported";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            }
        } else {
            if (noRSAKeysInKS) {
                // Should throw exception as no RSA keys in the keystore
                final IllegalArgumentException illegalArgumentException =
                        expectThrows(IllegalArgumentException.class, () -> SamlRealm.buildSigningConfiguration(realmConfig));
                final String expectedErrorMessage = "The configured key store for "
                        + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_SETTINGS.getPrefix())
                        + " does not contain any RSA key pairs";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            } else {
                // Should throw exception when multiple signing keys found and alias not set
                final IllegalArgumentException illegalArgumentException =
                        expectThrows(IllegalArgumentException.class, () -> SamlRealm.buildSigningConfiguration(realmConfig));
                final String expectedErrorMessage = "The configured key store for "
                        + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_SETTINGS.getPrefix())
                        + " has multiple keys but no alias has been specified (from setting "
                        + RealmSettings.getFullSettingKey(realmConfig, SamlRealmSettings.SIGNING_KEY_ALIAS) + ")";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            }
        }
    }

    private String getAliasName(final Tuple<X509Certificate, PrivateKey> certKeyPair) {
        // Keys are pre-generated with the same name, so add the serial no to the alias so that keystore entries won't be overwritten
        return certKeyPair.v1().getSubjectX500Principal().getName().toLowerCase(Locale.US) + "-"+
            certKeyPair.v1().getSerialNumber()+"-alias";
    }

    public void testBuildLogoutRequest() throws Exception {
        final Boolean useSingleLogout = randomFrom(true, false, null);
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final EntityDescriptor idp = mockIdp();
        final IDPSSODescriptor role = mock(IDPSSODescriptor.class);
        final SingleLogoutService slo = SamlUtils.buildObject(SingleLogoutService.class, SingleLogoutService.DEFAULT_ELEMENT_NAME);
        Mockito.when(idp.getRoleDescriptors(IDPSSODescriptor.DEFAULT_ELEMENT_NAME)).thenReturn(Collections.singletonList(role));
        Mockito.when(role.getSingleLogoutServices()).thenReturn(Collections.singletonList(slo));
        slo.setBinding(SAMLConstants.SAML2_REDIRECT_BINDING_URI);
        slo.setLocation("https://logout.saml/");

        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);

        final Settings.Builder realmSettings = Settings.builder()
                .put(SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getAttribute().getKey(), "uid");
        if (useSingleLogout != null) {
            realmSettings.put(SamlRealmSettings.IDP_SINGLE_LOGOUT.getKey(), useSingleLogout.booleanValue());
        }

        final RealmConfig config = realmConfigFromRealmSettings(realmSettings.build());

        final SamlRealm realm = new SamlRealm(config, roleMapper, authenticator, logoutHandler, () -> idp, sp);

        final NameID nameId = SamlUtils.buildObject(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
        nameId.setFormat(NameID.TRANSIENT);
        nameId.setValue(SamlUtils.generateSecureNCName(18));
        final String session = SamlUtils.generateSecureNCName(12);

        final LogoutRequest request = realm.buildLogoutRequest(nameId, session);
        if (Boolean.FALSE.equals(useSingleLogout)) {
            assertThat(request, nullValue());
        } else {
            assertThat(request, notNullValue());
            assertThat(request.getDestination(), equalTo("https://logout.saml/"));
            assertThat(request.getNameID(), equalTo(nameId));
            assertThat(request.getSessionIndexes(), iterableWithSize(1));
            assertThat(request.getSessionIndexes().get(0).getSessionIndex(), equalTo(session));
        }
    }

    private EntityDescriptor mockIdp() {
        final EntityDescriptor descriptor = mock(EntityDescriptor.class);
        Mockito.when(descriptor.getEntityID()).thenReturn("https://idp.saml/");
        return descriptor;
    }

    private Tuple<RealmConfig, SSLService> buildConfig(String path) throws Exception {
        Settings globalSettings = buildSettings(path).build();
        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        final RealmConfig config = realmConfigFromGlobalSettings(globalSettings);
        final SSLService sslService = new SSLService(globalSettings, env);
        return new Tuple<>(config, sslService);
    }

    private Settings.Builder buildSettings(String idpMetaDataPath) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".ssl.keystore.secure_password", "testnode");
        return Settings.builder()
                .put(REALM_SETTINGS_PREFIX + ".ssl.verification_mode", "certificate")
                .put(REALM_SETTINGS_PREFIX + ".ssl.keystore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .put(REALM_SETTINGS_PREFIX + ".type", "saml")
                .put(REALM_SETTINGS_PREFIX + "." + SamlRealmSettings.IDP_METADATA_PATH.getKey(), idpMetaDataPath)
                .put(REALM_SETTINGS_PREFIX + "." + SamlRealmSettings.IDP_ENTITY_ID.getKey(), TEST_IDP_ENTITY_ID)
                .put(REALM_SETTINGS_PREFIX + "." + SamlRealmSettings.IDP_METADATA_HTTP_REFRESH.getKey(), METADATA_REFRESH + "ms")
                .put("path.home", createTempDir())
                .setSecureSettings(secureSettings);
    }

    private RealmConfig realmConfigFromRealmSettings(Settings realmSettings) {
        final Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        return new RealmConfig(REALM_NAME, realmSettings, globalSettings, env, new ThreadContext(globalSettings));
    }

    private RealmConfig realmConfigFromGlobalSettings(Settings globalSettings) {
        final Settings realmSettings = globalSettings.getByPrefix(REALM_SETTINGS_PREFIX + ".");
        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        return new RealmConfig(REALM_NAME, realmSettings, globalSettings, env, new ThreadContext(globalSettings));
    }

    private void assertIdp1MetadataParsedCorrectly(EntityDescriptor descriptor) {
        IDPSSODescriptor idpssoDescriptor = descriptor.getIDPSSODescriptor(SAMLConstants.SAML20P_NS);
        assertNotNull(idpssoDescriptor);
        List<SingleSignOnService> ssoServices = idpssoDescriptor.getSingleSignOnServices();
        assertEquals(2, ssoServices.size());
        assertEquals(SAMLConstants.SAML2_POST_BINDING_URI, ssoServices.get(0).getBinding());
        assertEquals(SAMLConstants.SAML2_REDIRECT_BINDING_URI, ssoServices.get(1).getBinding());
    }
}
