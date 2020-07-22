/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import com.sun.net.httpserver.HttpsServer;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.TestsSSLService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.hamcrest.Matchers;
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
import java.security.AccessController;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PrivilegedAction;
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
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
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
import static org.mockito.Mockito.when;

/**
 * Basic unit tests for the SAMLRealm
 */
public class SamlRealmTests extends SamlTestCase {

    public static final String TEST_IDP_ENTITY_ID = "http://demo_josso_1.josso.dev.docker:8081/IDBUS/JOSSO-TUTORIAL/IDP1/SAML2/MD";
    private static final int METADATA_REFRESH = 3000;

    private static final String REALM_NAME = "my-saml";
    private static final String REALM_SETTINGS_PREFIX = "xpack.security.authc.realms.saml." + REALM_NAME;

    private Settings globalSettings;
    private Environment env;
    private ThreadContext threadContext;

    @Before
    public void setupEnv() throws PrivilegedActionException {
        SamlUtils.initialize(logger);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
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
        mockSecureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.key",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("xpack.security.http.ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .put("xpack.security.http.ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .putList("xpack.security.http.ssl.supported_protocols", getProtocols())
            .put("path.home", createTempDir())
            .setSecureSettings(mockSecureSettings)
            .build();
        TestsSSLService sslService = new TestsSSLService(TestEnvironment.newEnvironment(settings));
        try (MockWebServer proxyServer =
                     new MockWebServer(sslService.sslContext("xpack.security.http.ssl"), false)) {
            proxyServer.start();
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));
            proxyServer.enqueue(new MockResponse().setResponseCode(200).setBody(body).addHeader("Content-Type", "application/xml"));
            assertEquals(0, proxyServer.requests().size());

            Tuple<RealmConfig, SSLService> config = buildConfig("https://localhost:" + proxyServer.getPort());
            logger.info("Settings\n{}", config.v1().settings().toDelimitedString('\n'));
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
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        AtomicReference<UserRoleMapper.UserData> userData = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            assert invocation.getArguments().length == 2;
            userData.set((UserRoleMapper.UserData) invocation.getArguments()[0]);
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.singleton("superuser"));
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));

        final boolean useNameId = randomBoolean();
        final boolean principalIsEmailAddress = randomBoolean();
        final Boolean populateUserMetadata = randomFrom(Boolean.TRUE, Boolean.FALSE, null);
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        AuthenticationResult result = performAuthentication(roleMapper, useNameId, principalIsEmailAddress, populateUserMetadata, false,
            authenticatingRealm);
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser().principal(), equalTo(useNameId ? "clint.barton" : "cbarton"));
        assertThat(result.getUser().email(), equalTo("cbarton@shield.gov"));
        assertThat(result.getUser().roles(), arrayContainingInAnyOrder("superuser"));
        if (populateUserMetadata == Boolean.FALSE) {
            // TODO : "saml_nameid" should be null too, but the logout code requires it for now.
            assertThat(result.getUser().metadata().get("saml_uid"), nullValue());
        } else {
            final String nameIdValue = principalIsEmailAddress ? "clint.barton@shield.gov" : "clint.barton";
            final String uidValue = principalIsEmailAddress ? "cbarton@shield.gov" : "cbarton";
            assertThat(result.getUser().metadata().get("saml_nameid"), equalTo(nameIdValue));
            assertThat(result.getUser().metadata().get("saml_uid"), instanceOf(Iterable.class));
            assertThat((Iterable<?>) result.getUser().metadata().get("saml_uid"), contains(uidValue));
        }

        assertThat(userData.get().getUsername(), equalTo(useNameId ? "clint.barton" : "cbarton"));
        assertThat(userData.get().getGroups(), containsInAnyOrder("avengers", "shield"));
    }

    public void testAuthenticateWithAuthorizingRealm() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            assert invocation.getArguments().length == 2;
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onFailure(new RuntimeException("Role mapping should not be called"));
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));

        final boolean useNameId = randomBoolean();
        final boolean principalIsEmailAddress = randomBoolean();
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        AuthenticationResult result = performAuthentication(roleMapper, useNameId, principalIsEmailAddress, null, true,
            authenticatingRealm);
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser().principal(), equalTo(useNameId ? "clint.barton" : "cbarton"));
        assertThat(result.getUser().email(), equalTo("cbarton@shield.gov"));
        assertThat(result.getUser().roles(), arrayContainingInAnyOrder("lookup_user_role"));
        assertThat(result.getUser().fullName(), equalTo("Clinton Barton"));
        assertThat(result.getUser().metadata().entrySet(), Matchers.iterableWithSize(1));
        assertThat(result.getUser().metadata().get("is_lookup"), Matchers.equalTo(true));
    }

    public void testAuthenticateWithWrongRealmName() throws Exception {
        AuthenticationResult result = performAuthentication(mock(UserRoleMapper.class), randomBoolean(), randomBoolean(), null, true,
            REALM_NAME+randomAlphaOfLength(8));
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
    }

    private AuthenticationResult performAuthentication(UserRoleMapper roleMapper, boolean useNameId, boolean principalIsEmailAddress,
                                                       Boolean populateUserMetadata, boolean useAuthorizingRealm,
                                                       String authenticatingRealm) throws Exception {
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);

        final String userPrincipal = useNameId ? "clint.barton" : "cbarton";
        final String nameIdValue = principalIsEmailAddress ? "clint.barton@shield.gov" : "clint.barton";
        final String uidValue = principalIsEmailAddress ? "cbarton@shield.gov" : "cbarton";

        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("mock", "mock_lookup");
        final MockLookupRealm lookupRealm = new MockLookupRealm(
            new RealmConfig(realmIdentifier,
                Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
                env, threadContext));

        final Settings.Builder settingsBuilder = Settings.builder()
                .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getAttribute()), useNameId ? "nameid" : "uid")
                .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.GROUPS_ATTRIBUTE.getAttribute()), "groups")
                .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.MAIL_ATTRIBUTE.getAttribute()), "mail");
        if (principalIsEmailAddress) {
            final boolean anchoredMatch = randomBoolean();
            settingsBuilder.put(getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getPattern()),
                    anchoredMatch ? "^([^@]+)@shield.gov$" : "^([^@]+)@");
        }
        if (populateUserMetadata != null) {
            settingsBuilder.put(getFullSettingKey(REALM_NAME, SamlRealmSettings.POPULATE_USER_METADATA),
                    populateUserMetadata.booleanValue());
        }
        if (useAuthorizingRealm) {
            settingsBuilder.putList(getFullSettingKey(new RealmConfig.RealmIdentifier("saml", REALM_NAME),
                DelegatedAuthorizationSettings.AUTHZ_REALMS), lookupRealm.name());
            lookupRealm.registerUser(new User(userPrincipal, new String[]{ "lookup_user_role" }, "Clinton Barton", "cbarton@shield.gov",
                Collections.singletonMap("is_lookup", true), true));
        }

        final Settings realmSettings = settingsBuilder.build();
        final RealmConfig config = buildConfig(realmSettings);
        final SamlRealm realm = buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp);
        initializeRealms(realm, lookupRealm);

        final SamlToken token = new SamlToken(new byte[0], Collections.singletonList("<id>"), authenticatingRealm);

        final SamlAttributes attributes = new SamlAttributes(
                new SamlNameId(NameIDType.PERSISTENT, nameIdValue, idp.getEntityID(), sp.getEntityId(), null),
                randomAlphaOfLength(16),
                Arrays.asList(
                        new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.1", "uid", Collections.singletonList(uidValue)),
                        new SamlAttributes.SamlAttribute("urn:oid:1.3.6.1.4.1.5923.1.5.1.1", "groups", Arrays.asList("avengers", "shield")),
                        new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.3", "mail", Arrays.asList("cbarton@shield.gov"))
                ));
        when(authenticator.authenticate(token)).thenReturn(attributes);

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        return future.get();
    }

    private void initializeRealms(Realm... realms) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_AUTHORIZATION_REALM)).thenReturn(true);

        final List<Realm> realmList = Arrays.asList(realms);
        for (Realm realm : realms) {
            realm.initialize(realmList, licenseState);
        }
    }

    public SamlRealm buildRealm(RealmConfig config, UserRoleMapper roleMapper, SamlAuthenticator authenticator,
                                SamlLogoutRequestHandler logoutHandler, EntityDescriptor idp, SpConfiguration sp) throws Exception {
        try {
            return new SamlRealm(config, roleMapper, authenticator, logoutHandler, mock(SamlLogoutResponseHandler.class), () -> idp, sp);
        } catch (SettingsException e) {
            logger.info(new ParameterizedMessage("Settings are invalid:\n{}", config.settings().toDelimitedString('\n')), e);
            throw e;
        }
    }

    public void testAttributeSelectionWithRegex() throws Exception {
        final boolean useFriendlyName = randomBoolean();
        final Settings settings = Settings.builder()
                .put(REALM_SETTINGS_PREFIX + ".attributes.principal", useFriendlyName ? "mail" : "urn:oid:0.9.2342.19200300.100.1.3")
                .put(REALM_SETTINGS_PREFIX + ".attribute_patterns.principal", "^(.+)@\\w+.example.com$")
                .build();

        final RealmConfig config = buildConfig(settings);

        final SamlRealmSettings.AttributeSetting principalSetting = new SamlRealmSettings.AttributeSetting("principal");
        final SamlRealm.AttributeParser parser = SamlRealm.AttributeParser.forSetting(logger, principalSetting, config, false);

        final SamlAttributes attributes = new SamlAttributes(
                new SamlNameId(NameIDType.TRANSIENT, randomAlphaOfLength(24), null, null, null),
                randomAlphaOfLength(16),
                Collections.singletonList(new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.3", "mail",
                        Arrays.asList("john.smith@personal.example.net", "john.smith@corporate.example.com", "jsmith@corporate.example.com")
                )));

        final List<String> strings = parser.getAttribute(attributes);
        assertThat("For attributes: " + strings, strings, contains("john.smith", "jsmith"));
    }

    public void testSettingPatternWithoutAttributeThrowsSettingsException() throws Exception {
        final Settings realmSettings = Settings.builder()
                .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getAttribute()), "nameid")
                .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.NAME_ATTRIBUTE.getPattern()), "^\\s*(\\S.*\\S)\\s*$")
                .build();
        final RealmConfig config = buildConfig(realmSettings);

        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());

        final SettingsException settingsException = expectThrows(SettingsException.class,
                () -> buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attribute_patterns.name"));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attributes.name"));
    }

    public void testMissingPrincipalSettingThrowsSettingsException() throws Exception {
        final Settings realmSettings = Settings.EMPTY;
        final RealmConfig config = buildConfig(realmSettings);

        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());

        final SettingsException settingsException = expectThrows(SettingsException.class,
                () -> buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp));
        assertThat(settingsException.getMessage(), containsString(REALM_SETTINGS_PREFIX + ".attributes.principal"));
    }

    public void testNonMatchingPrincipalPatternThrowsSamlException() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);

        final Settings realmSettings = Settings.builder()
                .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getAttribute()), "mail")
                .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getPattern()), "^([^@]+)@mycorp\\.example\\.com$")
                .build();

        final RealmConfig config = buildConfig(realmSettings);

        final SamlRealm realm = buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp);
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final SamlToken token = new SamlToken(new byte[0], Collections.singletonList("<id>"), authenticatingRealm);

        for (String mail : Arrays.asList("john@your-corp.example.com", "john@mycorp.example.com.example.net", "john")) {
            final SamlAttributes attributes = new SamlAttributes(
                    new SamlNameId(NameIDType.TRANSIENT, randomAlphaOfLength(12), null, null, null),
                    randomAlphaOfLength(16),
                    Collections.singletonList(
                            new SamlAttributes.SamlAttribute("urn:oid:0.9.2342.19200300.100.1.3", "mail", Collections.singletonList(mail))
                    ));
            when(authenticator.authenticate(token)).thenReturn(attributes);

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
                        + RealmSettings.realmSettingPrefix(realmConfig.identifier()) + "signing."
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
                        + RealmSettings.realmSettingPrefix(realmConfig.identifier()) + "signing."
                        + " does not contain any RSA key pairs";
                assertEquals(expectedErrorMessage, illegalArgumentException.getLocalizedMessage());
            } else {
                // Should throw exception when multiple signing keys found and alias not set
                final IllegalArgumentException illegalArgumentException =
                        expectThrows(IllegalArgumentException.class, () -> SamlRealm.buildSigningConfiguration(realmConfig));
                final String expectedErrorMessage = "The configured key store for "
                        + RealmSettings.realmSettingPrefix(realmConfig.identifier()) + "signing."
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
        when(idp.getRoleDescriptors(IDPSSODescriptor.DEFAULT_ELEMENT_NAME)).thenReturn(Collections.singletonList(role));
        when(role.getSingleLogoutServices()).thenReturn(Collections.singletonList(slo));
        slo.setBinding(SAMLConstants.SAML2_REDIRECT_BINDING_URI);
        slo.setLocation("https://logout.saml/");

        final SpConfiguration sp = new SpConfiguration("<sp>", "https://saml/", null, null, null, Collections.emptyList());
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);

        final Settings.Builder realmSettings = Settings.builder()
                .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getAttribute()), "uid");
        if (useSingleLogout != null) {
            realmSettings.put(getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_SINGLE_LOGOUT), useSingleLogout.booleanValue());
        }

        final RealmConfig config = buildConfig(realmSettings.build());

        final SamlRealm realm = buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp);

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

    public void testCorrectRealmSelected() throws Exception {
        final String acsUrl = "https://idp.test/saml/login";
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final EntityDescriptor idp = mockIdp();
        final SpConfiguration sp = new SpConfiguration("<sp>", acsUrl, null, null, null, Collections.emptyList());
        final SamlAuthenticator authenticator = mock(SamlAuthenticator.class);
        final SamlLogoutRequestHandler logoutHandler = mock(SamlLogoutRequestHandler.class);
        final Settings.Builder realmSettings = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.PRINCIPAL_ATTRIBUTE.getAttribute()), "uid")
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_PATH), "http://url.to/metadata")
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_ENTITY_ID), TEST_IDP_ENTITY_ID)
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.SP_ACS), acsUrl);
        final RealmConfig config = buildConfig(realmSettings.build());
        final SamlRealm realm = buildRealm(config, roleMapper, authenticator, logoutHandler, idp, sp);
        final Realms realms = mock(Realms.class);
        when(realms.realm(REALM_NAME)).thenReturn(realm);
        when(realms.stream()).thenAnswer(i -> Stream.of(realm));
        final String emptyRealmName = randomBoolean() ? null : "";
        assertThat(SamlRealm.findSamlRealms(realms, emptyRealmName, acsUrl).size(), equalTo(1));
        assertThat(SamlRealm.findSamlRealms(realms, emptyRealmName, acsUrl).get(0), equalTo(realm));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", acsUrl).size(), equalTo(1));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", acsUrl).get(0), equalTo(realm));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", null).size(), equalTo(1));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", null).get(0), equalTo(realm));
        assertThat(SamlRealm.findSamlRealms(realms, "my-saml", "https://idp.test:443/saml/login").size(), equalTo(0));
        assertThat(SamlRealm.findSamlRealms(realms, "incorrect", acsUrl).size(), equalTo(0));
        assertThat(SamlRealm.findSamlRealms(realms, "incorrect", "https://idp.test:443/saml/login").size(), equalTo(0));
    }

    private EntityDescriptor mockIdp() {
        final EntityDescriptor descriptor = mock(EntityDescriptor.class);
        when(descriptor.getEntityID()).thenReturn("https://idp.saml/");
        return descriptor;
    }

    private Tuple<RealmConfig, SSLService> buildConfig(String idpMetadataPath) throws Exception {
        Settings globalSettings = buildSettings(idpMetadataPath).build();
        final RealmConfig config = realmConfigFromGlobalSettings(globalSettings);
        final SSLService sslService = new SSLService(config.env());
        return new Tuple<>(config, sslService);
    }

    private Settings.Builder buildSettings(String idpMetadataPath) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(REALM_SETTINGS_PREFIX + ".ssl.secure_key_passphrase", "testnode");
        return Settings.builder()
            .put(REALM_SETTINGS_PREFIX + ".ssl.verification_mode", "certificate")
            .put(REALM_SETTINGS_PREFIX + ".ssl.key",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put(REALM_SETTINGS_PREFIX + ".ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .put(REALM_SETTINGS_PREFIX + ".ssl.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_PATH), idpMetadataPath)
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_ENTITY_ID), TEST_IDP_ENTITY_ID)
            .put(getFullSettingKey(REALM_NAME, SamlRealmSettings.IDP_METADATA_HTTP_REFRESH), METADATA_REFRESH + "ms")
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings);
    }

    private RealmConfig buildConfig(Settings realmSettings) {
        final Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(realmSettings).build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("saml", REALM_NAME);
        return new RealmConfig(realmIdentifier,
            Settings.builder().put(settings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            env, threadContext);
    }

    private RealmConfig realmConfigFromGlobalSettings(Settings globalSettings) {
        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("saml", REALM_NAME);
        return new RealmConfig(realmIdentifier,
            Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            env,
            new ThreadContext(globalSettings));
    }

    private void assertIdp1MetadataParsedCorrectly(EntityDescriptor descriptor) {
        IDPSSODescriptor idpssoDescriptor = descriptor.getIDPSSODescriptor(SAMLConstants.SAML20P_NS);
        assertNotNull(idpssoDescriptor);
        List<SingleSignOnService> ssoServices = idpssoDescriptor.getSingleSignOnServices();
        assertEquals(2, ssoServices.size());
        assertEquals(SAMLConstants.SAML2_POST_BINDING_URI, ssoServices.get(0).getBinding());
        assertEquals(SAMLConstants.SAML2_REDIRECT_BINDING_URI, ssoServices.get(1).getBinding());
    }

    /**
     * The {@link HttpsServer} in the JDK has issues with TLSv1.3 when running in a JDK prior to
     * 12.0.1 so we pin to TLSv1.2 when running on an earlier JDK
     */
    private static List<String> getProtocols() {
        if (JavaVersion.current().compareTo(JavaVersion.parse("12")) < 0) {
            return List.of("TLSv1.2");
        } else {
            JavaVersion full =
                AccessController.doPrivileged(
                    (PrivilegedAction<JavaVersion>) () -> JavaVersion.parse(System.getProperty("java.version")));
            if (full.compareTo(JavaVersion.parse("12.0.1")) < 0) {
                return List.of("TLSv1.2");
            }
        }
        return XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS;
    }
}
