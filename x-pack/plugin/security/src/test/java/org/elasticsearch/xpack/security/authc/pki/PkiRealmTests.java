/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.ssl.SslConfigException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.InternalRealmsSettings;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.support.NoOpLogger;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.BytesKey;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.PublicKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.security.auth.x500.X500Principal;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PkiRealmTests extends ESTestCase {

    public static final String REALM_NAME = "my_pki";
    private Settings globalSettings;
    private MockLicenseState licenseState;
    private ThreadPool threadPool;
    private ResourceWatcherService watcherService;

    @Before
    public void setup() throws Exception {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME);
        globalSettings = Settings.builder()
            .put("path.home", createTempDir())
            .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
        threadPool = new TestThreadPool(getTestName());
        watcherService = new ResourceWatcherService(
            Settings.builder().put(ResourceWatcherService.ENABLED.getKey(), false).build(),
            threadPool
        );
    }

    @After
    public void stopWatcherService() throws Exception {
        watcherService.close();
        terminate(threadPool);
    }

    public void testTokenSupport() throws Exception {
        RealmConfig config = new RealmConfig(
            new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
            globalSettings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(globalSettings)
        );
        PkiRealm realm = new PkiRealm(config, mock(UserRoleMapper.class));

        assertRealmUsageStats(realm, false, false, true, false);
        assertThat(realm.supports(null), is(false));
        assertThat(realm.supports(new UsernamePasswordToken("", new SecureString(new char[0]))), is(false));
        X509AuthenticationToken token = randomBoolean()
            ? X509AuthenticationToken.delegated(new X509Certificate[0], AuthenticationTestHelper.builder().build())
            : new X509AuthenticationToken(new X509Certificate[0]);
        assertThat(realm.supports(token), is(true));
    }

    public void testExtractToken() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });
        PkiRealm realm = new PkiRealm(
            new RealmConfig(
                new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
                globalSettings,
                TestEnvironment.newEnvironment(globalSettings),
                threadContext
            ),
            mock(UserRoleMapper.class)
        );

        X509AuthenticationToken token = realm.token(threadContext);
        assertThat(token, is(notNullValue()));
        assertThat(token.dn(), is("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
        assertThat(token.isDelegated(), is(false));
    }

    public void testAuthenticateBasedOnCertToken() throws Exception {
        assertSuccessfulAuthentication(Collections.emptySet());
    }

    public void testAuthenticateWithRoleMapping() throws Exception {
        final Set<String> roles = new HashSet<>();
        roles.add("admin");
        roles.add("kibana_user");
        assertSuccessfulAuthentication(roles);
    }

    public void testCertificateFingerprintsAreExposedAsMetadata() throws Exception {
        final X509AuthenticationToken token = buildToken();
        final X509Certificate leafCertificate = token.credentials()[0];
        final String certificateFingerprint = MessageDigests.toHexString(MessageDigests.sha256().digest(leafCertificate.getEncoded()));
        final String publicKeyFingerprint = MessageDigests.toHexString(
            MessageDigests.sha256().digest(leafCertificate.getPublicKey().getEncoded())
        );
        final ExpressionRoleMapping certificateMapping = ExpressionRoleMapping.parse(
            "certificate-fingerprint",
            new BytesArray(Strings.format("""
                roles:
                - certificate_role
                rules:
                  field:
                    metadata.pki_cert_fingerprint: "%s"
                enabled: true
                """, certificateFingerprint)),
            XContentType.YAML
        );
        final ExpressionRoleMapping publicKeyMapping = ExpressionRoleMapping.parse(
            "public-key-fingerprint",
            new BytesArray(Strings.format("""
                roles:
                - public_key_role
                rules:
                  field:
                    metadata.pki_public_key_fingerprint: "%s"
                enabled: true
                """, publicKeyFingerprint)),
            XContentType.YAML
        );
        final List<ExpressionRoleMapping> mappings = List.of(certificateMapping, publicKeyMapping);
        final PkiRealm realm = buildRealm(buildRoleMapper(mappings), globalSettings);

        final AuthenticationResult<User> result = authenticate(token, realm);

        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue().metadata().get(PkiRealm.PKI_CERT_FINGERPRINT_METADATA_KEY), is(certificateFingerprint));
        assertThat(result.getValue().metadata().get(PkiRealm.PKI_PUBLIC_KEY_FINGERPRINT_METADATA_KEY), is(publicKeyFingerprint));
        assertThat(result.getValue().roles(), arrayContainingInAnyOrder("certificate_role", "public_key_role"));
    }

    public void testAuthenticationWithoutEncodedPublicKey() throws Exception {
        final X509Certificate certificate = mock(X509Certificate.class);
        when(certificate.getEncoded()).thenReturn(randomByteArrayOfLength(32));
        when(certificate.getSubjectX500Principal()).thenReturn(new X500Principal("CN=Test Client"));
        final PublicKey publicKey = mock(PublicKey.class);
        when(publicKey.getEncoded()).thenReturn(null);
        when(certificate.getPublicKey()).thenReturn(publicKey);
        final X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[] { certificate });
        final PkiRealm realm = buildRealm(buildRoleMapper(), globalSettings);

        final AuthenticationResult<User> result = authenticate(token, realm);

        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue().metadata().get(PkiRealm.PKI_CERT_FINGERPRINT_METADATA_KEY), notNullValue());
        assertThat(result.getValue().metadata().get(PkiRealm.PKI_PUBLIC_KEY_FINGERPRINT_METADATA_KEY), nullValue());
    }

    private void assertSuccessfulAuthentication(Set<String> roles) throws Exception {
        X509AuthenticationToken token = buildToken();
        UserRoleMapper roleMapper = buildRoleMapper(roles, token.dn());
        PkiRealm realm = buildRealm(roleMapper, globalSettings);
        verify(roleMapper).clearRealmCacheOnChange(realm);

        final String expectedUsername = PkiRealm.getPrincipalFromSubjectDN(
            Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN),
            token,
            NoOpLogger.INSTANCE
        );
        final AuthenticationResult<User> result = authenticate(token, realm);
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getValue();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is(expectedUsername));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(roles.size()));
        assertThat(user.roles(), arrayContainingInAnyOrder(roles.toArray()));

        final boolean testCaching = randomBoolean();
        final boolean invalidate = testCaching && randomBoolean();
        if (testCaching) {
            if (invalidate) {
                if (randomBoolean()) {
                    realm.expireAll();
                } else {
                    realm.expire(expectedUsername);
                }
            }
            final AuthenticationResult<User> result2 = authenticate(token, realm);
            assertThat(AuthenticationResult.Status.SUCCESS, is(result2.getStatus()));
            assertThat(user, is(result2.getValue()));
        }

        final int numTimes = invalidate ? 2 : 1;
        verify(roleMapper, times(numTimes)).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        verifyNoMoreInteractions(roleMapper);
    }

    private UserRoleMapper buildRoleMapper() {
        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        return roleMapper;
    }

    private UserRoleMapper buildRoleMapper(List<ExpressionRoleMapping> mappings) {
        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            final UserRoleMapper.UserData userData = invocation.getArgument(0);
            final ActionListener<Set<String>> listener = invocation.getArgument(1);
            listener.onResponse(ExpressionRoleMapping.resolveRoles(userData, mappings, null, NoOpLogger.INSTANCE));
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        return roleMapper;
    }

    private UserRoleMapper buildRoleMapper(Set<String> roles, String dn) {
        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            final UserRoleMapper.UserData userData = (UserRoleMapper.UserData) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            final ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            if (userData.getDn().equals(dn)) {
                listener.onResponse(roles);
            } else {
                listener.onFailure(new IllegalArgumentException("Expected DN '" + dn + "' but was '" + userData + "'"));
            }
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        return roleMapper;
    }

    private PkiRealm buildWatchedRealm(Settings settings) {
        final RealmConfig config = new RealmConfig(
            new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
            settings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(settings)
        );
        final PkiRealm realm = new PkiRealm(config, buildRoleMapper(), watcherService);
        realm.initialize(List.of(realm), licenseState);
        return realm;
    }

    private PkiRealm buildWatchedRealm(Path caCertPath) {
        return buildWatchedRealm(
            Settings.builder()
                .put(globalSettings)
                .putList("xpack.security.authc.realms.pki.my_pki.certificate_authorities", caCertPath.toString())
                .build()
        );
    }

    private PkiRealm buildWatchedRealmWithJksTruststore(Path jksPath, String password) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.authc.realms.pki.my_pki.truststore.secure_password", password);
        return buildWatchedRealm(
            Settings.builder()
                .put(globalSettings)
                .put("xpack.security.authc.realms.pki.my_pki.truststore.path", jksPath.toString())
                .setSecureSettings(secureSettings)
                .build()
        );
    }

    private static void replaceFileAndBumpMtime(Path src, Path dst) throws IOException {
        final FileTime originalModifiedTime = Files.getLastModifiedTime(dst);
        Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
        Files.setLastModifiedTime(dst, FileTime.fromMillis(originalModifiedTime.toMillis() + 5_000));
    }

    private PkiRealm buildRealm(UserRoleMapper roleMapper, Settings settings, Realm... otherRealms) {
        final RealmConfig config = new RealmConfig(
            new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
            settings,
            TestEnvironment.newEnvironment(settings),
            new ThreadContext(settings)
        );
        PkiRealm realm = new PkiRealm(config, roleMapper);
        List<Realm> allRealms = CollectionUtils.arrayAsArrayList(otherRealms);
        allRealms.add(realm);
        Collections.shuffle(allRealms, random());
        realm.initialize(allRealms, licenseState);
        return realm;
    }

    private X509AuthenticationToken buildToken() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        return new X509AuthenticationToken(new X509Certificate[] { certificate });
    }

    private AuthenticationResult<User> authenticate(X509AuthenticationToken token, PkiRealm realm) {
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        return future.actionGet();
    }

    public void testCustomUsernamePatternMatches() throws Exception {
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.username_pattern", "OU=(.*?),")
            .build();
        ThreadContext threadContext = new ThreadContext(settings);
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        UserRoleMapper roleMapper = buildRoleMapper();
        PkiRealm realm = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realm, false, false, false, false);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = authenticate(token, realm).getValue();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("elasticsearch"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testRdnOidMatches() throws Exception {
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.username_rdn_oid", "2.5.4.11")
            .build();
        ThreadContext threadContext = new ThreadContext(settings);
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        UserRoleMapper roleMapper = buildRoleMapper();
        PkiRealm realm = buildRealm(roleMapper, settings);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = authenticate(token, realm).getValue();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("elasticsearch"));
    }

    public void testRdnOidNameMatches() throws Exception {
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.username_rdn_name", "OU")
            .build();
        ThreadContext threadContext = new ThreadContext(settings);
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        UserRoleMapper roleMapper = buildRoleMapper();
        PkiRealm realm = buildRealm(roleMapper, settings);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = authenticate(token, realm).getValue();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("elasticsearch"));
    }

    public void testRdnOidNameNotMatches() throws Exception {
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.username_rdn_name", "UID")
            .build();
        ThreadContext threadContext = new ThreadContext(settings);
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        UserRoleMapper roleMapper = buildRoleMapper();
        PkiRealm realm = buildRealm(roleMapper, settings);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        assertThat(token, is(nullValue()));
    }

    public void testRdnOidNameUnknown() {
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.username_rdn_name", "UNKNOWN_OID_NAME")
            .build();
        UserRoleMapper roleMapper = buildRoleMapper();
        assertThrows(IllegalArgumentException.class, () -> buildRealm(roleMapper, settings));
    }

    public void testRedundantRdnOidSettings() {
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.username_rdn_oid", "2.5.4.3")
            .put("xpack.security.authc.realms.pki.my_pki.username_rdn_name", "UID")
            .build();
        UserRoleMapper roleMapper = buildRoleMapper();
        assertThrows(SettingsException.class, () -> buildRealm(roleMapper, settings));
    }

    public void testCustomUsernamePatternMismatchesAndNullToken() throws Exception {
        final Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.username_pattern", "OU=(mismatch.*?),")
            .build();
        ThreadContext threadContext = new ThreadContext(settings);
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        UserRoleMapper roleMapper = buildRoleMapper();
        PkiRealm realm = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realm, false, false, false, false);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        assertThat(token, is(nullValue()));
    }

    public void testVerificationUsingATruststore() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));

        UserRoleMapper roleMapper = buildRoleMapper();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.authc.realms.pki.my_pki.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(
                "xpack.security.authc.realms.pki.my_pki.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks")
            )
            .setSecureSettings(secureSettings)
            .build();
        ThreadContext threadContext = new ThreadContext(globalSettings);
        PkiRealm realm = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realm, true, false, true, false);

        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = authenticate(token, realm).getValue();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("Elasticsearch Test Node"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testVerificationUsingCertificateAuthorities() throws Exception {
        final Path caPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.crt");
        final Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/trusted.crt");
        final X509Certificate certificate = readCert(certPath);

        UserRoleMapper roleMapper = buildRoleMapper();
        Settings settings = Settings.builder()
            .put(globalSettings)
            .putList("xpack.security.authc.realms.pki.my_pki.certificate_authorities", caPath.toString())
            .build();
        ThreadContext threadContext = new ThreadContext(globalSettings);
        PkiRealm realm = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realm, true, false, true, false);

        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = authenticate(token, realm).getValue();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("trusted"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testTruststoreReloadUpdatesTrustDecisionsAndClearsCache() throws Exception {
        final Path tempDir = createTempDir();
        final Path caCertPath = tempDir.resolve("ca.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.crt"), caCertPath);
        final PkiRealm realm = buildWatchedRealm(caCertPath);

        final X509Certificate trustedCert = readCert(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/trusted.crt")
        );
        final X509AuthenticationToken trustedToken = new X509AuthenticationToken(new X509Certificate[] { trustedCert });
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));

        final X509Certificate restrictedTrustCert = readCert(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/restricted.trust.crt")
        );
        final X509AuthenticationToken restrictedToken = new X509AuthenticationToken(new X509Certificate[] { restrictedTrustCert });
        assertThat(authenticate(restrictedToken, realm).isAuthenticated(), is(false));

        replaceFileAndBumpMtime(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/restricted.trust.crt"),
            caCertPath
        );
        watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);

        // trusted.crt is no longer accepted; cache was cleared so the previous success entry is gone
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(false));
        // restricted.trust.crt is now the trust anchor
        assertThat(authenticate(restrictedToken, realm).isAuthenticated(), is(true));
    }

    public void testReloadKeepsPreviousContextOnFailureAndLogsWarning() throws Exception {
        final Path tempDir = createTempDir();
        final Path caCertPath = tempDir.resolve("ca.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.crt"), caCertPath);
        final PkiRealm realm = buildWatchedRealm(caCertPath);

        final X509Certificate trustedCert = readCert(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/trusted.crt")
        );
        final X509AuthenticationToken trustedToken = new X509AuthenticationToken(new X509Certificate[] { trustedCert });
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));

        final FileTime originalModifiedTime = Files.getLastModifiedTime(caCertPath);
        Files.writeString(caCertPath, "this is not a PEM certificate\n");
        Files.setLastModifiedTime(caCertPath, FileTime.fromMillis(originalModifiedTime.toMillis() + 5_000));
        MockLog.assertThatLogger(
            () -> watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH),
            PkiRealm.class,
            new MockLog.SeenEventExpectation(
                "failed reload logs a warning and retains the previous trust manager",
                PkiRealm.class.getCanonicalName(),
                Level.WARN,
                "*failed to reload truststore*"
            )
        );

        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));
    }

    public void testDeletedTruststorePreservesPreviousTrustManagerAndLogsWarning() throws Exception {
        final Path tempDir = createTempDir();
        final Path caCertPath = tempDir.resolve("ca.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.crt"), caCertPath);
        final PkiRealm realm = buildWatchedRealm(caCertPath);

        final X509Certificate trustedCert = readCert(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/trusted.crt")
        );
        final X509AuthenticationToken trustedToken = new X509AuthenticationToken(new X509Certificate[] { trustedCert });
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));

        Files.delete(caCertPath);
        MockLog.assertThatLogger(
            () -> watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH),
            PkiRealm.class,
            new MockLog.SeenEventExpectation(
                "deleted truststore logs a warning and retains the previous trust manager",
                PkiRealm.class.getCanonicalName(),
                Level.WARN,
                "*failed to reload truststore*"
            )
        );

        // expireAll() bypasses the cache so the assertion exercises the TM directly
        realm.expireAll();
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));
    }

    public void testDeletedThenRecreatedTruststoreReloadsNewTrustMaterial() throws Exception {
        final Path tempDir = createTempDir();
        final Path caCertPath = tempDir.resolve("ca.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.crt"), caCertPath);
        final PkiRealm realm = buildWatchedRealm(caCertPath);

        final X509Certificate trustedCert = readCert(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/trusted.crt")
        );
        final X509AuthenticationToken trustedToken = new X509AuthenticationToken(new X509Certificate[] { trustedCert });
        final X509Certificate restrictedTrustCert = readCert(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/restricted.trust.crt")
        );
        final X509AuthenticationToken restrictedToken = new X509AuthenticationToken(new X509Certificate[] { restrictedTrustCert });

        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));
        assertThat(authenticate(restrictedToken, realm).isAuthenticated(), is(false));

        // Step 1: delete the CA file — onFileDeleted fires, reload fails, old TM is preserved
        Files.delete(caCertPath);
        MockLog.assertThatLogger(
            () -> watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH),
            PkiRealm.class,
            new MockLog.SeenEventExpectation(
                "deletion logs a warning",
                PkiRealm.class.getCanonicalName(),
                Level.WARN,
                "*failed to reload truststore*"
            )
        );
        realm.expireAll();
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));

        // Step 2: create a new file at the same path — onFileCreated fires, reload succeeds
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/restricted.trust.crt"), caCertPath);
        watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);

        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(false));
        assertThat(authenticate(restrictedToken, realm).isAuthenticated(), is(true));
    }

    public void testCloseStopsWatchers() throws Exception {
        final Path tempDir = createTempDir();
        final Path caCertPath = tempDir.resolve("ca.crt");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.crt"), caCertPath);
        final PkiRealm realm = buildWatchedRealm(caCertPath);

        final X509Certificate trustedCert = readCert(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/trusted.crt")
        );
        final X509AuthenticationToken trustedToken = new X509AuthenticationToken(new X509Certificate[] { trustedCert });
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));

        realm.close();

        // After close(): replace the CA — the deregistered watcher must not fire, so the old trust manager stays in place
        replaceFileAndBumpMtime(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/restricted.trust.crt"),
            caCertPath
        );
        watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);

        realm.expireAll();
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));
    }

    public void testSystemDefaultTrustBehavior() throws Exception {
        // Realm with no certificate_authorities: trustManager is null — trust falls back to the TLS channel.
        // No file watchers are registered so close() is a no-op beyond setting the closed flag.
        final PkiRealm realm = buildWatchedRealm(globalSettings);

        assertRealmUsageStats(realm, false, false, true, false);

        final X509Certificate cert = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));

        // Non-delegated: TLS channel is the trust anchor — any cert that passed the channel is accepted
        final X509AuthenticationToken nonDelegatedToken = new X509AuthenticationToken(new X509Certificate[] { cert });
        assertThat(authenticate(nonDelegatedToken, realm).isAuthenticated(), is(true));

        realm.expireAll();

        // Delegated: re-validation requires an explicit trust manager — rejected without one
        final X509AuthenticationToken delegatedToken = X509AuthenticationToken.delegated(
            new X509Certificate[] { cert },
            AuthenticationTestHelper.builder().build()
        );
        assertThat(authenticate(delegatedToken, realm).isAuthenticated(), is(false));

        // No watchers were registered: close() sets the closed flag but is otherwise a no-op
        realm.close();
        realm.close(); // idempotent
    }

    public void testMultiCertTruststoreReloadRemovesOneAnchor() throws Exception {
        final Path cert1Path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/self-signed/n1.c1.crt");
        final Path cert2Path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/self-signed/n2.c1.crt");

        final Path tempDir = createTempDir();
        final Path combinedTrustPath = tempDir.resolve("combined.crt");

        // Write both self-signed certs into a single PEM file — each is its own trust anchor
        Files.write(combinedTrustPath, Files.readAllBytes(cert1Path));
        Files.write(combinedTrustPath, Files.readAllBytes(cert2Path), StandardOpenOption.APPEND);

        final PkiRealm realm = buildWatchedRealm(combinedTrustPath);

        final X509Certificate cert1 = readCert(cert1Path);
        final X509Certificate cert2 = readCert(cert2Path);
        final X509AuthenticationToken token1 = new X509AuthenticationToken(new X509Certificate[] { cert1 });
        final X509AuthenticationToken token2 = new X509AuthenticationToken(new X509Certificate[] { cert2 });

        assertThat(authenticate(token1, realm).isAuthenticated(), is(true));
        assertThat(authenticate(token2, realm).isAuthenticated(), is(true));

        // Overwrite the combined file with only cert1 — cert2 is no longer a trust anchor
        final FileTime originalModifiedTime = Files.getLastModifiedTime(combinedTrustPath);
        Files.write(combinedTrustPath, Files.readAllBytes(cert1Path));
        Files.setLastModifiedTime(combinedTrustPath, FileTime.fromMillis(originalModifiedTime.toMillis() + 5_000));
        watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);

        assertThat(authenticate(token1, realm).isAuthenticated(), is(true));
        assertThat(authenticate(token2, realm).isAuthenticated(), is(false));
    }

    public void testTruststorePathHotReloadPreservesPreviousTrustManagerOnFailure() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());

        final Path tempDir = createTempDir();
        final Path jksPath = tempDir.resolve("truststore.jks");
        Files.copy(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"), jksPath);
        final PkiRealm realm = buildWatchedRealmWithJksTruststore(jksPath, "testnode");

        final X509Certificate trustedCert = readCert(
            getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt")
        );
        final X509AuthenticationToken trustedToken = new X509AuthenticationToken(new X509Certificate[] { trustedCert });
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));

        // Overwrite the JKS file with garbage — StoreTrustConfig.createTrustManager throws; previous TM is preserved
        final FileTime originalModifiedTime = Files.getLastModifiedTime(jksPath);
        Files.writeString(jksPath, "this is not a JKS keystore\n");
        Files.setLastModifiedTime(jksPath, FileTime.fromMillis(originalModifiedTime.toMillis() + 5_000));
        MockLog.assertThatLogger(
            () -> watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH),
            PkiRealm.class,
            new MockLog.SeenEventExpectation(
                "failed JKS reload logs a warning and retains the previous trust manager",
                PkiRealm.class.getCanonicalName(),
                Level.WARN,
                "*failed to reload truststore*"
            )
        );

        realm.expireAll();
        assertThat(authenticate(trustedToken, realm).isAuthenticated(), is(true));
    }

    public void testTwoCACertFilesOneChangeTriggersReload() throws Exception {
        // Two separate certificate_authorities entries produce two FileWatcher registrations.
        // Changing one file triggers a reload that re-reads both CA files.
        final Path cert1Path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/self-signed/n1.c1.crt");
        final Path cert2Path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/self-signed/n2.c1.crt");
        final Path restrictedPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/restricted.trust.crt");

        final Path tempDir = createTempDir();
        final Path ca1 = tempDir.resolve("ca1.crt");
        final Path ca2 = tempDir.resolve("ca2.crt");
        Files.copy(cert1Path, ca1);
        Files.copy(cert2Path, ca2);

        final PkiRealm realm = buildWatchedRealm(
            Settings.builder()
                .put(globalSettings)
                .putList("xpack.security.authc.realms.pki.my_pki.certificate_authorities", ca1.toString(), ca2.toString())
                .build()
        );

        final X509Certificate cert1 = readCert(cert1Path);
        final X509Certificate cert2 = readCert(cert2Path);
        final X509Certificate restrictedCert = readCert(restrictedPath);
        final X509AuthenticationToken token1 = new X509AuthenticationToken(new X509Certificate[] { cert1 });
        final X509AuthenticationToken token2 = new X509AuthenticationToken(new X509Certificate[] { cert2 });
        final X509AuthenticationToken restrictedToken = new X509AuthenticationToken(new X509Certificate[] { restrictedCert });

        assertThat(authenticate(token1, realm).isAuthenticated(), is(true));
        assertThat(authenticate(token2, realm).isAuthenticated(), is(true));
        assertThat(authenticate(restrictedToken, realm).isAuthenticated(), is(false));

        // Replace only ca1 — triggers a reload of both CA files; ca2 remains unchanged
        replaceFileAndBumpMtime(restrictedPath, ca1);
        watcherService.notifyNow(ResourceWatcherService.Frequency.HIGH);

        // ca1 now contains restrictedCert (now trusted), ca2 still contains cert2 (still trusted), cert1 removed
        assertThat(authenticate(token1, realm).isAuthenticated(), is(false));
        assertThat(authenticate(token2, realm).isAuthenticated(), is(true));
        assertThat(authenticate(restrictedToken, realm).isAuthenticated(), is(true));
    }

    public void testAuthenticationDelegationFailsWithoutTokenServiceAndTruststore() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.delegation.enabled", true)
            .build();
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new PkiRealm(
                new RealmConfig(
                    new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
                    settings,
                    TestEnvironment.newEnvironment(globalSettings),
                    threadContext
                ),
                mock(UserRoleMapper.class)
            )
        );
        assertThat(
            e.getMessage(),
            is(
                "PKI realms with delegation enabled require a trust configuration "
                    + "(xpack.security.authc.realms.pki.my_pki.certificate_authorities or "
                    + "xpack.security.authc.realms.pki.my_pki.truststore.path)"
                    + " and that the token service be also enabled (xpack.security.authc.token.enabled)"
            )
        );
    }

    public void testAuthenticationDelegationFailsWithoutTruststore() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put("xpack.security.authc.realms.pki.my_pki.delegation.enabled", true)
            .put("xpack.security.authc.token.enabled", true)
            .build();
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new PkiRealm(
                new RealmConfig(
                    new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
                    settings,
                    TestEnvironment.newEnvironment(globalSettings),
                    threadContext
                ),
                mock(UserRoleMapper.class)
            )
        );
        assertThat(
            e.getMessage(),
            is(
                "PKI realms with delegation enabled require a trust configuration "
                    + "(xpack.security.authc.realms.pki.my_pki.certificate_authorities "
                    + "or xpack.security.authc.realms.pki.my_pki.truststore.path)"
            )
        );
    }

    public void testAuthenticationDelegationSuccess() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        User mockUser = mock(User.class);
        when(mockUser.principal()).thenReturn("mockup_delegate_username");
        RealmRef mockRealmRef = mock(RealmRef.class);
        when(mockRealmRef.getName()).thenReturn("mockup_delegate_realm");
        when(mockRealmRef.getType()).thenReturn("mockup_delegate_realm");
        Authentication mockAuthentication = AuthenticationTestHelper.builder().user(mockUser).realmRef(mockRealmRef).build(false);
        X509AuthenticationToken delegatedToken = X509AuthenticationToken.delegated(
            new X509Certificate[] { certificate },
            mockAuthentication
        );

        UserRoleMapper roleMapper = buildRoleMapper();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.authc.realms.pki.my_pki.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(
                "xpack.security.authc.realms.pki.my_pki.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks")
            )
            .put("xpack.security.authc.realms.pki.my_pki.delegation.enabled", true)
            .put("xpack.security.authc.token.enabled", true)
            .setSecureSettings(secureSettings)
            .build();
        PkiRealm realmWithDelegation = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realmWithDelegation, true, false, true, true);

        AuthenticationResult<User> result = authenticate(delegatedToken, realmWithDelegation);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue(), is(notNullValue()));
        assertThat(result.getValue().principal(), is("Elasticsearch Test Node"));
        assertThat(result.getValue().roles(), is(notNullValue()));
        assertThat(result.getValue().roles().length, is(0));
        assertThat(result.getValue().metadata().get(PkiRealm.PKI_DELEGATED_BY_USER_METADATA_KEY), is("mockup_delegate_username"));
        assertThat(result.getValue().metadata().get(PkiRealm.PKI_DELEGATED_BY_REALM_METADATA_KEY), is("mockup_delegate_realm"));

        // Delegatee is run-as
        final Authentication runAsAuthentication = AuthenticationTestHelper.builder().realm().build(true);
        assertThat(runAsAuthentication.isRunAs(), is(true));
        delegatedToken = X509AuthenticationToken.delegated(new X509Certificate[] { certificate }, runAsAuthentication);
        realmWithDelegation.expireAll(); // clear the cache so the user is built again
        result = authenticate(delegatedToken, realmWithDelegation);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue(), is(notNullValue()));
        assertThat(result.getValue().principal(), is("Elasticsearch Test Node"));
        assertThat(result.getValue().roles(), is(notNullValue()));
        assertThat(result.getValue().roles().length, is(0));
        assertThat(
            result.getValue().metadata().get(PkiRealm.PKI_DELEGATED_BY_USER_METADATA_KEY),
            is(runAsAuthentication.getEffectiveSubject().getUser().principal())
        );
        assertThat(
            result.getValue().metadata().get(PkiRealm.PKI_DELEGATED_BY_REALM_METADATA_KEY),
            is(runAsAuthentication.getEffectiveSubject().getRealm().getName())
        );
    }

    public void testAuthenticationDelegationFailure() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        X509AuthenticationToken delegatedToken = X509AuthenticationToken.delegated(
            new X509Certificate[] { certificate },
            AuthenticationTestHelper.builder().build()
        );

        UserRoleMapper roleMapper = buildRoleMapper();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.authc.realms.pki.my_pki.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(
                "xpack.security.authc.realms.pki.my_pki.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks")
            )
            .setSecureSettings(secureSettings)
            .build();
        PkiRealm realmNoDelegation = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realmNoDelegation, true, false, true, false);

        AuthenticationResult<User> result = authenticate(delegatedToken, realmNoDelegation);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getValue(), is(nullValue()));
        assertThat(result.getMessage(), containsString("Realm does not permit delegation for"));
    }

    public void testVerificationFailsUsingADifferentTruststore() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        UserRoleMapper roleMapper = buildRoleMapper();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.authc.realms.pki.my_pki.truststore.secure_password", "testnode-client-profile");
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(
                "xpack.security.authc.realms.pki.my_pki.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks")
            )
            .setSecureSettings(secureSettings)
            .build();
        ThreadContext threadContext = new ThreadContext(settings);
        PkiRealm realm = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realm, true, false, true, false);

        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        AuthenticationResult<User> result = authenticate(token, realm);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getMessage(), containsString("not trusted"));
        assertThat(result.getValue(), is(nullValue()));
    }

    public void testTruststorePathWithoutPasswordThrowsException() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(
                "xpack.security.authc.realms.pki.my_pki.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks")
            )
            .build();
        SslConfigException e = expectThrows(
            SslConfigException.class,
            () -> new PkiRealm(
                new RealmConfig(
                    new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
                    settings,
                    TestEnvironment.newEnvironment(settings),
                    new ThreadContext(settings)
                ),
                mock(UserRoleMapper.class)
            )
        );
        assertThat(e, throwableWithMessage(containsString("incorrect password; (no password")));
    }

    public void testTruststorePathWithLegacyPasswordDoesNotThrow() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(
                "xpack.security.authc.realms.pki.my_pki.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks")
            )
            .put("xpack.security.authc.realms.pki.my_pki.truststore.password", "testnode-client-profile")
            .build();
        new PkiRealm(
            new RealmConfig(
                new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
                settings,
                TestEnvironment.newEnvironment(settings),
                new ThreadContext(settings)
            ),
            mock(UserRoleMapper.class)
        );
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { PkiRealmSettings.LEGACY_TRUST_STORE_PASSWORD.getConcreteSettingForNamespace(REALM_NAME) }
        );
    }

    public void testCertificateWithOnlyCnExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[] { certificate });
        assertThat(token, notNullValue());
        assertThat(token.dn(), is("CN=PKI Client"));

        String parsedPrincipal = PkiRealm.getPrincipalFromSubjectDN(
            Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN),
            token,
            NoOpLogger.INSTANCE
        );
        assertThat(parsedPrincipal, is("PKI Client"));
    }

    public void testCertificateWithCnAndOuExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client, OU=Security");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[] { certificate });
        assertThat(token, notNullValue());
        assertThat(token.dn(), is("CN=PKI Client, OU=Security"));

        String parsedPrincipal = PkiRealm.getPrincipalFromSubjectDN(
            Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN),
            token,
            NoOpLogger.INSTANCE
        );
        assertThat(parsedPrincipal, is("PKI Client"));
    }

    public void testCertificateWithCnInMiddle() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Security");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[] { certificate });
        assertThat(token, notNullValue());
        assertThat(token.dn(), is("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Security"));

        String parsedPrincipal = PkiRealm.getPrincipalFromSubjectDN(
            Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN),
            token,
            NoOpLogger.INSTANCE
        );
        assertThat(parsedPrincipal, is("PKI Client"));
    }

    public void testPKIRealmSettingsPassValidation() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.pki.pki1.order", "1")
            .put("xpack.security.authc.realms.pki.pki1.truststore.path", "/foo/bar")
            .put("xpack.security.authc.realms.pki.pki1.truststore.password", "supersecret")
            .build();
        List<Setting<?>> settingList = new ArrayList<>();
        settingList.addAll(InternalRealmsSettings.getSettings());
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(settingList));
        clusterSettings.validate(settings, true);

        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { PkiRealmSettings.LEGACY_TRUST_STORE_PASSWORD.getConcreteSettingForNamespace("pki1") }
        );
    }

    public void testDelegatedAuthorization() throws Exception {
        final X509AuthenticationToken token = buildToken();
        String parsedPrincipal = PkiRealm.getPrincipalFromSubjectDN(
            Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN),
            token,
            NoOpLogger.INSTANCE
        );

        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("mock", "other_realm");
        final MockLookupRealm otherRealm = new MockLookupRealm(
            new RealmConfig(
                realmIdentifier,
                Settings.builder()
                    .put(globalSettings)
                    .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
                    .build(),
                TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings)
            )
        );
        final User lookupUser = new User(parsedPrincipal);
        otherRealm.registerUser(lookupUser);

        final Settings realmSettings = Settings.builder()
            .put(globalSettings)
            .putList("xpack.security.authc.realms.pki." + REALM_NAME + ".authorization_realms", "other_realm")
            .build();
        final UserRoleMapper roleMapper = buildRoleMapper(Collections.emptySet(), token.dn());
        final PkiRealm pkiRealm = buildRealm(roleMapper, realmSettings, otherRealm);
        assertRealmUsageStats(pkiRealm, false, true, true, false);

        AuthenticationResult<User> result = authenticate(token, pkiRealm);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue(), sameInstance(lookupUser));

        // check that the authorizing realm is consulted even for cached principals
        final User lookupUser2 = new User(parsedPrincipal);
        otherRealm.registerUser(lookupUser2);

        result = authenticate(token, pkiRealm);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getValue(), sameInstance(lookupUser2));
    }

    public void testX509AuthenticationTokenOrdered() throws Exception {
        X509Certificate[] mockCertChain = new X509Certificate[2];
        mockCertChain[0] = mock(X509Certificate.class);
        when(mockCertChain[0].getIssuerX500Principal()).thenReturn(new X500Principal("CN=Test, OU=elasticsearch, O=org"));
        mockCertChain[1] = mock(X509Certificate.class);
        when(mockCertChain[1].getSubjectX500Principal()).thenReturn(new X500Principal("CN=Not Test, OU=elasticsearch, O=org"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new X509AuthenticationToken(mockCertChain));
        assertThat(e.getMessage(), is("certificates chain array is not ordered"));
    }

    private void assertRealmUsageStats(
        Realm realm,
        Boolean hasTruststore,
        Boolean hasAuthorizationRealms,
        Boolean hasDefaultUsernamePattern,
        Boolean isAuthenticationDelegated
    ) throws Exception {
        final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        realm.usageStats(future);
        Map<String, Object> usage = future.get();
        assertThat(usage.get("has_truststore"), is(hasTruststore));
        assertThat(usage.get("has_authorization_realms"), is(hasAuthorizationRealms));
        assertThat(usage.get("has_default_username_pattern"), is(hasDefaultUsernamePattern));
        assertThat(usage.get("is_authentication_delegated"), is(isAuthenticationDelegated));
    }

    public void testX509AuthenticationTokenCaching() throws Exception {
        X509Certificate[] mockCertChain = new X509Certificate[2];
        mockCertChain[0] = mock(X509Certificate.class);
        when(mockCertChain[0].getSubjectX500Principal()).thenReturn(new X500Principal("CN=Test, OU=elasticsearch, O=org"));
        when(mockCertChain[0].getIssuerX500Principal()).thenReturn(new X500Principal("CN=Test CA, OU=elasticsearch, O=org"));
        when(mockCertChain[0].getEncoded()).thenReturn(randomByteArrayOfLength(2));
        mockCertChain[1] = mock(X509Certificate.class);
        when(mockCertChain[1].getSubjectX500Principal()).thenReturn(new X500Principal("CN=Test CA, OU=elasticsearch, O=org"));
        when(mockCertChain[1].getEncoded()).thenReturn(randomByteArrayOfLength(3));
        BytesKey cacheKey = PkiRealm.computeTokenFingerprint(new X509AuthenticationToken(mockCertChain));

        BytesKey sameCacheKey = PkiRealm.computeTokenFingerprint(
            new X509AuthenticationToken(new X509Certificate[] { mockCertChain[0], mockCertChain[1] })
        );
        assertThat(cacheKey, is(sameCacheKey));

        BytesKey cacheKeyClient = PkiRealm.computeTokenFingerprint(new X509AuthenticationToken(new X509Certificate[] { mockCertChain[0] }));
        assertThat(cacheKey, is(not(cacheKeyClient)));

        BytesKey cacheKeyRoot = PkiRealm.computeTokenFingerprint(new X509AuthenticationToken(new X509Certificate[] { mockCertChain[1] }));
        assertThat(cacheKey, is(not(cacheKeyRoot)));
        assertThat(cacheKeyClient, is(not(cacheKeyRoot)));
    }

    static X509Certificate readCert(Path path) throws Exception {
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
