/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.InternalRealmsSettings;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.support.NoOpLogger;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.BytesKey;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.junit.Before;
import org.mockito.Mockito;

import javax.security.auth.x500.X500Principal;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
    private XPackLicenseState licenseState;

    @Before
    public void setup() throws Exception {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME);
        globalSettings = Settings.builder()
                .put("path.home", createTempDir())
                .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
                .build();
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_AUTHORIZATION_REALM)).thenReturn(true);
    }

    public void testTokenSupport() throws Exception {
        RealmConfig config = new RealmConfig(new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME),
            globalSettings,
            TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        PkiRealm realm = new PkiRealm(config, mock(UserRoleMapper.class));

        assertRealmUsageStats(realm, false, false, true, false);
        assertThat(realm.supports(null), is(false));
        assertThat(realm.supports(new UsernamePasswordToken("", new SecureString(new char[0]))), is(false));
        X509AuthenticationToken token = randomBoolean()
                ? X509AuthenticationToken.delegated(new X509Certificate[0], mock(Authentication.class))
                : new X509AuthenticationToken(new X509Certificate[0]);
        assertThat(realm.supports(token), is(true));
    }

    public void testExtractToken() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[]{certificate});
        PkiRealm realm = new PkiRealm(new RealmConfig(new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME), globalSettings,
                TestEnvironment.newEnvironment(globalSettings), threadContext), mock(UserRoleMapper.class));

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

    private void assertSuccessfulAuthentication(Set<String> roles) throws Exception {
        X509AuthenticationToken token = buildToken();
        UserRoleMapper roleMapper = buildRoleMapper(roles, token.dn());
        PkiRealm realm = buildRealm(roleMapper, globalSettings);
        verify(roleMapper).refreshRealmOnChange(realm);

        final String expectedUsername = PkiRealm.getPrincipalFromSubjectDN(Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN),
                token, NoOpLogger.INSTANCE);
        final AuthenticationResult result = authenticate(token, realm);
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getUser();
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
            final AuthenticationResult result2 = authenticate(token, realm);
            assertThat(AuthenticationResult.Status.SUCCESS, is(result2.getStatus()));
            assertThat(user, is(result2.getUser()));
        }

        final int numTimes = invalidate ? 2 : 1;
        verify(roleMapper, times(numTimes)).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));
        verifyNoMoreInteractions(roleMapper);
    }

    private UserRoleMapper buildRoleMapper() {
        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));
        return roleMapper;
    }

    private UserRoleMapper buildRoleMapper(Set<String> roles, String dn) {
        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        Mockito.doAnswer(invocation -> {
            final UserRoleMapper.UserData userData = (UserRoleMapper.UserData) invocation.getArguments()[0];
            final ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            if (userData.getDn().equals(dn)) {
                listener.onResponse(roles);
            } else {
                listener.onFailure(new IllegalArgumentException("Expected DN '" + dn + "' but was '" + userData + "'"));
            }
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));
        return roleMapper;
    }

    private PkiRealm buildRealm(UserRoleMapper roleMapper, Settings settings, Realm... otherRealms) {
        final RealmConfig config = new RealmConfig(new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME), settings,
            TestEnvironment.newEnvironment(settings), new ThreadContext(settings));
        PkiRealm realm = new PkiRealm(config, roleMapper);
        List<Realm> allRealms = CollectionUtils.arrayAsArrayList(otherRealms);
        allRealms.add(realm);
        Collections.shuffle(allRealms, random());
        realm.initialize(allRealms, licenseState);
        return realm;
    }

    private X509AuthenticationToken buildToken() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        return new X509AuthenticationToken(new X509Certificate[]{certificate});
    }

    private AuthenticationResult authenticate(X509AuthenticationToken token, PkiRealm realm) {
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        return future.actionGet();
    }

    public void testCustomUsernamePatternMatches() throws Exception {
        final Settings settings = Settings.builder()
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
        User user = authenticate(token, realm).getUser();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("elasticsearch"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
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
                .put("xpack.security.authc.realms.pki.my_pki.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .setSecureSettings(secureSettings)
                .build();
        ThreadContext threadContext = new ThreadContext(globalSettings);
        PkiRealm realm = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realm, true, false, true, false);

        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = authenticate(token, realm).getUser();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("Elasticsearch Test Node"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testAuthenticationDelegationFailsWithoutTokenServiceAndTruststore() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Settings settings = Settings.builder()
                .put(globalSettings)
                .put("xpack.security.authc.realms.pki.my_pki.delegation.enabled", true)
                .build();
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> new PkiRealm(new RealmConfig(new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME), settings,
                        TestEnvironment.newEnvironment(globalSettings), threadContext), mock(UserRoleMapper.class)));
        assertThat(e.getMessage(),
                is("PKI realms with delegation enabled require a trust configuration "
                        + "(xpack.security.authc.realms.pki.my_pki.certificate_authorities or "
                        + "xpack.security.authc.realms.pki.my_pki.truststore.path)"
                        + " and that the token service be also enabled (xpack.security.authc.token.enabled)"));
    }

    public void testAuthenticationDelegationFailsWithoutTruststore() throws Exception {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Settings settings = Settings.builder()
                .put(globalSettings)
                .put("xpack.security.authc.realms.pki.my_pki.delegation.enabled", true)
                .put("xpack.security.authc.token.enabled", true)
                .build();
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> new PkiRealm(new RealmConfig(new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME), settings,
                        TestEnvironment.newEnvironment(globalSettings), threadContext), mock(UserRoleMapper.class)));
        assertThat(e.getMessage(),
                is("PKI realms with delegation enabled require a trust configuration "
                        + "(xpack.security.authc.realms.pki.my_pki.certificate_authorities "
                        + "or xpack.security.authc.realms.pki.my_pki.truststore.path)"));
    }

    public void testAuthenticationDelegationSuccess() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        Authentication mockAuthentication = mock(Authentication.class);
        User mockUser = mock(User.class);
        when(mockUser.principal()).thenReturn("mockup_delegate_username");
        RealmRef mockRealmRef = mock(RealmRef.class);
        when(mockRealmRef.getName()).thenReturn("mockup_delegate_realm");
        when(mockAuthentication.getUser()).thenReturn(mockUser);
        when(mockAuthentication.getAuthenticatedBy()).thenReturn(mockRealmRef);
        X509AuthenticationToken delegatedToken = X509AuthenticationToken.delegated(new X509Certificate[] { certificate },
                mockAuthentication);

        UserRoleMapper roleMapper = buildRoleMapper();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.authc.realms.pki.my_pki.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put(globalSettings)
                .put("xpack.security.authc.realms.pki.my_pki.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .put("xpack.security.authc.realms.pki.my_pki.delegation.enabled", true)
                .put("xpack.security.authc.token.enabled", true)
                .setSecureSettings(secureSettings)
                .build();
        PkiRealm realmWithDelegation = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realmWithDelegation, true, false, true, true);

        AuthenticationResult result = authenticate(delegatedToken, realmWithDelegation);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser(), is(notNullValue()));
        assertThat(result.getUser().principal(), is("Elasticsearch Test Node"));
        assertThat(result.getUser().roles(), is(notNullValue()));
        assertThat(result.getUser().roles().length, is(0));
        assertThat(result.getUser().metadata().get("pki_delegated_by_user"), is("mockup_delegate_username"));
        assertThat(result.getUser().metadata().get("pki_delegated_by_realm"), is("mockup_delegate_realm"));
    }

    public void testAuthenticationDelegationFailure() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        X509AuthenticationToken delegatedToken = X509AuthenticationToken.delegated(new X509Certificate[] { certificate },
                mock(Authentication.class));

        UserRoleMapper roleMapper = buildRoleMapper();
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.authc.realms.pki.my_pki.truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put(globalSettings)
                .put("xpack.security.authc.realms.pki.my_pki.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .setSecureSettings(secureSettings)
                .build();
        PkiRealm realmNoDelegation = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realmNoDelegation, true, false, true, false);

        AuthenticationResult result = authenticate(delegatedToken, realmNoDelegation);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getUser(), is(nullValue()));
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
            .put("xpack.security.authc.realms.pki.my_pki.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .setSecureSettings(secureSettings)
                .build();
        ThreadContext threadContext = new ThreadContext(settings);
        PkiRealm realm = buildRealm(roleMapper, settings);
        assertRealmUsageStats(realm, true, false, true, false);

        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        AuthenticationResult result = authenticate(token, realm);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getMessage(), containsString("not trusted"));
        assertThat(result.getUser(), is(nullValue()));
    }

    public void testTruststorePathWithoutPasswordThrowsException() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        Settings settings = Settings.builder()
                .put(globalSettings)
                .put("xpack.security.authc.realms.pki.my_pki.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
                new PkiRealm(new RealmConfig(new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME), settings,
                        TestEnvironment.newEnvironment(settings), new ThreadContext(settings)),
                    mock(UserRoleMapper.class))
        );
        assertThat(e.getMessage(), containsString("Neither [xpack.security.authc.realms.pki.my_pki.truststore.secure_password] or [" +
                    "xpack.security.authc.realms.pki.my_pki.truststore.password] is configured"));
    }

    public void testTruststorePathWithLegacyPasswordDoesNotThrow() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, JKS keystores can't be used", inFipsJvm());
        Settings settings = Settings.builder()
                .put(globalSettings)
                .put("xpack.security.authc.realms.pki.my_pki.truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .put("xpack.security.authc.realms.pki.my_pki.truststore.password", "testnode-client-profile")
                .build();
        new PkiRealm(new RealmConfig(new RealmConfig.RealmIdentifier(PkiRealmSettings.TYPE, REALM_NAME), settings,
                TestEnvironment.newEnvironment(settings), new ThreadContext(settings)), mock(UserRoleMapper.class));
        assertSettingDeprecationsAndWarnings(new Setting[]{
                PkiRealmSettings.LEGACY_TRUST_STORE_PASSWORD.getConcreteSettingForNamespace(REALM_NAME)
        });
    }

    public void testCertificateWithOnlyCnExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[]{certificate});
        assertThat(token, notNullValue());
        assertThat(token.dn(), is("CN=PKI Client"));

        String parsedPrincipal = PkiRealm.getPrincipalFromSubjectDN(Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN), token,
                NoOpLogger.INSTANCE);
        assertThat(parsedPrincipal, is("PKI Client"));
    }

    public void testCertificateWithCnAndOuExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client, OU=Security");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[]{certificate});
        assertThat(token, notNullValue());
        assertThat(token.dn(), is("CN=PKI Client, OU=Security"));

        String parsedPrincipal = PkiRealm.getPrincipalFromSubjectDN(Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN), token,
                NoOpLogger.INSTANCE);
        assertThat(parsedPrincipal, is("PKI Client"));
    }

    public void testCertificateWithCnInMiddle() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Security");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[]{certificate});
        assertThat(token, notNullValue());
        assertThat(token.dn(), is("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Security"));

        String parsedPrincipal = PkiRealm.getPrincipalFromSubjectDN(Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN), token,
                NoOpLogger.INSTANCE);
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
        clusterSettings.validate(settings, false);

        assertSettingDeprecationsAndWarnings(new Setting[]{
                PkiRealmSettings.LEGACY_TRUST_STORE_PASSWORD.getConcreteSettingForNamespace("pki1")
        });
    }

    public void testDelegatedAuthorization() throws Exception {
        final X509AuthenticationToken token = buildToken();
        String parsedPrincipal = PkiRealm.getPrincipalFromSubjectDN(Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN), token,
                NoOpLogger.INSTANCE);

        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("mock", "other_realm");
        final MockLookupRealm otherRealm = new MockLookupRealm(new RealmConfig(
            realmIdentifier,
            Settings.builder().put(globalSettings)
                .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings)));
        final User lookupUser = new User(parsedPrincipal);
        otherRealm.registerUser(lookupUser);

        final Settings realmSettings = Settings.builder()
            .put(globalSettings)
            .putList("xpack.security.authc.realms.pki." + REALM_NAME + ".authorization_realms", "other_realm")
            .build();
        final UserRoleMapper roleMapper = buildRoleMapper(Collections.emptySet(), token.dn());
        final PkiRealm pkiRealm = buildRealm(roleMapper, realmSettings, otherRealm);
        assertRealmUsageStats(pkiRealm, false, true, true, false);

        AuthenticationResult result = authenticate(token, pkiRealm);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser(), sameInstance(lookupUser));

        // check that the authorizing realm is consulted even for cached principals
        final User lookupUser2 = new User(parsedPrincipal);
        otherRealm.registerUser(lookupUser2);

        result = authenticate(token, pkiRealm);
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser(), sameInstance(lookupUser2));
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

    private void assertRealmUsageStats(Realm realm, Boolean hasTruststore, Boolean hasAuthorizationRealms,
            Boolean hasDefaultUsernamePattern, Boolean isAuthenticationDelegated) throws Exception {
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

        BytesKey sameCacheKey = PkiRealm
                .computeTokenFingerprint(new X509AuthenticationToken(new X509Certificate[] { mockCertChain[0], mockCertChain[1] }));
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
