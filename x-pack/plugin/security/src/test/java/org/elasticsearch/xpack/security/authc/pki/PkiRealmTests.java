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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.support.NoOpLogger;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
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
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PkiRealmTests extends ESTestCase {

    private Settings globalSettings;

    @Before
    public void setup() throws Exception {
        globalSettings = Settings.builder()
                .put("path.home", createTempDir())
                .build();
    }

    public void testTokenSupport() {
        RealmConfig config = new RealmConfig("", Settings.EMPTY, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        PkiRealm realm = new PkiRealm(config, mock(UserRoleMapper.class));

        assertThat(realm.supports(null), is(false));
        assertThat(realm.supports(new UsernamePasswordToken("", new SecureString(new char[0]))), is(false));
        assertThat(realm.supports(new X509AuthenticationToken(new X509Certificate[0], "", "")), is(true));
    }

    public void testExtractToken() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.EMPTY, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings)), mock(UserRoleMapper.class));

        X509AuthenticationToken token = realm.token(threadContext);
        assertThat(token, is(notNullValue()));
        assertThat(token.dn(), is("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
        assertThat(token.principal(), is("Elasticsearch Test Node"));
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
        String dn = "CN=Elasticsearch Test Node,";
        final String expectedUsername = "Elasticsearch Test Node";
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[] { certificate }, "Elasticsearch Test Node", dn);
        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.EMPTY, globalSettings, TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(globalSettings)), roleMapper);
        verify(roleMapper).refreshRealmOnChange(realm);
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

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        final AuthenticationResult result = future.actionGet();
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
            future = new PlainActionFuture<>();
            realm.authenticate(token, future);
            assertEquals(AuthenticationResult.Status.SUCCESS, future.actionGet().getStatus());
            assertEquals(user, future.actionGet().getUser());
        }

        final int numTimes = invalidate ? 2 : 1;
        verify(roleMapper, times(numTimes)).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));
        verifyNoMoreInteractions(roleMapper);
    }

    public void testCustomUsernamePattern() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.builder().put("username_pattern", "OU=(.*?),").build(), globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings)),
                roleMapper);
        Mockito.doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        User user = future.actionGet().getUser();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("elasticsearch"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testVerificationUsingATruststore() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));

        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("truststore.secure_password", "testnode");
        Settings settings = Settings.builder()
                .put("truststore.path", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks"))
                .setSecureSettings(secureSettings)
                .build();
        PkiRealm realm = new PkiRealm(new RealmConfig("", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings)), roleMapper);
        Mockito.doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        User user = future.actionGet().getUser();
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("Elasticsearch Test Node"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testVerificationFailsUsingADifferentTruststore() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
        UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("truststore.secure_password", "testnode-client-profile");
        Settings settings = Settings.builder()
                .put("truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .setSecureSettings(secureSettings)
                .build();
        PkiRealm realm = new PkiRealm(new RealmConfig("", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings)), roleMapper);
        Mockito.doAnswer(invocation -> {
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(Collections.emptySet());
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), any(ActionListener.class));

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        User user = future.actionGet().getUser();
        assertThat(user, is(nullValue()));
    }

    public void testTruststorePathWithoutPasswordThrowsException() throws Exception {
        Settings settings = Settings.builder()
                .put("truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .build();
        try {
            new PkiRealm(new RealmConfig("mypki", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                    new ThreadContext(globalSettings)), mock(UserRoleMapper.class));
            fail("exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Neither [xpack.security.authc.realms.mypki.truststore.secure_password] or [" +
                    "xpack.security.authc.realms.mypki.truststore.password] is configured"));
        }
    }

    public void testTruststorePathWithLegacyPasswordDoesNotThrow() throws Exception {
        Settings settings = Settings.builder()
                .put("truststore.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .put("truststore.password", "testnode-client-profile")
                .build();
        new PkiRealm(new RealmConfig("mypki", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings)), mock(UserRoleMapper.class));
        assertSettingDeprecationsAndWarnings(new Setting[] { SSLConfigurationSettings.withoutPrefix().legacyTruststorePassword });
    }

    public void testCertificateWithOnlyCnExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[] { certificate },
                Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
        assertThat(token, notNullValue());
        assertThat(token.principal(), is("PKI Client"));
        assertThat(token.dn(), is("CN=PKI Client"));
    }

    public void testCertificateWithCnAndOuExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client, OU=Security");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[] { certificate },
                Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
        assertThat(token, notNullValue());
        assertThat(token.principal(), is("PKI Client"));
        assertThat(token.dn(), is("CN=PKI Client, OU=Security"));
    }

    public void testCertificateWithCnInMiddle() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Security");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[] { certificate },
                Pattern.compile(PkiRealmSettings.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
        assertThat(token, notNullValue());
        assertThat(token.principal(), is("PKI Client"));
        assertThat(token.dn(), is("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Security"));
    }

    public void testPKIRealmSettingsPassValidation() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.authc.realms.pki1.type", "pki")
                .put("xpack.security.authc.realms.pki1.truststore.path", "/foo/bar")
                .put("xpack.security.authc.realms.pki1.truststore.password", "supersecret")
                .build();
        List<Setting<?>> settingList = new ArrayList<>();
        RealmSettings.addSettings(settingList, Collections.emptyList());
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(settingList));
        clusterSettings.validate(settings, false);

        assertSettingDeprecationsAndWarnings(new Setting[] { SSLConfigurationSettings.withoutPrefix().legacyTruststorePassword });
    }

    static X509Certificate readCert(Path path) throws Exception {
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
