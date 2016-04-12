/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.pki;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.DnRoleMapper;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import javax.security.auth.x500.X500Principal;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PkiRealmTests extends ESTestCase {
    private Settings globalSettings;

    @Before
    public void setup() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    }

    public void testTokenSupport() {
        RealmConfig config = new RealmConfig("", Settings.EMPTY, globalSettings);
        PkiRealm realm = new PkiRealm(config, mock(DnRoleMapper.class));

        assertThat(realm.supports(null), is(false));
        assertThat(realm.supports(new UsernamePasswordToken("", new SecuredString(new char[0]))), is(false));
        assertThat(realm.supports(new X509AuthenticationToken(new X509Certificate[0], "", "")), is(true));
    }

    public void testExtractToken() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.crt"));
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.EMPTY, globalSettings), mock(DnRoleMapper.class));

        X509AuthenticationToken token = realm.token(threadContext);
        assertThat(token, is(notNullValue()));
        assertThat(token.dn(), is("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
        assertThat(token.principal(), is("Elasticsearch Test Node"));
    }

    public void testAuthenticateBasedOnCertToken() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.crt"));
        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[] { certificate }, "Elasticsearch Test Node",
                "CN=Elasticsearch Test Node,");
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.EMPTY, globalSettings), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());

        User user = realm.authenticate(token);
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("Elasticsearch Test Node"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testCustomUsernamePattern() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.crt"));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.builder().put("username_pattern", "OU=(.*?),").build(), globalSettings),
                roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = realm.authenticate(token);
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("elasticsearch"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testVerificationUsingATruststore() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.crt"));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        Settings settings = Settings.builder()
                .put("truststore.path", getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks"))
                .put("truststore.password", "testnode")
                .build();
        PkiRealm realm = new PkiRealm(new RealmConfig("", settings, globalSettings), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = realm.authenticate(token);
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("Elasticsearch Test Node"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    public void testVerificationFailsUsingADifferentTruststore() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.crt"));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        Settings settings = Settings.builder()
                .put("truststore.path", getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .put("truststore.password", "testnode-client-profile")
                .build();
        PkiRealm realm = new PkiRealm(new RealmConfig("", settings, globalSettings), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(threadContext);
        User user = realm.authenticate(token);
        assertThat(user, is(nullValue()));
    }

    public void testTruststorePathWithoutPasswordThrowsException() throws Exception {
        Settings settings = Settings.builder()
                .put("truststore.path", getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .build();
        try {
            new PkiRealm(new RealmConfig("", settings, globalSettings), mock(DnRoleMapper.class));
            fail("exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("no truststore password configured"));
        }
    }

    public void testCertificateWithOnlyCnExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[]{certificate},
                Pattern.compile(PkiRealm.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
        assertThat(token, notNullValue());
        assertThat(token.principal(), is("PKI Client"));
        assertThat(token.dn(), is("CN=PKI Client"));
    }

    public void testCertificateWithCnAndOuExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client, OU=Shield");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[]{certificate},
                Pattern.compile(PkiRealm.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
        assertThat(token, notNullValue());
        assertThat(token.principal(), is("PKI Client"));
        assertThat(token.dn(), is("CN=PKI Client, OU=Shield"));
    }

    public void testCertificateWithCnInMiddle() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Shield");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[]{certificate},
                Pattern.compile(PkiRealm.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
        assertThat(token, notNullValue());
        assertThat(token.principal(), is("PKI Client"));
        assertThat(token.dn(), is("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Shield"));
    }

    static X509Certificate readCert(Path path) throws Exception {
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
