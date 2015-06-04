/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.pki;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.DnRoleMapper;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.x500.X500Principal;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class PkiRealmTests extends ElasticsearchTestCase {

    private Settings globalSettings;

    @Before
    public void setup() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    }

    @Test
    public void testTokenSupport() {
        RealmConfig config = new RealmConfig("", Settings.EMPTY, globalSettings);
        PkiRealm realm = new PkiRealm(config, mock(DnRoleMapper.class));

        assertThat(realm.supports(null), is(false));
        assertThat(realm.supports(new UsernamePasswordToken("", new SecuredString(new char[0]))), is(false));
        assertThat(realm.supports(new X509AuthenticationToken(new X509Certificate[0], "", "")), is(true));
    }

    @Test
    public void extractTokenFromRestRequest() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert"));
        RestRequest restRequest = new FakeRestRequest();
        restRequest.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.EMPTY, globalSettings), mock(DnRoleMapper.class));

        X509AuthenticationToken token = realm.token(restRequest);
        assertThat(token, is(notNullValue()));
        assertThat(token.dn(), is("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
        assertThat(token.principal(), is("Elasticsearch Test Node"));
    }

    @Test
    public void extractTokenFromTransportMessage() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert"));
        Message message = new Message();
        message.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[]{certificate});
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.EMPTY, globalSettings), mock(DnRoleMapper.class));

        X509AuthenticationToken token = realm.token(message);
        assertThat(token, is(notNullValue()));
        assertThat(token.dn(), is("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
        assertThat(token.principal(), is("Elasticsearch Test Node"));
    }

    @Test
    public void authenticateBasedOnCertToken() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert"));
        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[] { certificate }, "Elasticsearch Test Node", "CN=Elasticsearch Test Node,");
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.EMPTY, globalSettings), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());

        User user = realm.authenticate(token);
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("Elasticsearch Test Node"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    @Test
    public void customUsernamePattern() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert"));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        PkiRealm realm = new PkiRealm(new RealmConfig("", Settings.builder().put("username_pattern", "OU=(.*?),").build(), globalSettings), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());
        FakeRestRequest restRequest = new FakeRestRequest();
        restRequest.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(restRequest);
        User user = realm.authenticate(token);
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("elasticsearch"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    @Test
    public void verificationUsingATruststore() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert"));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        Settings settings = Settings.builder()
                .put("truststore.path", getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks"))
                .put("truststore.password", "testnode")
                .build();
        PkiRealm realm = new PkiRealm(new RealmConfig("", settings, globalSettings), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());

        FakeRestRequest restRequest = new FakeRestRequest();
        restRequest.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(restRequest);
        User user = realm.authenticate(token);
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("Elasticsearch Test Node"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    @Test
    public void verificationFailsUsingADifferentTruststore() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert"));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        Settings settings = Settings.builder()
                .put("truststore.path", getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .put("truststore.password", "testnode-client-profile")
                .build();
        PkiRealm realm = new PkiRealm(new RealmConfig("", settings, globalSettings), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());

        FakeRestRequest restRequest = new FakeRestRequest();
        restRequest.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(restRequest);
        User user = realm.authenticate(token);
        assertThat(user, is(nullValue()));
    }

    @Test
    public void truststorePathWithoutPasswordThrowsException() throws Exception {
        Settings settings = Settings.builder()
                .put("truststore.path", getDataPath("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks"))
                .build();
        try {
            new PkiRealm(new RealmConfig("", settings, globalSettings), mock(DnRoleMapper.class));
            fail("exception should have been thrown");
        } catch (ShieldSettingsException e) {
            assertThat(e.getMessage(), containsString("no truststore password configured"));
        }
    }

    @Test
    public void certificateWithOnlyCnExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[]{certificate}, Pattern.compile(PkiRealm.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
        assertThat(token, notNullValue());
        assertThat(token.principal(), is("PKI Client"));
        assertThat(token.dn(), is("CN=PKI Client"));
    }

    @Test
    public void certificateWithCnAndOuExtractsProperly() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("CN=PKI Client, OU=Shield");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[]{certificate}, Pattern.compile(PkiRealm.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
        assertThat(token, notNullValue());
        assertThat(token.principal(), is("PKI Client"));
        assertThat(token.dn(), is("CN=PKI Client, OU=Shield"));
    }

    @Test
    public void certificateWithCnInMiddle() throws Exception {
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = new X500Principal("EMAILADDRESS=pki@elastic.co, CN=PKI Client, OU=Shield");
        when(certificate.getSubjectX500Principal()).thenReturn(principal);

        X509AuthenticationToken token = PkiRealm.token(new X509Certificate[]{certificate}, Pattern.compile(PkiRealm.DEFAULT_USERNAME_PATTERN), NoOpLogger.INSTANCE);
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

    private static class Message extends TransportMessage<Message> {
        private Message() {
        }
    }
}
