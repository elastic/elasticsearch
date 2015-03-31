/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.pki;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.FakeRestRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.DnRoleMapper;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

public class PkiRealmTests extends ElasticsearchTestCase {

    @Test
    public void testTokenSupport() {
        RealmConfig config = new RealmConfig("");
        PkiRealm realm = new PkiRealm(config, mock(DnRoleMapper.class));

        assertThat(realm.supports(null), is(false));
        assertThat(realm.supports(new UsernamePasswordToken("", new SecuredString(new char[0]))), is(false));
        assertThat(realm.supports(new X509AuthenticationToken(new X509Certificate[0], "", "")), is(true));
    }

    @Test
    public void extractTokenFromRestRequest() throws Exception {
        X509Certificate certificate = readCert(Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert").toURI()));
        RestRequest restRequest = new FakeRestRequest();
        restRequest.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });
        PkiRealm realm = new PkiRealm(new RealmConfig(""), mock(DnRoleMapper.class));

        X509AuthenticationToken token = realm.token(restRequest);
        assertThat(token, is(notNullValue()));
        assertThat(token.dn(), is("CN=Elasticsearch Test Node,OU=elasticsearch,O=org"));
        assertThat(token.principal(), is("Elasticsearch Test Node"));
    }

    @Test
    public void extractTokenFromTransportMessage() throws Exception {
        X509Certificate certificate = readCert(Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert").toURI()));
        Message message = new Message();
        message.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[]{certificate});
        PkiRealm realm = new PkiRealm(new RealmConfig(""), mock(DnRoleMapper.class));

        X509AuthenticationToken token = realm.token(message);
        assertThat(token, is(notNullValue()));
        assertThat(token.dn(), is("CN=Elasticsearch Test Node,OU=elasticsearch,O=org"));
        assertThat(token.principal(), is("Elasticsearch Test Node"));
    }

    @Test
    public void authenticateBasedOnCertToken() throws Exception {
        X509Certificate certificate = readCert(Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert").toURI()));
        X509AuthenticationToken token = new X509AuthenticationToken(new X509Certificate[] { certificate }, "Elasticsearch Test Node", "CN=Elasticsearch Test Node,");
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        PkiRealm realm = new PkiRealm(new RealmConfig(""), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());

        User user = realm.authenticate(token);
        assertThat(user, is(notNullValue()));
        assertThat(user.principal(), is("Elasticsearch Test Node"));
        assertThat(user.roles(), is(notNullValue()));
        assertThat(user.roles().length, is(0));
    }

    @Test
    public void customUsernamePattern() throws Exception {
        X509Certificate certificate = readCert(Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert").toURI()));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        PkiRealm realm = new PkiRealm(new RealmConfig("", ImmutableSettings.builder().put("username_pattern", "OU=(.*?),").build()), roleMapper);
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
        X509Certificate certificate = readCert(Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert").toURI()));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        Settings settings = ImmutableSettings.builder()
                .put("truststore.path", Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI()).toAbsolutePath())
                .put("truststore.password", "testnode")
                .build();
        PkiRealm realm = new PkiRealm(new RealmConfig("", settings), roleMapper);
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
        X509Certificate certificate = readCert(Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.cert").toURI()));
        DnRoleMapper roleMapper = mock(DnRoleMapper.class);
        Settings settings = ImmutableSettings.builder()
                .put("truststore.path", Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks").toURI()).toAbsolutePath())
                .put("truststore.password", "testnode-client-profile")
                .build();
        PkiRealm realm = new PkiRealm(new RealmConfig("", settings), roleMapper);
        when(roleMapper.resolveRoles(anyString(), anyList())).thenReturn(Collections.<String>emptySet());

        FakeRestRequest restRequest = new FakeRestRequest();
        restRequest.putInContext(PkiRealm.PKI_CERT_HEADER_NAME, new X509Certificate[] { certificate });

        X509AuthenticationToken token = realm.token(restRequest);
        User user = realm.authenticate(token);
        assertThat(user, is(nullValue()));
    }

    @Test
    public void truststorePathWithoutPasswordThrowsException() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("truststore.path", Paths.get(PkiRealmTests.class.getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode-client-profile.jks").toURI()).toAbsolutePath())
                .build();
        try {
            new PkiRealm(new RealmConfig("", settings), mock(DnRoleMapper.class));
            fail("exception should have been thrown");
        } catch (ShieldSettingsException e) {
            assertThat(e.getMessage(), containsString("no truststore password configured"));
        }
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
