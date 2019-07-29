/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.AuthenticateResponse.RealmInfo;
import org.elasticsearch.client.security.DeleteRoleMappingRequest;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse;
import org.elasticsearch.xpack.security.action.TransportDelegatePkiAuthenticationAction;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class PkiAuthDelegationIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
                .put("xpack.security.authc.realms.pki.pki1.order", "1")
                .putList("xpack.security.authc.realms.pki.pki1.certificate_authorities",
                    getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString())
                .put("xpack.security.authc.realms.pki.pki1.delegation.enabled", true)
                .build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testDelegatePki() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"));
        DelegatePkiAuthenticationRequest delegatePkiRequest = new DelegatePkiAuthenticationRequest(new X509Certificate[] { certificate });
        PlainActionFuture<DelegatePkiAuthenticationResponse> future = new PlainActionFuture<>();
        client().execute(TransportDelegatePkiAuthenticationAction.TYPE, delegatePkiRequest, future);
        String token = future.get().getTokenString();
        assertThat(token, is(notNullValue()));
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", "Bearer " + token);
        RequestOptions options = optionsBuilder.build();
        try(RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            AuthenticateResponse resp = restClient.security().authenticate(options);
            User user = resp.getUser();
            assertThat(user, is(notNullValue()));
            assertThat(user.getUsername(), is("Elasticsearch Test Client"));
            assertThat(user.getMetadata().get("pki_dn"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_dn"), is("CN=Elasticsearch Test Client, OU=elasticsearch, O=org"));
            assertThat(user.getMetadata().get("pki_delegated_from_user"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_delegated_from_user"), is("test_user"));
            assertThat(user.getMetadata().get("pki_delegated_from_realm"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_delegated_from_realm"), is("file"));
            // no roles because no role mappings
            assertThat(user.getRoles(), is(emptyCollectionOf(String.class)));
            RealmInfo authnRealm = resp.getAuthenticationRealm();
            assertThat(authnRealm, is(notNullValue()));
            assertThat(authnRealm.getName(), is("pki1"));
            assertThat(authnRealm.getType(), is("pki"));
        }
    }

    public void testDelegatePkiWithRoleMapping() throws Exception {
        final RequestOptions testUserOptions = RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())))
                .build();
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            // put role mappings for delegated PKI
            PutRoleMappingRequest request = new PutRoleMappingRequest("role_from_delegated_user", true,
                    Collections.singletonList("role_from_delegated_user"), Collections.emptyList(),
                    new FieldRoleMapperExpression("metadata.pki_delegated_from_user", "test_user"), null, RefreshPolicy.IMMEDIATE);
            restClient.security().putRoleMapping(request, testUserOptions);
            request = new PutRoleMappingRequest("role_from_delegated_realm", true, Collections.singletonList("role_from_delegated_realm"),
                    Collections.emptyList(), new FieldRoleMapperExpression("metadata.pki_delegated_from_realm", "file"), null,
                    RefreshPolicy.IMMEDIATE);
            restClient.security().putRoleMapping(request, testUserOptions);

            X509Certificate cert = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"));
            DelegatePkiAuthenticationRequest delegatePkiRequest = new DelegatePkiAuthenticationRequest(new X509Certificate[] { cert });
            PlainActionFuture<DelegatePkiAuthenticationResponse> future = new PlainActionFuture<>();
            client().execute(TransportDelegatePkiAuthenticationAction.TYPE, delegatePkiRequest, future);
            String token = future.get().getTokenString();
            assertThat(token, is(notNullValue()));
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization", "Bearer " + token);
            RequestOptions options = optionsBuilder.build();
            AuthenticateResponse resp = restClient.security().authenticate(options);
            User user = resp.getUser();
            assertThat(user, is(notNullValue()));
            assertThat(user.getUsername(), is("Elasticsearch Test Client"));
            assertThat(user.getMetadata().get("pki_dn"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_dn"), is("CN=Elasticsearch Test Client, OU=elasticsearch, O=org"));
            assertThat(user.getMetadata().get("pki_delegated_from_user"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_delegated_from_user"), is("test_user"));
            assertThat(user.getMetadata().get("pki_delegated_from_realm"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_delegated_from_realm"), is("file"));
            // assert roles
            assertThat(user.getRoles(), containsInAnyOrder("role_from_delegated_user", "role_from_delegated_realm"));
            RealmInfo authnRealm = resp.getAuthenticationRealm();
            assertThat(authnRealm, is(notNullValue()));
            assertThat(authnRealm.getName(), is("pki1"));
            assertThat(authnRealm.getType(), is("pki"));

            // delete role mappings for delegated PKI
            restClient.security().deleteRoleMapping(new DeleteRoleMappingRequest("role_from_delegated_user", RefreshPolicy.IMMEDIATE),
                  testUserOptions);
            restClient.security().deleteRoleMapping(new DeleteRoleMappingRequest("role_from_delegated_realm", RefreshPolicy.IMMEDIATE),
                  testUserOptions);
        }
    }

    static X509Certificate readCert(Path path) throws Exception {
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
