/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.AuthenticateResponse.RealmInfo;
import org.elasticsearch.client.security.DeleteRoleMappingRequest;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;
import org.elasticsearch.client.security.DelegatePkiAuthenticationResponse;
import org.elasticsearch.client.security.InvalidateTokenRequest;
import org.elasticsearch.client.security.InvalidateTokenResponse;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.junit.Before;
import org.elasticsearch.test.SecuritySettingsSource;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Arrays;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.startsWith;

public class PkiAuthDelegationIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
                // pki1 does not allow delegation
                .put("xpack.security.authc.realms.pki.pki1.order", "2")
                .putList("xpack.security.authc.realms.pki.pki1.certificate_authorities",
                    getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString())
                .put("xpack.security.authc.realms.pki.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
                // pki2 allows delegation but has a non-matching username pattern
                .put("xpack.security.authc.realms.pki.pki2.order", "3")
                .putList("xpack.security.authc.realms.pki.pki2.certificate_authorities",
                    getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString())
                .put("xpack.security.authc.realms.pki.pki2.username_pattern", "CN=MISMATCH(.*?)(?:,|$)")
                .put("xpack.security.authc.realms.pki.pki2.delegation.enabled", true)
                .put("xpack.security.authc.realms.pki.pki2.files.role_mapping", getDataPath("role_mapping.yml"))
                // pki3 allows delegation and the username pattern (default) matches
                .put("xpack.security.authc.realms.pki.pki3.order", "4")
                .putList("xpack.security.authc.realms.pki.pki3.certificate_authorities",
                    getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString())
                .put("xpack.security.authc.realms.pki.pki3.delegation.enabled", true)
                .put("xpack.security.authc.realms.pki.pki3.files.role_mapping", getDataPath("role_mapping.yml"))
                .build();
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(Hasher.resolve(
            randomFrom("pbkdf2", "pbkdf2_1000", "bcrypt", "bcrypt9")).hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        return super.configUsers() +
            "user_manage:" + usersPasswdHashed + "\n" +
            "user_manage_security:" + usersPasswdHashed + "\n" +
            "user_delegate_pki:" + usersPasswdHashed + "\n" +
            "user_all:" + usersPasswdHashed + "\n" +
            "kibana_system:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" +
                "role_manage:\n" +
                "  cluster: [ manage ]\n" +
                "\n" +
                "role_manage_security:\n" +
                "  cluster: [ manage_security ]\n" +
                "\n" +
                "role_delegate_pki:\n" +
                "  cluster: [ delegate_pki ]\n" +
                "\n" +
                "role_all:\n" +
                "  cluster: [ all ]\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "\n" +
                "role_manage:user_manage\n" +
                "role_manage_security:user_manage_security\n" +
                "role_delegate_pki:user_delegate_pki\n" +
                "role_all:user_all\n" +
                "kibana_system:kibana_system\n";
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Before
    void clearRealmCache() {
        new ClearRealmCacheRequestBuilder(client()).get();
    }

    public void testDelegateThenAuthenticate() throws Exception {
        final X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        final X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        final X509Certificate rootCA = readCertForPkiDelegation("testRootCA.crt");
        DelegatePkiAuthenticationRequest delegatePkiRequest;
        // trust root is optional
        if (randomBoolean()) {
            delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA));
        } else {
            delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA, rootCA));
        }

        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            for (String delegateeUsername : Arrays.asList("user_all", "user_delegate_pki", "kibana_system")) {
                // delegate
                RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
                optionsBuilder.addHeader("Authorization",
                        basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
                DelegatePkiAuthenticationResponse delegatePkiResponse = restClient.security().delegatePkiAuthentication(delegatePkiRequest,
                        optionsBuilder.build());
                String token = delegatePkiResponse.getAccessToken();
                assertThat(token, is(notNullValue()));
                // authenticate
                optionsBuilder = RequestOptions.DEFAULT.toBuilder();
                optionsBuilder.addHeader("Authorization", "Bearer " + token);
                AuthenticateResponse resp = restClient.security().authenticate(optionsBuilder.build());
                User user = resp.getUser();
                assertThat(user, is(notNullValue()));
                assertThat(user.getUsername(), is("Elasticsearch Test Client"));
                RealmInfo authnRealm = resp.getAuthenticationRealm();
                assertThat(authnRealm, is(notNullValue()));
                assertThat(authnRealm.getName(), is("pki3"));
                assertThat(authnRealm.getType(), is("pki"));
            }
        }
    }

    public void testTokenInvalidate() throws Exception {
        final X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        final X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        final X509Certificate rootCA = readCertForPkiDelegation("testRootCA.crt");
        DelegatePkiAuthenticationRequest delegatePkiRequest;
        // trust root is optional
        if (randomBoolean()) {
            delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA));
        } else {
            delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA, rootCA));
        }

        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            String delegateeUsername = randomFrom("user_all", "user_delegate_pki", "kibana_system");
            // delegate
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization",
                    basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
            DelegatePkiAuthenticationResponse delegatePkiResponse = restClient.security().delegatePkiAuthentication(delegatePkiRequest,
                    optionsBuilder.build());
            String token = delegatePkiResponse.getAccessToken();
            assertThat(token, is(notNullValue()));
            // authenticate
            optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization", "Bearer " + token);
            AuthenticateResponse resp = restClient.security().authenticate(optionsBuilder.build());
            User user = resp.getUser();
            assertThat(user, is(notNullValue()));
            assertThat(user.getUsername(), is("Elasticsearch Test Client"));
            assertThat(user.getMetadata().get("pki_dn"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_dn"), is("O=org, OU=Elasticsearch, CN=Elasticsearch Test Client"));
            assertThat(user.getMetadata().get("pki_delegated_by_user"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_delegated_by_user"), is(delegateeUsername));
            assertThat(user.getMetadata().get("pki_delegated_by_realm"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_delegated_by_realm"), is("file"));
            // no roles because no role mappings
            assertThat(user.getRoles(), is(emptyCollectionOf(String.class)));
            RealmInfo authnRealm = resp.getAuthenticationRealm();
            assertThat(authnRealm, is(notNullValue()));
            assertThat(authnRealm.getName(), is("pki3"));
            assertThat(authnRealm.getType(), is("pki"));
            // invalidate
            InvalidateTokenRequest invalidateRequest = new InvalidateTokenRequest(token, null, null, null);
            optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization",
                    basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
            InvalidateTokenResponse invalidateResponse = restClient.security().invalidateToken(invalidateRequest, optionsBuilder.build());
            assertThat(invalidateResponse.getInvalidatedTokens(), is(1));
            assertThat(invalidateResponse.getErrorsCount(), is(0));
            // failed authenticate
            ElasticsearchStatusException e1 = expectThrows(ElasticsearchStatusException.class, () -> restClient.security()
                    .authenticate(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + token).build()));
            assertThat(e1.getMessage(), is("Elasticsearch exception [type=security_exception, reason=token expired]"));
        }
    }

    public void testDelegateUnauthorized() throws Exception {
        final X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        final X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        final X509Certificate rootCA = readCertForPkiDelegation("testRootCA.crt");
        DelegatePkiAuthenticationRequest delegatePkiRequest;
        // trust root is optional
        if (randomBoolean()) {
            delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA));
        } else {
            delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA, rootCA));
        }
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            for (String delegateeUsername : Arrays.asList("user_manage", "user_manage_security")) {
                RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
                optionsBuilder.addHeader("Authorization",
                        basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
                ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> {
                    restClient.security().delegatePkiAuthentication(delegatePkiRequest, optionsBuilder.build());
                });
                assertThat(e.getMessage(), startsWith("Elasticsearch exception [type=security_exception, reason=action"
                        + " [cluster:admin/xpack/security/delegate_pki] is unauthorized for user"));
            }
        }
    }

    public void testDelegatePkiWithRoleMapping() throws Exception {
        X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        X509Certificate rootCA = readCertForPkiDelegation("testRootCA.crt");
        DelegatePkiAuthenticationRequest delegatePkiRequest;
        // trust root is optional
        if (randomBoolean()) {
            delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA));
        } else {
            delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA, rootCA));
        }
        final RequestOptions testUserOptions = RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())))
                .build();
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            // put role mappings for delegated PKI
            PutRoleMappingRequest request = new PutRoleMappingRequest("role_by_delegated_user", true,
                    Collections.singletonList("role_by_delegated_user"), Collections.emptyList(),
                    new FieldRoleMapperExpression("metadata.pki_delegated_by_user", "test_user"), null, RefreshPolicy.IMMEDIATE);
            restClient.security().putRoleMapping(request, testUserOptions);
            request = new PutRoleMappingRequest("role_by_delegated_realm", true, Collections.singletonList("role_by_delegated_realm"),
                    Collections.emptyList(), new FieldRoleMapperExpression("metadata.pki_delegated_by_realm", "file"), null,
                    RefreshPolicy.IMMEDIATE);
            restClient.security().putRoleMapping(request, testUserOptions);
            // delegate
            DelegatePkiAuthenticationResponse delegatePkiResponse = restClient.security().delegatePkiAuthentication(delegatePkiRequest,
                    testUserOptions);
            // authenticate
            AuthenticateResponse resp = restClient.security().authenticate(RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Authorization", "Bearer " + delegatePkiResponse.getAccessToken()).build());
            User user = resp.getUser();
            assertThat(user, is(notNullValue()));
            assertThat(user.getUsername(), is("Elasticsearch Test Client"));
            assertThat(user.getMetadata().get("pki_dn"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_dn"), is("O=org, OU=Elasticsearch, CN=Elasticsearch Test Client"));
            assertThat(user.getMetadata().get("pki_delegated_by_user"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_delegated_by_user"), is("test_user"));
            assertThat(user.getMetadata().get("pki_delegated_by_realm"), is(notNullValue()));
            assertThat(user.getMetadata().get("pki_delegated_by_realm"), is("file"));
            // assert roles
            assertThat(user.getRoles(), containsInAnyOrder("role_by_delegated_user", "role_by_delegated_realm"));
            RealmInfo authnRealm = resp.getAuthenticationRealm();
            assertThat(authnRealm, is(notNullValue()));
            assertThat(authnRealm.getName(), is("pki3"));
            assertThat(authnRealm.getType(), is("pki"));
            // delete role mappings for delegated PKI
            restClient.security().deleteRoleMapping(new DeleteRoleMappingRequest("role_by_delegated_user", RefreshPolicy.IMMEDIATE),
                  testUserOptions);
            restClient.security().deleteRoleMapping(new DeleteRoleMappingRequest("role_by_delegated_realm", RefreshPolicy.IMMEDIATE),
                  testUserOptions);
        }
    }

    public void testIncorrectCertChain() throws Exception {
        X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        X509Certificate bogusCertificate = readCertForPkiDelegation("bogus.crt");
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            // incomplete cert chain
            DelegatePkiAuthenticationRequest delegatePkiRequest1 = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate));
            ElasticsearchStatusException e1 = expectThrows(ElasticsearchStatusException.class,
                    () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest1, optionsBuilder.build()));
            assertThat(e1.getMessage(), is("Elasticsearch exception [type=security_exception, reason=unable to authenticate user"
                    + " [O=org, OU=Elasticsearch, CN=Elasticsearch Test Client] for action [cluster:admin/xpack/security/delegate_pki]]"));
            // swapped order
            DelegatePkiAuthenticationRequest delegatePkiRequest2 = new DelegatePkiAuthenticationRequest(
                    Arrays.asList(intermediateCA, clientCertificate));
            ValidationException e2 = expectThrows(ValidationException.class,
                    () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest2, optionsBuilder.build()));
            assertThat(e2.getMessage(), is("Validation Failed: 1: certificates chain must be an ordered chain;"));
            // bogus certificate
            DelegatePkiAuthenticationRequest delegatePkiRequest3 = new DelegatePkiAuthenticationRequest(Arrays.asList(bogusCertificate));
            ElasticsearchStatusException e3 = expectThrows(ElasticsearchStatusException.class,
                    () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest3, optionsBuilder.build()));
            assertThat(e3.getMessage(), startsWith("Elasticsearch exception [type=security_exception, reason=unable to authenticate user"));
        }
    }

    private X509Certificate readCertForPkiDelegation(String certName) throws Exception {
        Path path = getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/" + certName);
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }

}
