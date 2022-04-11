/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;
import org.elasticsearch.client.security.DelegatePkiAuthenticationResponse;
import org.elasticsearch.client.security.InvalidateTokenRequest;
import org.elasticsearch.client.security.InvalidateTokenResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.User.Fields;
import org.junit.Before;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

@SuppressWarnings("removal")
public class PkiAuthDelegationIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
            // pki1 does not allow delegation
            .put("xpack.security.authc.realms.pki.pki1.order", "2")
            .putList(
                "xpack.security.authc.realms.pki.pki1.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString()
            )
            .put("xpack.security.authc.realms.pki.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
            // pki2 allows delegation but has a non-matching username pattern
            .put("xpack.security.authc.realms.pki.pki2.order", "3")
            .putList(
                "xpack.security.authc.realms.pki.pki2.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString()
            )
            .put("xpack.security.authc.realms.pki.pki2.username_pattern", "CN=MISMATCH(.*?)(?:,|$)")
            .put("xpack.security.authc.realms.pki.pki2.delegation.enabled", true)
            .put("xpack.security.authc.realms.pki.pki2.files.role_mapping", getDataPath("role_mapping.yml"))
            // pki3 allows delegation and the username pattern (default) matches
            .put("xpack.security.authc.realms.pki.pki3.order", "4")
            .putList(
                "xpack.security.authc.realms.pki.pki3.certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString()
            )
            .put("xpack.security.authc.realms.pki.pki3.delegation.enabled", true)
            .put("xpack.security.authc.realms.pki.pki3.files.role_mapping", getDataPath("role_mapping.yml"))
            .build();
    }

    @Override
    protected String configUsers() {
        final Hasher passwdHasher = getFastStoredHashAlgoForTests();
        final String usersPasswdHashed = new String(passwdHasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        return super.configUsers()
            + "user_manage:"
            + usersPasswdHashed
            + "\n"
            + "user_manage_security:"
            + usersPasswdHashed
            + "\n"
            + "user_delegate_pki:"
            + usersPasswdHashed
            + "\n"
            + "user_all:"
            + usersPasswdHashed
            + "\n"
            + "my_kibana_system:"
            + usersPasswdHashed
            + "\n";
    }

    @Override
    protected String configRoles() {
        return """
            %s
            role_manage:
              cluster: [ manage ]

            role_manage_security:
              cluster: [ manage_security ]

            role_delegate_pki:
              cluster: [ delegate_pki ]

            role_all:
              cluster: [ all ]
            """.formatted(super.configRoles());
    }

    @Override
    protected String configUsersRoles() {
        return """
            %s
            role_manage:user_manage
            role_manage_security:user_manage_security
            role_delegate_pki:user_delegate_pki
            role_all:user_all
            kibana_system:my_kibana_system
            """.formatted(super.configUsersRoles());
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
            for (String delegateeUsername : Arrays.asList("user_all", "user_delegate_pki", "my_kibana_system")) {
                // delegate
                RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
                optionsBuilder.addHeader(
                    "Authorization",
                    basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
                );
                DelegatePkiAuthenticationResponse delegatePkiResponse = restClient.security()
                    .delegatePkiAuthentication(delegatePkiRequest, optionsBuilder.build());
                String token = delegatePkiResponse.getAccessToken();
                assertThat(token, is(notNullValue()));
                assertNotNull(delegatePkiResponse.getAuthentication());
                assertEquals("Elasticsearch Test Client", delegatePkiResponse.getAuthentication().getUser().getUsername());

                // authenticate
                optionsBuilder = RequestOptions.DEFAULT.toBuilder();
                optionsBuilder.addHeader("Authorization", "Bearer " + token);

                final TestSecurityClient securityClient = getSecurityClient(optionsBuilder.build());
                final Map<String, Object> authenticateResponse = securityClient.authenticate();
                assertThat(authenticateResponse, hasEntry(Fields.USERNAME.getPreferredName(), "Elasticsearch Test Client"));

                Map<String, Object> realm = assertMap(authenticateResponse, Fields.AUTHENTICATION_REALM);
                assertThat(realm, hasEntry(Fields.REALM_NAME.getPreferredName(), "pki3"));
                assertThat(realm, hasEntry(Fields.REALM_TYPE.getPreferredName(), "pki"));

                assertThat(authenticateResponse, hasEntry(Fields.AUTHENTICATION_TYPE.getPreferredName(), "token"));
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
            String delegateeUsername = randomFrom("user_all", "user_delegate_pki", "my_kibana_system");
            // delegate
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader(
                "Authorization",
                basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            );
            DelegatePkiAuthenticationResponse delegatePkiResponse = restClient.security()
                .delegatePkiAuthentication(delegatePkiRequest, optionsBuilder.build());
            String token = delegatePkiResponse.getAccessToken();
            assertThat(token, is(notNullValue()));
            assertNotNull(delegatePkiResponse.getAuthentication());
            // authenticate
            optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization", "Bearer " + token);
            final TestSecurityClient securityClient = getSecurityClient(optionsBuilder.build());
            final Map<String, Object> authenticateResponse = securityClient.authenticate();
            assertThat(authenticateResponse, hasEntry(Fields.USERNAME.getPreferredName(), "Elasticsearch Test Client"));

            final Map<String, Object> metadata = assertMap(authenticateResponse, Fields.METADATA);
            assertThat(metadata, hasEntry("pki_dn", "O=org, OU=Elasticsearch, CN=Elasticsearch Test Client"));
            assertThat(metadata, hasEntry("pki_delegated_by_user", delegateeUsername));
            assertThat(metadata, hasEntry("pki_delegated_by_realm", "file"));

            // no roles because no role mappings
            List<?> roles = assertList(authenticateResponse, Fields.ROLES);
            assertThat(roles, empty());

            Map<String, Object> realm = assertMap(authenticateResponse, Fields.AUTHENTICATION_REALM);
            assertThat(realm, hasEntry(Fields.REALM_NAME.getPreferredName(), "pki3"));
            assertThat(realm, hasEntry(Fields.REALM_TYPE.getPreferredName(), "pki"));

            assertThat(authenticateResponse, hasEntry(Fields.AUTHENTICATION_TYPE.getPreferredName(), "token"));

            // invalidate
            InvalidateTokenRequest invalidateRequest = InvalidateTokenRequest.accessToken(token);
            optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader(
                "Authorization",
                basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            );
            InvalidateTokenResponse invalidateResponse = restClient.security().invalidateToken(invalidateRequest, optionsBuilder.build());
            assertThat(invalidateResponse.getInvalidatedTokens(), is(1));
            assertThat(invalidateResponse.getErrorsCount(), is(0));
            // failed authenticate
            ResponseException ex = expectThrows(
                ResponseException.class,
                () -> new TestSecurityClient(
                    getRestClient(),
                    RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + token).build()
                ).authenticate()
            );

            assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(RestStatus.UNAUTHORIZED.getStatus()));

            final Map<String, Object> response = ESRestTestCase.entityAsMap(ex.getResponse());
            assertThat(ObjectPath.eval("error.type", response), is("security_exception"));
            assertThat(ObjectPath.eval("error.reason", response), is("token expired"));
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
                optionsBuilder.addHeader(
                    "Authorization",
                    basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
                );
                ElasticsearchStatusException e = expectThrows(
                    ElasticsearchStatusException.class,
                    () -> { restClient.security().delegatePkiAuthentication(delegatePkiRequest, optionsBuilder.build()); }
                );
                assertThat(
                    e.getMessage(),
                    startsWith(
                        "Elasticsearch exception [type=security_exception, reason=action"
                            + " [cluster:admin/xpack/security/delegate_pki] is unauthorized for user"
                    )
                );
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
            .addHeader(
                "Authorization",
                basicAuthHeaderValue(
                    SecuritySettingsSource.TEST_USER_NAME,
                    new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
                )
            )
            .build();
        final TestSecurityClient securityClient = getSecurityClient(testUserOptions);
        // put role mappings for delegated PKI
        String roleMapping = """
            {
                "enabled": true,
                "roles": [ "role_by_delegated_user" ],
                "rules": {
                  "field": { "metadata.pki_delegated_by_user": "test_user" }
                }
            }
            """;
        securityClient.putRoleMapping("role_by_delegated_user", roleMapping);

        roleMapping = """
            {
                "enabled": true,
                "roles": [ "role_by_delegated_realm" ],
                "rules": {
                  "field": { "metadata.pki_delegated_by_realm": "file" }
                }
            }
            """;
        securityClient.putRoleMapping("role_by_delegated_realm", roleMapping);

        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            // delegate
            DelegatePkiAuthenticationResponse delegatePkiResponse = restClient.security()
                .delegatePkiAuthentication(delegatePkiRequest, testUserOptions);
            // authenticate
            TestSecurityClient accessTokenClient = getSecurityClient(
                RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + delegatePkiResponse.getAccessToken()).build()
            );
            final Map<String, Object> authenticateResponse = accessTokenClient.authenticate();
            assertThat(authenticateResponse, hasEntry(Fields.USERNAME.getPreferredName(), "Elasticsearch Test Client"));

            final Map<String, Object> metadata = assertMap(authenticateResponse, Fields.METADATA);
            assertThat(metadata, hasEntry("pki_dn", "O=org, OU=Elasticsearch, CN=Elasticsearch Test Client"));
            assertThat(metadata, hasEntry("pki_delegated_by_user", "test_user"));
            assertThat(metadata, hasEntry("pki_delegated_by_realm", "file"));

            // assert roles
            List<?> roles = assertList(authenticateResponse, Fields.ROLES);
            assertThat(roles, containsInAnyOrder("role_by_delegated_user", "role_by_delegated_realm"));

            Map<String, Object> realm = assertMap(authenticateResponse, Fields.AUTHENTICATION_REALM);
            assertThat(realm, hasEntry(Fields.REALM_NAME.getPreferredName(), "pki3"));
            assertThat(realm, hasEntry(Fields.REALM_TYPE.getPreferredName(), "pki"));

            assertThat(authenticateResponse, hasEntry(Fields.AUTHENTICATION_TYPE.getPreferredName(), "token"));
        }

        // delete role mappings for delegated PKI
        securityClient.deleteRoleMapping("role_by_delegated_user");
        securityClient.deleteRoleMapping("role_by_delegated_realm");
    }

    public void testIncorrectCertChain() throws Exception {
        X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        X509Certificate bogusCertificate = readCertForPkiDelegation("bogus.crt");
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader(
            "Authorization",
            basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            // incomplete cert chain
            DelegatePkiAuthenticationRequest delegatePkiRequest1 = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate));
            ElasticsearchStatusException e1 = expectThrows(
                ElasticsearchStatusException.class,
                () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest1, optionsBuilder.build())
            );
            assertThat(
                e1.getMessage(),
                is(
                    "Elasticsearch exception [type=security_exception, reason=unable to authenticate user"
                        + " [O=org, OU=Elasticsearch, CN=Elasticsearch Test Client] for action [cluster:admin/xpack/security/delegate_pki]]"
                )
            );
            // swapped order
            DelegatePkiAuthenticationRequest delegatePkiRequest2 = new DelegatePkiAuthenticationRequest(
                Arrays.asList(intermediateCA, clientCertificate)
            );
            ValidationException e2 = expectThrows(
                ValidationException.class,
                () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest2, optionsBuilder.build())
            );
            assertThat(e2.getMessage(), is("Validation Failed: 1: certificates chain must be an ordered chain;"));
            // bogus certificate
            DelegatePkiAuthenticationRequest delegatePkiRequest3 = new DelegatePkiAuthenticationRequest(Arrays.asList(bogusCertificate));
            ElasticsearchStatusException e3 = expectThrows(
                ElasticsearchStatusException.class,
                () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest3, optionsBuilder.build())
            );
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> assertMap(Map<String, Object> map, ParseField field) {
        final Object val = map.get(field.getPreferredName());
        assertThat("Field " + field + " of " + map, val, instanceOf(Map.class));
        return (Map<String, Object>) val;
    }

    private List<?> assertList(Map<String, Object> map, ParseField field) {
        final Object val = map.get(field.getPreferredName());
        assertThat("Field " + field + " of " + map, val, instanceOf(List.class));
        return (List<?>) val;
    }
}
