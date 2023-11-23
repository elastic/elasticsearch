/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
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

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class PkiAuthDelegationIntegTests extends SecurityIntegTestCase {

    private static final ParseField AUTHENTICATION_FIELD = new ParseField("authentication");

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
        return Strings.format("""
            %s
            role_manage:
              cluster: [ manage ]

            role_manage_security:
              cluster: [ manage_security ]

            role_delegate_pki:
              cluster: [ delegate_pki ]

            role_all:
              cluster: [ all ]
            """, super.configRoles());
    }

    @Override
    protected String configUsersRoles() {
        return Strings.format("""
            %s
            role_manage:user_manage
            role_manage_security:user_manage_security
            role_delegate_pki:user_delegate_pki
            role_all:user_all
            kibana_system:my_kibana_system
            """, super.configUsersRoles());
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
        final List<X509Certificate> certificateChain;
        // trust root is optional
        if (randomBoolean()) {
            certificateChain = List.of(clientCertificate, intermediateCA);
        } else {
            certificateChain = List.of(clientCertificate, intermediateCA, rootCA);
        }

        for (String delegateeUsername : Arrays.asList("user_all", "user_delegate_pki", "my_kibana_system")) {
            // delegate
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader(
                "Authorization",
                basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            );
            var delegatePkiResponse = getSecurityClient(optionsBuilder.build()).delegatePkiAuthentication(certificateChain);
            String token = delegatePkiResponse.v1();
            assertThat(token, is(notNullValue()));
            assertNotNull(delegatePkiResponse.v2());
            final Map<String, Object> authentication = assertMap(delegatePkiResponse.v2(), AUTHENTICATION_FIELD);
            assertThat(authentication, hasEntry(Fields.USERNAME.getPreferredName(), "Elasticsearch Test Client"));

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

    public void testTokenInvalidate() throws Exception {
        final X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        final X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        final X509Certificate rootCA = readCertForPkiDelegation("testRootCA.crt");
        final List<X509Certificate> certificateChain;
        // trust root is optional
        if (randomBoolean()) {
            certificateChain = List.of(clientCertificate, intermediateCA);
        } else {
            certificateChain = List.of(clientCertificate, intermediateCA, rootCA);
        }
        String delegateeUsername = randomFrom("user_all", "user_delegate_pki", "my_kibana_system");
        // delegate
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader(
            "Authorization",
            basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        var delegatePkiResponse = getSecurityClient(optionsBuilder.build()).delegatePkiAuthentication(certificateChain);
        String token = delegatePkiResponse.v1();
        assertThat(token, is(notNullValue()));
        assertNotNull(delegatePkiResponse.v2());
        final Map<String, Object> authentication = assertMap(delegatePkiResponse.v2(), AUTHENTICATION_FIELD);
        assertThat(authentication, hasEntry(Fields.USERNAME.getPreferredName(), "Elasticsearch Test Client"));

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
        optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader(
            "Authorization",
            basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );

        var invalidateResponse = getSecurityClient(optionsBuilder.build()).invalidateAccessToken(token);
        assertThat(invalidateResponse.invalidated(), is(1));
        assertThat(invalidateResponse.errors(), hasSize(0));

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

    public void testDelegateUnauthorized() throws Exception {
        final X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        final X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        final X509Certificate rootCA = readCertForPkiDelegation("testRootCA.crt");
        final List<X509Certificate> certificateChain;
        // trust root is optional
        if (randomBoolean()) {
            certificateChain = List.of(clientCertificate, intermediateCA);
        } else {
            certificateChain = List.of(clientCertificate, intermediateCA, rootCA);
        }
        for (String delegateeUsername : Arrays.asList("user_manage", "user_manage_security")) {
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader(
                "Authorization",
                basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
            );
            ResponseException e = expectThrows(
                ResponseException.class,
                () -> getSecurityClient(optionsBuilder.build()).delegatePkiAuthentication(certificateChain)
            );
            assertThat(
                e,
                throwableWithMessage(containsString("action [cluster:admin/xpack/security/delegate_pki] is unauthorized for user"))
            );
        }
    }

    public void testDelegatePkiWithRoleMapping() throws Exception {
        X509Certificate clientCertificate = readCertForPkiDelegation("testClient.crt");
        X509Certificate intermediateCA = readCertForPkiDelegation("testIntermediateCA.crt");
        X509Certificate rootCA = readCertForPkiDelegation("testRootCA.crt");
        final List<X509Certificate> certificateChain;
        // trust root is optional
        if (randomBoolean()) {
            certificateChain = List.of(clientCertificate, intermediateCA);
        } else {
            certificateChain = List.of(clientCertificate, intermediateCA, rootCA);
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

        // delegate
        var delegatePkiResponse = getSecurityClient(testUserOptions).delegatePkiAuthentication(certificateChain);
        // authenticate
        TestSecurityClient accessTokenClient = getSecurityClient(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + delegatePkiResponse.v1()).build()
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
        // incomplete cert chain
        final List<X509Certificate> certificateChain1 = List.of(clientCertificate);
        ResponseException e1 = expectThrows(
            ResponseException.class,
            () -> getSecurityClient(optionsBuilder.build()).delegatePkiAuthentication(certificateChain1)
        );
        assertThat(
            e1,
            throwableWithMessage(
                containsString(
                    "unable to authenticate user [O=org, OU=Elasticsearch, CN=Elasticsearch Test Client]"
                        + " for action [cluster:admin/xpack/security/delegate_pki]"
                )
            )
        );
        // swapped order
        final List<X509Certificate> certificateChain2 = Arrays.asList(intermediateCA, clientCertificate);
        ResponseException e2 = expectThrows(
            ResponseException.class,
            () -> getSecurityClient(optionsBuilder.build()).delegatePkiAuthentication(certificateChain2)
        );
        assertThat(e2, throwableWithMessage(containsString("Validation Failed: 1: certificates chain must be an ordered chain;")));
        // bogus certificate
        final List<X509Certificate> certificateChain3 = Arrays.asList(bogusCertificate);
        ResponseException e3 = expectThrows(
            ResponseException.class,
            () -> getSecurityClient(optionsBuilder.build()).delegatePkiAuthentication(certificateChain3)
        );
        assertThat(e3, throwableWithMessage(containsString("security_exception")));
        assertThat(e3, throwableWithMessage(containsString("unable to authenticate user")));
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
