/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public abstract class SecurityRealmSmokeTestCase extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .configFile("http-server.key", Resource.fromClasspath("ssl/http-server.key"))
        .configFile("http-server.crt", Resource.fromClasspath("ssl/http-server.crt"))
        .configFile("http-client-ca.crt", Resource.fromClasspath("ssl/http-client-ca.crt"))
        .configFile("saml-metadata.xml", Resource.fromClasspath("saml-metadata.xml"))
        .configFile("kerberos.keytab", Resource.fromClasspath("kerberos.keytab"))
        .configFile("oidc-jwkset.json", Resource.fromClasspath("oidc-jwkset.json"))
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        // Need a trial license (not basic) to enable all realms
        .setting("xpack.license.self_generated.type", "trial")
        // Need SSL to enable PKI realms
        .setting("xpack.security.http.ssl.enabled", "true")
        .setting("xpack.security.http.ssl.certificate", "http-server.crt")
        .setting("xpack.security.http.ssl.key", "http-server.key")
        .setting("xpack.security.http.ssl.key_passphrase", "http-password")
        .setting("xpack.security.http.ssl.client_authentication", "optional")
        .setting("xpack.security.http.ssl.certificate_authorities", "http-client-ca.crt")
        // Don't need transport SSL, so leave it out
        .setting("xpack.security.transport.ssl.enabled", "false")
        // Configure every realm type
        // - File
        .setting("xpack.security.authc.realms.file.file0.order", "0")
        // - Native
        .setting("xpack.security.authc.realms.native.native1.order", "1")
        // - LDAP (configured but won't work because we don't want external fixtures in this test suite)
        .setting("xpack.security.authc.realms.ldap.ldap2.order", "2")
        .setting("xpack.security.authc.realms.ldap.ldap2.url", "ldap://localhost:7777")
        .setting("xpack.security.authc.realms.ldap.ldap2.user_search.base_dn", "OU=users,DC=example,DC=com")
        // - AD (configured but won't work because we don't want external fixtures in this test suite)
        .setting("xpack.security.authc.realms.active_directory.ad3.order", "3")
        .setting("xpack.security.authc.realms.active_directory.ad3.domain_name", "localhost")
        // - PKI (works)
        .setting("xpack.security.authc.realms.pki.pki4.order", "4")
        // - SAML (configured but won't work because we don't want external fixtures in this test suite)
        .setting("xpack.security.authc.realms.saml.saml5.order", "5")
        .setting("xpack.security.authc.realms.saml.saml5.idp.metadata.path", "saml-metadata.xml")
        .setting("xpack.security.authc.realms.saml.saml5.idp.entity_id", "http://idp.example.com/")
        .setting("xpack.security.authc.realms.saml.saml5.sp.entity_id", "http://kibana.example.net/")
        .setting("xpack.security.authc.realms.saml.saml5.sp.acs", "http://kibana.example.net/api/security/saml/callback")
        .setting("xpack.security.authc.realms.saml.saml5.attributes.principal", "uid")
        // - Kerberos (configured but won't work because we don't want external fixtures in this test suite)
        .setting("xpack.security.authc.realms.kerberos.kerb6.order", "6")
        .setting("xpack.security.authc.realms.kerberos.kerb6.keytab.path", "kerberos.keytab")
        // - OIDC (configured but won't work because we don't want external fixtures in this test suite)
        .setting("xpack.security.authc.realms.oidc.openid7.order", "7")
        .setting("xpack.security.authc.realms.oidc.openid7.rp.client_id", "http://rp.example.net")
        .setting("xpack.security.authc.realms.oidc.openid7.rp.response_type", "id_token")
        .setting("xpack.security.authc.realms.oidc.openid7.rp.redirect_uri", "https://kibana.example.net/api/security/v1/oidc")
        .setting("xpack.security.authc.realms.oidc.openid7.op.issuer", "https://op.example.com/")
        .setting("xpack.security.authc.realms.oidc.openid7.op.authorization_endpoint", "https://op.example.com/auth")
        .setting("xpack.security.authc.realms.oidc.openid7.op.jwkset_path", "oidc-jwkset.json")
        .setting("xpack.security.authc.realms.oidc.openid7.claims.principal", "sub")
        .setting("xpack.security.authc.realms.oidc.openid7.http.connection_pool_ttl", "1m")
        .keystore("xpack.security.authc.realms.oidc.openid7.rp.client_secret", "this-is-my-secret")
        // - JWT (works)
        .setting("xpack.security.authc.realms.jwt.jwt8.order", "8")
        .setting("xpack.security.authc.realms.jwt.jwt8.allowed_issuer", "iss8")
        .setting("xpack.security.authc.realms.jwt.jwt8.allowed_signature_algorithms", "HS256")
        .setting("xpack.security.authc.realms.jwt.jwt8.allowed_audiences", "aud8")
        .setting("xpack.security.authc.realms.jwt.jwt8.claims.principal", "sub")
        .setting("xpack.security.authc.realms.jwt.jwt8.client_authentication.type", "shared_secret")
        .keystore("xpack.security.authc.realms.jwt.jwt8.client_authentication.shared_secret", "client-shared-secret-string")
        .keystore("xpack.security.authc.realms.jwt.jwt8.hmac_key", "hmac-oidc-key-string-for-hs256-algorithm")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("admin_user", "admin-password")
        .user("security_test_user", "security-test-password", "security_test_role", false)
        .user("index_and_app_user", "security-test-password", "all_index_privileges,all_application_privileges", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static Path httpCAPath;
    private TestSecurityClient securityClient;

    @BeforeClass
    public static void findHttpCertificateAuthority() throws Exception {
        final URL resource = SecurityRealmSmokeTestCase.class.getResource("/ssl/http-server-ca.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/http-server-ca.crt");
        }
        httpCAPath = PathUtils.get(resource.toURI());
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).put(CERTIFICATE_AUTHORITIES, httpCAPath).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("security_test_user", new SecureString("security-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).put(CERTIFICATE_AUTHORITIES, httpCAPath).build();
    }

    @Override
    protected String getProtocol() {
        // Because http.ssl.enabled = true
        return "https";
    }

    protected Map<String, Object> authenticate(RequestOptions.Builder options) throws IOException {
        final Request request = new Request("GET", "_security/_authenticate");
        request.setOptions(options);
        final Response response = client().performRequest(request);
        return entityAsMap(response);
    }

    protected void assertUsername(Map<String, Object> authenticateResponse, String username) {
        assertThat(authenticateResponse, hasEntry("username", username));
    }

    protected void assertRealm(Map<String, Object> authenticateResponse, String realmType, String realmName) {
        assertThat(authenticateResponse, hasEntry(equalTo("authentication_realm"), instanceOf(Map.class)));
        Map<?, ?> realmObj = (Map<?, ?>) authenticateResponse.get("authentication_realm");
        assertThat(realmObj, hasEntry("type", realmType));
        assertThat(realmObj, hasEntry("name", realmName));
    }

    protected void assertRoles(Map<String, Object> authenticateResponse, String... roles) {
        assertThat(authenticateResponse, hasEntry(equalTo("roles"), instanceOf(List.class)));
        String[] roleJson = ((List<?>) authenticateResponse.get("roles")).toArray(String[]::new);
        assertThat(
            "Server returned unexpected roles list [" + Strings.arrayToCommaDelimitedString(roleJson) + "]",
            roleJson,
            arrayContainingInAnyOrder(roles)
        );
    }

    protected void assertNoApiKeyInfo(Map<String, Object> authenticateResponse, AuthenticationType type) {
        // If authentication type is API_KEY, authentication.api_key={"id":"abc123","name":"my-api-key"}. No encoded, api_key, or metadata.
        // If authentication type is other, authentication.api_key not present.
        assertThat(authenticateResponse, not(hasKey("api_key")));
    }

    protected void createUser(String username, SecureString password, List<String> roles) throws IOException {
        getSecurityClient().putUser(new User(username, roles.toArray(String[]::new)), password);
    }

    protected void changePassword(String username, SecureString password) throws IOException {
        getSecurityClient().changePassword(username, password);
    }

    protected void createRole(String name, Collection<String> clusterPrivileges) throws IOException {
        final RoleDescriptor role = new RoleDescriptor(
            name,
            clusterPrivileges.toArray(String[]::new),
            new RoleDescriptor.IndicesPrivileges[0],
            new String[0]
        );
        getSecurityClient().putRole(role);
    }

    protected void deleteUser(String username) throws IOException {
        getSecurityClient().deleteUser(username);
    }

    protected void deleteRole(String name) throws IOException {
        getSecurityClient().deleteRole(name);
    }

    protected TestSecurityClient getSecurityClient() {
        if (securityClient == null) {
            securityClient = new TestSecurityClient(adminClient());
        }
        return securityClient;
    }
}
