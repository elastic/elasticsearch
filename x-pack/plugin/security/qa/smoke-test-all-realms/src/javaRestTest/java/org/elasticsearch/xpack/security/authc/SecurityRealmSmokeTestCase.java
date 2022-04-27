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
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.BeforeClass;

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

@SuppressWarnings("removal")
public abstract class SecurityRealmSmokeTestCase extends ESRestTestCase {

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
