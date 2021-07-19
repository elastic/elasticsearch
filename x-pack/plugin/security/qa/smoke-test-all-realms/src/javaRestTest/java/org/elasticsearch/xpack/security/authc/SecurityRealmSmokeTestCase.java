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
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
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
import static org.hamcrest.Matchers.instanceOf;

public abstract class SecurityRealmSmokeTestCase extends ESRestTestCase {

    private static Path httpCAPath;
    private RestHighLevelClient highLevelAdminClient;

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
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CERTIFICATE_AUTHORITIES, httpCAPath)
            .build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("security_test_user", new SecureString("security-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CERTIFICATE_AUTHORITIES, httpCAPath)
            .build();
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
        assertThat("Server returned unexpected roles list [" + Strings.arrayToCommaDelimitedString(roleJson) + "]",
            roleJson,
            arrayContainingInAnyOrder(roles)
        );
    }

    protected void createUser(String username, SecureString password, List<String> roles) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        client.security().putUser(
            PutUserRequest.withPassword(new User(username, roles), password.getChars(), true, RefreshPolicy.WAIT_UNTIL),
            RequestOptions.DEFAULT);
    }

    protected void changePassword(String username, SecureString password) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        client.security().changePassword(new ChangePasswordRequest(username, password.getChars(), RefreshPolicy.WAIT_UNTIL),
            RequestOptions.DEFAULT);
    }

    protected void createRole(String name, Collection<String> clusterPrivileges) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final Role role = Role.builder().name(name).clusterPrivileges(clusterPrivileges).build();
        client.security().putRole(new PutRoleRequest(role, RefreshPolicy.WAIT_UNTIL), RequestOptions.DEFAULT);
    }

    protected void deleteUser(String username) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        client.security().deleteUser(new DeleteUserRequest(username), RequestOptions.DEFAULT);
    }

    protected void deleteRole(String name) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        client.security().deleteRole(new DeleteRoleRequest(name), RequestOptions.DEFAULT);
    }

    private RestHighLevelClient getHighLevelAdminClient() {
        if (highLevelAdminClient == null) {
            highLevelAdminClient = new RestHighLevelClient(
                adminClient(),
                ignore -> {
                },
                List.of()) {
            };
        }
        return highLevelAdminClient;
    }
}
