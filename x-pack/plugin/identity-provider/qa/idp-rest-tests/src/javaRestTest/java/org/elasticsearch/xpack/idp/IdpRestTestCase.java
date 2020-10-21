/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.PutPrivilegesRequest;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.ApplicationPrivilege;
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.idp.saml.sp.SamlServiceProviderIndex;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public abstract class IdpRestTestCase extends ESRestTestCase {

    private RestHighLevelClient highLevelAdminClient;

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("idp_admin", new SecureString("idp-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
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

    protected User createUser(String username, SecureString password, String... roles) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final User user = new User(username, List.of(roles), Map.of(), username + " in " + getTestName(), username + "@test.example.com");
        final PutUserRequest request = PutUserRequest.withPassword(user, password.getChars(), true, RefreshPolicy.IMMEDIATE);
        client.security().putUser(request, RequestOptions.DEFAULT);
        return user;
    }

    protected void deleteUser(String username) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final DeleteUserRequest request = new DeleteUserRequest(username, RefreshPolicy.WAIT_UNTIL);
        client.security().deleteUser(request, RequestOptions.DEFAULT);
    }

    protected void createRole(String name, Collection<String> clusterPrivileges, Collection<IndicesPrivileges> indicesPrivileges,
                              Collection<ApplicationResourcePrivileges> applicationPrivileges) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final Role role = Role.builder()
            .name(name)
            .clusterPrivileges(clusterPrivileges)
            .indicesPrivileges(indicesPrivileges)
            .applicationResourcePrivileges(applicationPrivileges)
            .build();
        client.security().putRole(new PutRoleRequest(role, null), RequestOptions.DEFAULT);
    }

    protected void deleteRole(String name) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final DeleteRoleRequest request = new DeleteRoleRequest(name, RefreshPolicy.WAIT_UNTIL);
        client.security().deleteRole(request, RequestOptions.DEFAULT);
    }

    protected void createApplicationPrivileges(String applicationName, Map<String, Collection<String>> privileges) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final List<ApplicationPrivilege> applicationPrivileges = privileges.entrySet().stream()
            .map(e -> new ApplicationPrivilege(applicationName, e.getKey(), List.copyOf(e.getValue()), null))
            .collect(Collectors.toUnmodifiableList());
        final PutPrivilegesRequest request = new PutPrivilegesRequest(applicationPrivileges, RefreshPolicy.IMMEDIATE);
        client.security().putPrivileges(request, RequestOptions.DEFAULT);
    }

    protected void setUserPassword(String username, SecureString password) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final ChangePasswordRequest request = new ChangePasswordRequest(username, password.getChars(), RefreshPolicy.NONE);
        client.security().changePassword(request, RequestOptions.DEFAULT);
    }

    protected SamlServiceProviderIndex.DocumentVersion createServiceProvider(String entityId, Map<String, Object> body) throws IOException {
        // so that we don't hit [SERVICE_UNAVAILABLE/1/state not recovered / initialized]
        ensureGreen("");
        final Request request = new Request("PUT", "/_idp/saml/sp/" + encode(entityId) + "?refresh=" + RefreshPolicy.IMMEDIATE.getValue());
        final String entity = Strings.toString(JsonXContent.contentBuilder().map(body));
        request.setJsonEntity(entity);
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("service_provider.entity_id", map), equalTo(entityId));
        assertThat(ObjectPath.eval("service_provider.enabled", map), equalTo(true));

        final Object docId = ObjectPath.eval("document._id", map);
        final Object seqNo = ObjectPath.eval("document._seq_no", map);
        final Object primaryTerm = ObjectPath.eval("document._primary_term", map);
        assertThat(docId, instanceOf(String.class));
        assertThat(seqNo, instanceOf(Number.class));
        assertThat(primaryTerm, instanceOf(Number.class));
        return new SamlServiceProviderIndex.DocumentVersion((String) docId, asLong(primaryTerm), asLong(seqNo));
    }

    protected void checkIndexDoc(SamlServiceProviderIndex.DocumentVersion docVersion) throws IOException {
        final Request request = new Request("GET", SamlServiceProviderIndex.INDEX_NAME + "/_doc/" + docVersion.id);
        final Response response = adminClient().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(map.get("_index"), equalTo(SamlServiceProviderIndex.INDEX_NAME));
        assertThat(map.get("_id"), equalTo(docVersion.id));
        assertThat(asLong(map.get("_seq_no")), equalTo(docVersion.seqNo));
        assertThat(asLong(map.get("_primary_term")), equalTo(docVersion.primaryTerm));
    }

    protected Long asLong(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof Long) {
            return (Long) val;
        }
        if (val instanceof Number) {
            return ((Number) val).longValue();
        }
        if (val instanceof String) {
            return Long.parseLong((String) val);
        }
        throw new IllegalArgumentException("Value [" + val + "] of type [" + val.getClass() + "] is not a Long");
    }

    protected String encode(String param) {
        return URLEncoder.encode(param, StandardCharsets.UTF_8);
    }

}
