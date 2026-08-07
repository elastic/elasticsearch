/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class ManagedServiceAccountMultiProjectIT extends ESRestTestCase {

    private static final String ADMIN_PASSWORD = "hunter2";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.INTEG_TEST)
        .module("analysis-common")
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .user("admin", ADMIN_PASSWORD)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("admin", new SecureString(ADMIN_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    @After
    public void cleanup() throws Exception {
        cleanUpProjects();
    }

    public void testManagedServiceAccountsAreIsolatedByProject() throws Exception {
        final String project1 = randomIdentifier();
        final String project2 = randomIdentifier();
        createProject(project1);
        createProject(project2);

        final String namespace = "mpoc" + randomAlphaOfLengthBetween(3, 6).toLowerCase(Locale.ROOT);
        final String service = "worker";
        final String principal = namespace + "/" + service;
        final String monitorRole = "mpoc_monitor_" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        final String manageRole = "mpoc_manage_" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);

        putRole(project1, monitorRole, "monitor");
        putRole(project2, manageRole, "manage");
        putManagedAccount(project1, namespace, service, monitorRole);
        putManagedAccount(project2, namespace, service, manageRole);

        final String project1Bearer = createToken(project1, namespace, service, "token-1");
        final String project2Bearer = createToken(project2, namespace, service, "token-1");

        assertHasClusterPrivilege(project1, project1Bearer, principal, "monitor", true);
        assertHasClusterPrivilege(project1, project1Bearer, principal, "manage", false);

        assertHasClusterPrivilege(project2, project2Bearer, principal, "manage", true);
        assertHasClusterPrivilege(project2, project2Bearer, principal, "monitor", false);

        assertAuthenticateFails(project2, project1Bearer);
        assertAuthenticateFails(project1, project2Bearer);
    }

    private void putRole(String projectId, String roleName, String clusterPrivilege) throws IOException {
        final Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity("{\"cluster\":[\"" + clusterPrivilege + "\"]}");
        setProjectHeader(request, projectId);
        client().performRequest(request);
    }

    private void putManagedAccount(String projectId, String namespace, String service, String roleName) throws IOException {
        final Request request = new Request("PUT", "/_security/service/" + namespace + "/" + service);
        request.setJsonEntity("{\"roles\":[\"" + roleName + "\"],\"enabled\":true}");
        setProjectHeader(request, projectId);
        client().performRequest(request);
    }

    private String createToken(String projectId, String namespace, String service, String tokenName) throws IOException {
        final Request request = new Request(
            "PUT",
            "/_security/service/" + namespace + "/" + service + "/credential/token/" + tokenName
        );
        setProjectHeader(request, projectId);
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        @SuppressWarnings("unchecked")
        final Map<String, Object> token = (Map<String, Object>) response.get("token");
        return token.get("value").toString();
    }

    @SuppressWarnings("unchecked")
    private void assertHasClusterPrivilege(
        String projectId,
        String bearer,
        String principal,
        String privilege,
        boolean expected
    ) throws IOException {
        final Request request = new Request("GET", "/_security/user/_has_privileges");
        request.setJsonEntity("{\"cluster\":[\"" + privilege + "\"]}");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId)
                .addHeader("Authorization", "Bearer " + bearer)
                .build()
        );
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        @SuppressWarnings("unchecked")
        final Map<String, Boolean> cluster = (Map<String, Boolean>) response.get("cluster");
        assertThat(cluster.get(privilege), is(expected));
        assertThat(response, hasEntry("username", principal));
    }

    private void assertAuthenticateFails(String projectId, String bearer) {
        final Request request = new Request("GET", "/_security/_authenticate");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId)
                .addHeader("Authorization", "Bearer " + bearer)
                .build()
        );
        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(401));
    }

    private static void setProjectHeader(Request request, String projectId) {
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .removeHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER)
                .addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId)
                .build()
        );
    }
}
