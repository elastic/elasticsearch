/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.client.Request;
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

import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * User-managed service accounts are withheld from multi-project clusters: the default service-account
 * stores are not project-aware, and a multi-project deployment replaces the token store through a
 * {@code SecurityExtension}. Built-in {@code elastic/*} accounts stay readable.
 */
public class UserManagedServiceAccountMultiProjectIT extends ESRestTestCase {

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

    public void testUserManagedServiceAccountsUnavailableInMultiProjectCluster() throws Exception {
        final String project = randomIdentifier();
        createProject(project);

        final String namespace = randomIdentifier();
        final String service = "worker";
        final String principal = namespace + "/" + service;

        final Request putRequest = new Request("PUT", "/_security/service/" + namespace + "/" + service);
        putRequest.setJsonEntity("{\"roles\":[\"superuser\"],\"enabled\":true}");
        setProjectHeader(putRequest, project);
        assertError(putRequest, 500, "user-managed service accounts are not available in this cluster configuration");

        final Request deleteRequest = new Request("DELETE", "/_security/service/" + namespace + "/" + service);
        setProjectHeader(deleteRequest, project);
        assertError(deleteRequest, 500, "user-managed service accounts are not available in this cluster configuration");

        final Request tokenRequest = new Request("PUT", "/_security/service/" + namespace + "/" + service + "/credential/token/token-1");
        setProjectHeader(tokenRequest, project);
        assertError(tokenRequest, 400, "service account [" + principal + "] does not exist");

        final Request getBothKindsRequest = new Request("GET", "/_security/service");
        getBothKindsRequest.addParameter("managed_by", "elastic,user");
        setProjectHeader(getBothKindsRequest, project);
        final Map<String, Object> accounts = entityAsMap(client().performRequest(getBothKindsRequest));
        assertThat(accounts, hasKey("elastic/kibana"));
        assertThat(accounts, not(hasKey(principal)));

        final Request getUserManagedRequest = new Request("GET", "/_security/service");
        getUserManagedRequest.addParameter("managed_by", "user");
        setProjectHeader(getUserManagedRequest, project);
        assertThat(entityAsMap(client().performRequest(getUserManagedRequest)), anEmptyMap());
    }

    private void assertError(Request request, int status, String expectedMessage) {
        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(status));
        assertThat(exception.getMessage(), containsString(expectedMessage));
    }

    private static void setProjectHeader(Request request, String projectId) {
        request.setOptions(request.getOptions().toBuilder().addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId).build());
    }
}
