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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Managed service accounts are deliberately unavailable in multi-project clusters: the service
 * account credential caches are keyed without a project dimension, and multi-project deployments
 * (serverless) replace the token store through a {@code SecurityExtension} which disables the
 * index-backed stores entirely. This test pins that scoping decision for a multi-project cluster
 * running the default store wiring; built-in {@code elastic/*} accounts must remain readable.
 */
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

    public void testManagedServiceAccountsUnavailableInMultiProjectCluster() throws Exception {
        final String project = randomIdentifier();
        createProject(project);

        final String namespace = "mpoc" + randomAlphaOfLengthBetween(3, 6).toLowerCase(Locale.ROOT);
        final String service = "worker";

        final Request putRequest = new Request("PUT", "/_security/service/" + namespace + "/" + service);
        putRequest.setJsonEntity("{\"roles\":[\"superuser\"],\"enabled\":true}");
        setProjectHeader(putRequest, project);
        assertBadRequest(putRequest, "managed service accounts are not available in this cluster configuration");

        final Request deleteRequest = new Request("DELETE", "/_security/service/" + namespace + "/" + service);
        setProjectHeader(deleteRequest, project);
        assertBadRequest(deleteRequest, "managed service accounts are not available in this cluster configuration");

        final Request tokenRequest = new Request("PUT", "/_security/service/" + namespace + "/" + service + "/credential/token/token-1");
        setProjectHeader(tokenRequest, project);
        assertBadRequest(tokenRequest, "service account [" + namespace + "/" + service + "] does not exist");

        final Request getRequest = new Request("GET", "/_security/service");
        getRequest.addParameter("managed_by", "elastic,user");
        setProjectHeader(getRequest, project);
        final Map<String, Object> accounts = entityAsMap(client().performRequest(getRequest));
        assertThat(accounts, hasKey("elastic/kibana"));
        assertThat(accounts, not(hasKey(namespace + "/" + service)));
    }

    private void assertBadRequest(Request request, String expectedMessage) {
        final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(readResponseBody(exception), containsString(expectedMessage));
    }

    private static String readResponseBody(ResponseException exception) {
        try {
            return new String(exception.getResponse().getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError("failed to read error response body", e);
        }
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
