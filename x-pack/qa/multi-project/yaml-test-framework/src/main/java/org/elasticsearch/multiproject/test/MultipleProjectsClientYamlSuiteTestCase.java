/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multiproject.test;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for running YAML Rest tests against a cluster with multiple projects
 */
public abstract class MultipleProjectsClientYamlSuiteTestCase extends ESClientYamlSuiteTestCase {

    /**
     * Username to use to execute tests
     */
    protected static final String USER = Objects.requireNonNull(System.getProperty("tests.rest.cluster.username", "test_admin"));
    /**
     * Password for {@link #USER}.
     */
    protected static final String PASS = Objects.requireNonNull(System.getProperty("tests.rest.cluster.password", "test-password"));

    // The active project-id is slightly longer, and has a fixed suffix so that it's easier to pick in error messages etc.
    private final String activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
    private final Set<String> extraProjects = randomSet(1, 3, () -> randomAlphaOfLength(12).toLowerCase(Locale.ROOT));

    public MultipleProjectsClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Before
    public void configureProjects() throws Exception {
        initClient();
        createProject(activeProject);
        for (var project : extraProjects) {
            createProject(project);
        }

        if (shouldCreateTestIndex()) {
            // To verify everything is configured correctly, we create an index in the default project (using the admin client)
            // And check it's not visible by the regular (project) client
            // This ensures that the regular client (used by the yaml tests) is correctly pointing to a non-default project
            createIndex(adminClient(), "test-index", indexSettings(1, 1).build());
            assertThat(indexExists(adminClient(), "test-index"), equalTo(true));
            assertThat(indexExists(client(), "test-index"), equalTo(false));
        }
    }

    protected abstract boolean shouldCreateTestIndex();

    @After
    public void removeProjects() throws Exception {
        for (var project : extraProjects) {
            deleteProject(project);
        }
        deleteProject(activeProject);
    }

    private void createProject(String project) throws IOException {
        RestClient client = adminClient();
        final Request request = new Request("PUT", "/_project/" + project);
        try {
            final Response response = client.performRequest(request);
            logger.info("Created project {} : {}", project, response.getStatusLine());
        } catch (ResponseException e) {
            logger.error("Failed to create project: {}", project);
            throw e;
        }
    }

    private void deleteProject(String project) throws IOException {
        var client = adminClient();
        final Request request = new Request("DELETE", "/_project/" + project);
        try {
            final Response response = client.performRequest(request);
            logger.info("Deleted project {} : {}", project, response.getStatusLine());
        } catch (ResponseException e) {
            logger.error("Failed to delete project: {}", project);
            throw e;
        }
    }

    @Override
    protected Settings restClientSettings() {
        return clientSettings(true);
    }

    @Override
    protected Settings restAdminSettings() {
        return clientSettings(false);
    }

    private Settings clientSettings(boolean projectScoped) {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        if (projectScoped) {
            builder.put(ThreadContext.PREFIX + "." + Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, activeProject);
        }
        return builder.build();
    }
}
