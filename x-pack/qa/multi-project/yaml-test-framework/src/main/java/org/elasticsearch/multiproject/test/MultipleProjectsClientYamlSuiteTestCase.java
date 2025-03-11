/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multiproject.test;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

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

    private static String activeProject;
    private static Set<String> extraProjects;
    private static boolean projectsConfigured = false;

    public MultipleProjectsClientYamlSuiteTestCase(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @BeforeClass
    public static void initializeProjectIds() {
        // The active project-id is slightly longer, and has a fixed suffix so that it's easier to pick in error messages etc.
        activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
        extraProjects = randomSet(1, 3, () -> randomAlphaOfLength(12).toLowerCase(Locale.ROOT));
    }

    @Override
    protected RestClient getCleanupClient() {
        return client();
    }

    @Before
    public void configureProjects() throws Exception {
        if (projectsConfigured) {
            return;
        }
        projectsConfigured = true;
        initClient();
        createProject(activeProject);
        for (var project : extraProjects) {
            createProject(project);
        }

        // The admin client does not set a project id, and can see all projects
        assertProjectIds(
            adminClient(),
            CollectionUtils.concatLists(List.of(Metadata.DEFAULT_PROJECT_ID.id(), activeProject), extraProjects)
        );
        // The test client can only see the project it targets
        assertProjectIds(client(), List.of(activeProject));
    }

    @After
    public final void assertEmptyProjects() throws Exception {
        assertEmptyProject(Metadata.DEFAULT_PROJECT_ID.id());
        for (var project : extraProjects) {
            assertEmptyProject(project);
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
