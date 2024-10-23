/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.multiproject.test;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

@TimeoutSuite(millis = 30 * TimeUnits.MINUTE)
public class CoreWithMultipleProjectsClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final String USER = Objects.requireNonNull(System.getProperty("tests.rest.cluster.username", "test_admin"));
    private static final String PASS = Objects.requireNonNull(System.getProperty("tests.rest.cluster.password", "test-password"));

    // The active project-id is slightly longer, and has a fixed suffix so that its easier to pick in error messages etc.
    private final String activeProject = randomAlphaOfLength(8).toLowerCase(Locale.ROOT) + "00active";
    private final Set<String> extraProjects = randomSet(1, 3, () -> randomAlphaOfLength(12).toLowerCase(Locale.ROOT));

    private static boolean firstRun = true;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("constant-keyword")
        .module("mapper-extras")
        .module("rank-eval")
        .module("x-pack-ilm")
        .module("x-pack-stack")
        .module("ingest-common")
        .module("reindex")
        .module("wildcard")
        .module("analysis-common")
        .module("health-shards-availability")
        .module("data-streams")
        .module("multi-project")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user(USER, PASS)
        .feature(FeatureFlag.TIME_SERIES_MODE)
        .feature(FeatureFlag.FAILURE_STORE_ENABLED)
        .build();

    public CoreWithMultipleProjectsClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Before
    public void configureProjects() throws Exception {
        super.initClient();
        createProject(activeProject);
        for (var project : extraProjects) {
            createProject(project);
        }

        if (firstRun) {
            firstRun = false;
            // To verify everything is configured correctly, we create an index in the default project (using the admin client)
            // And check it's not visible by the regular (project) client
            // This ensures that the regular client (used by the yaml tests) is correctly pointing to a non-default project
            createIndex(adminClient(), "test-index", indexSettings(1, 1).build());
            assertThat(indexExists(adminClient(), "test-index"), equalTo(true));
            assertThat(indexExists(client(), "test-index"), equalTo(false));
        }
    }

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

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
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

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
