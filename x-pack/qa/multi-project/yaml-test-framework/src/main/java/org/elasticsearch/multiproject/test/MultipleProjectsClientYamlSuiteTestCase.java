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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

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

    private void assertEmptyProject(String projectId) throws IOException {
        final Request request = new Request("GET", "_cluster/state/metadata,routing_table,customs");
        request.setOptions(request.getOptions().toBuilder().addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId).build());

        var response = responseAsMap(adminClient().performRequest(request));
        ObjectPath state = new ObjectPath(response);

        final var indexNames = ((Map<?, ?>) state.evaluate("metadata.indices")).keySet();
        final var routingTableEntries = ((Map<?, ?>) state.evaluate("routing_table.indices")).keySet();
        if (indexNames.isEmpty() == false || routingTableEntries.isEmpty() == false) {
            // Only the default project is allowed to have the security index after tests complete.
            // The security index could show up in the indices, routing table, or both.
            // If that happens, we need to check that it hasn't been modified by any leaking API calls.
            if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())
                && (indexNames.isEmpty() || (indexNames.size() == 1 && indexNames.contains(".security-7")))
                && (routingTableEntries.isEmpty() || (routingTableEntries.size() == 1 && routingTableEntries.contains(".security-7")))) {
                checkSecurityIndex();
            } else {
                // If there are any other indices or if this is for a non-default project, we fail the test.
                assertThat("Project [" + projectId + "] should not have indices", indexNames, empty());
                assertThat("Project [" + projectId + "] should not have routing entries", routingTableEntries, empty());
            }
        }
        assertThat(
            "Project [" + projectId + "] should not have graveyard entries",
            state.evaluate("metadata.index-graveyard.tombstones"),
            empty()
        );

        final Map<String, ?> legacyTemplates = state.evaluate("metadata.templates");
        if (legacyTemplates != null) {
            var templateNames = legacyTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have legacy templates", templateNames, empty());
        }

        final Map<String, Object> indexTemplates = state.evaluate("metadata.index_template.index_template");
        if (indexTemplates != null) {
            var templateNames = indexTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have index templates", templateNames, empty());
        } else if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())) {
            fail("Expected default project to have standard templates, but was null");
        }

        final Map<String, Object> componentTemplates = state.evaluate("metadata.component_template.component_template");
        if (componentTemplates != null) {
            var templateNames = componentTemplates.keySet().stream().filter(name -> isXPackTemplate(name) == false).toList();
            assertThat("Project [" + projectId + "] should not have component templates", templateNames, empty());
        } else if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())) {
            fail("Expected default project to have standard component templates, but was null");
        }

        final List<Map<String, ?>> pipelines = state.evaluate("metadata.ingest.pipeline");
        if (pipelines != null) {
            var pipelineNames = pipelines.stream()
                .map(pipeline -> String.valueOf(pipeline.get("id")))
                .filter(id -> isXPackIngestPipeline(id) == false)
                .toList();
            assertThat("Project [" + projectId + "] should not have ingest pipelines", pipelineNames, empty());
        } else if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())) {
            fail("Expected default project to have standard ingest pipelines, but was null");
        }

        final Map<String, Object> ilmPolicies = state.evaluate("metadata.index_lifecycle.policies");
        if (ilmPolicies != null) {
            var policyNames = new HashSet<>(ilmPolicies.keySet());
            policyNames.removeAll(preserveILMPolicyIds());
            assertThat("Project [" + projectId + "] should not have ILM Policies", policyNames, empty());
        } else if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id())) {
            fail("Expected default project to have standard ILM policies, but was null");
        }
    }

    private void checkSecurityIndex() throws IOException {
        final Request request = new Request("GET", "/_security/_query/role");
        request.setJsonEntity("""
            {
              "query": {
                "bool": {
                  "must_not": {
                    "term": {
                      "metadata._reserved": true
                    }
                  }
                }
              }
            }""");
        request.setOptions(
            request.getOptions().toBuilder().addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, Metadata.DEFAULT_PROJECT_ID.id()).build()
        );
        final var response = responseAsMap(adminClient().performRequest(request));
        assertThat("Security index should not contain any non-reserved roles", (Collection<?>) response.get("roles"), empty());
    }

    private void assertProjectIds(RestClient client, List<String> expectedProjects) throws IOException {
        final Collection<String> actualProjects = getProjectIds(client);
        assertThat(
            "Cluster returned project ids: " + actualProjects,
            actualProjects,
            containsInAnyOrder(expectedProjects.toArray(String[]::new))
        );
    }

    protected Collection<String> getProjectIds(RestClient client) throws IOException {
        final Request request = new Request("GET", "/_cluster/state/routing_table?multi_project=true");
        try {
            final ObjectPath response = ObjectPath.createFromResponse(client.performRequest(request));
            final List<Map<String, Object>> projectRouting = response.evaluate("routing_table.projects");
            return projectRouting.stream().map(obj -> (String) obj.get("id")).toList();
        } catch (ResponseException e) {
            logger.error("Failed to retrieve cluster state");
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
