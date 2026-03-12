/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.ClassRule;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SecurityIndexMigrationMultiProjectIT extends ESRestTestCase {

    private static final String USER = "admin-user";
    private static final String PASS = "admin-password";

    @ClassRule
    public static ElasticsearchCluster CLUSTER = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-multi-project")
        .module("analysis-common")
        .nodes(2)
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .user(USER, PASS)
        .build();

    @Override
    protected String getTestRestCluster() {
        return CLUSTER.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        final Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(ThreadContext.PREFIX + ".Authorization", token);
        return builder.build();
    }

    public void testMigrateSecurityIndex() throws Exception {
        final String expectedMigrationVersion = SecurityMigrations.MIGRATIONS_BY_VERSION.keySet()
            .stream()
            .max(Comparator.naturalOrder())
            .map(String::valueOf)
            .orElseThrow();

        final List<String> projectIds = randomList(1, 4, ESTestCase::randomIdentifier);
        for (String projectId : projectIds) {
            logger.info("Creating project [{}]", projectId);
            createProject(projectId);
            final TestSecurityClient securityClient = new TestSecurityClient(
                adminClient(),
                RequestOptions.DEFAULT.toBuilder().addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId).build()
            );
            // Trigger creation of the security index
            final String username = randomAlphaOfLength(8);
            securityClient.putUser(new User(username), new SecureString(randomAlphaOfLength(12).toCharArray()));
            logger.info("Created user [{}] in project [{}]", username, projectId);
        }

        assertBusy(() -> {
            // Get the index state for every security index
            final Map<String, Object> clusterState = getAsMap(
                adminClient(),
                "/_cluster/state/metadata/" + SecuritySystemIndices.SECURITY_MAIN_ALIAS + "?multi_project=true"
            );
            final Map<Object, Map<String, Object>> projectStateById = ObjectPath.<List<Map<String, Object>>>evaluate(
                clusterState,
                "metadata.projects"
            ).stream().collect(Collectors.toMap(obj -> obj.get("id"), Function.identity()));

            for (var projectId : projectIds) {
                var projectState = projectStateById.get(projectId);
                assertThat("project [" + projectId + "] is not available in cluster state", projectState, notNullValue());

                Map<String, Object> indices = ObjectPath.evaluate(projectState, "indices");
                assertThat("project [  " + projectId + "] should have a single security index", indices, aMapWithSize(1));

                final var entry = Iterables.get(indices.entrySet(), 0);
                Object migrationVersion = ObjectPath.evaluate(entry.getValue(), "migration_version.version");
                assertThat(
                    "project [" + projectId + ", index [" + entry.getKey() + "] should have been migrated",
                    migrationVersion,
                    equalTo(expectedMigrationVersion)
                );
            }
        });
    }
}
