/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.multiproject.MultiProjectRestTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.MutableResource;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class SecurityRolesMultiProjectIT extends MultiProjectRestTestCase {

    private static final String PASSWORD = "wh@tever";

    private static final MutableResource rolesFile = MutableResource.from(Resource.fromString(""));

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .distribution(DistributionType.INTEG_TEST)
        .module("analysis-common")
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .user("admin", PASSWORD)
        .rolesFile(rolesFile)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("admin", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testUsersWithSameRoleNamesInDifferentProjects() throws Exception {
        var project1 = randomUniqueProjectId();
        var project2 = randomUniqueProjectId();

        createProject(project1.id());
        createProject(project2.id());

        String roleName = randomAlphaOfLength(8);
        createRole(project1, roleName, "monitor");
        createRole(project2, roleName, "manage");

        String username = randomAlphaOfLength(6);
        createUser(project1, username, roleName);
        createUser(project2, username, roleName);

        assertThat(getClusterPrivileges(project1, username), containsInAnyOrder("monitor"));
        assertThat(getClusterPrivileges(project2, username), containsInAnyOrder("manage"));
    }

    public void testInvalidateRoleInSingleProjectOnly() throws Exception {
        var project1 = randomUniqueProjectId();
        var project2 = randomUniqueProjectId();

        createProject(project1.id());
        createProject(project2.id());

        String roleName = randomAlphaOfLength(8);
        createRole(project1, roleName, "monitor");
        createRole(project2, roleName, "manage");

        String username = randomAlphaOfLength(6);
        createUser(project1, username, roleName);
        createUser(project2, username, roleName);

        assertThat(getClusterPrivileges(project1, username), containsInAnyOrder("monitor"));
        assertThat(getClusterPrivileges(project2, username), containsInAnyOrder("manage"));

        deleteDocument(project1, ".security-7", "role-" + roleName);
        deleteDocument(project2, ".security-7", "role-" + roleName);

        invalidateRoleCache(project1, roleName);

        // In project 1, the role is no longer cached so the user has no privileges
        assertThat(getClusterPrivileges(project1, username), empty());

        // In project 2 the role is still cached, so we can get privileges even though the role doc was deleted
        assertThat(getClusterPrivileges(project2, username), containsInAnyOrder("manage"));
    }

    public void testUpdatingFileBasedRoleAffectsAllProjects() throws Exception {
        final String originalRoles;
        final var roleName = "test_role";
        rolesFile.update(Resource.fromString(Strings.format("""
            %s:
               cluster:
                 - monitor
            """, roleName)));

        var project1 = randomUniqueProjectId();
        var project2 = randomUniqueProjectId();

        createProject(project1.id());
        createProject(project2.id());

        String username1 = randomAlphaOfLength(5);
        createUser(project1, username1, roleName);

        String username2 = randomAlphaOfLength(7);
        createUser(project2, username2, roleName);

        assertBusy(() -> {
            assertThat(getClusterPrivileges(project1, username1), contains("monitor"));
            assertThat(getClusterPrivileges(project2, username2), contains("monitor"));
        }, 20, TimeUnit.SECONDS); // increasing this to try and solve for a rare failure

        rolesFile.update(Resource.fromString(""));

        assertBusy(() -> {
            // Both projects should automatically reflect that the role has been removed
            assertThat(getClusterPrivileges(project1, username1), empty());
            assertThat(getClusterPrivileges(project2, username2), empty());
        });
    }

    private void createUser(ProjectId projectId, String username, String roleName) throws IOException {
        Request request = new Request("PUT", "/_security/user/" + username);
        request.setJsonEntity(Strings.format("""
            {
              "roles": [ "%s" ],
              "password": "%s"
            }
            """, roleName, PASSWORD));
        setRequestProjectId(request, projectId.id());
        client().performRequest(request);
    }

    private void createRole(ProjectId projectId, String roleName, String clusterPrivilege) throws IOException {
        Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity(Strings.format("""
            {
              "cluster": [ "%s" ]
            }
            """, clusterPrivilege));
        setRequestProjectId(request, projectId.id());
        client().performRequest(request);
    }

    private void invalidateRoleCache(ProjectId projectId, String roleName) throws IOException {
        Request request = new Request("POST", "/_security/role/" + roleName + "/_clear_cache");
        setRequestProjectId(request, projectId.id());
        client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private Collection<String> getClusterPrivileges(ProjectId projectId, String username) throws IOException {
        Request request = new Request("GET", "/_security/user/_privileges");
        setRequestProjectId(request, projectId.id());
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(username, new SecureString(PASSWORD.toCharArray())))
                .build()
        );
        final Map<String, Object> response = entityAsMap(client().performRequest(request));
        return (Collection<String>) response.get("cluster");
    }

    private void deleteDocument(ProjectId projectId, String index, String docId) throws IOException {
        Request request = new Request("DELETE", "/" + index + "/_doc/" + docId);
        setRequestProjectId(request, projectId.id());
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        client().performRequest(request);
    }

}
