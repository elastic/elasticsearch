/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under
 * one or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.privilege;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class KibanaWorkflowsImplicitPrivilegesProviderTests extends ESTestCase {

    private final KibanaWorkflowsImplicitPrivilegesProvider provider = new KibanaWorkflowsImplicitPrivilegesProvider();

    public void testGrantsUnmanagedExecutionReadForAuthorizedSpace() {
        final Collection<IndicesPrivileges> privileges = provider.getImplicitIndicesPrivileges(
            roleWithPrivileges(
                ApplicationResourcePrivileges.builder()
                    .application("kibana-.kibana")
                    .privileges("feature_workflowsManagement.workflow_execution_read")
                    .resources("space:marketing")
                    .build()
            ),
            Set.of(
                new ApplicationPrivilegeDescriptor(
                    "kibana-.kibana",
                    "feature_workflowsManagement.workflow_execution_read",
                    Set.of(KibanaWorkflowsImplicitPrivilegesProvider.WORKFLOWS_READ_EXECUTION_ACTION),
                    Map.of()
                )
            )
        );

        assertThat(privileges, hasSize(1));
        final IndicesPrivileges indexPrivilege = privileges.iterator().next();
        assertArrayEquals(new String[] { KibanaWorkflowsImplicitPrivilegesProvider.WORKFLOWS_EXECUTIONS_INDEX }, indexPrivilege.getIndices());
        assertArrayEquals(new String[] { "read" }, indexPrivilege.getPrivileges());
        final String query = query(indexPrivilege);
        assertThat(query, containsString("\"spaceId\":[\"marketing\"]"));
        assertThat(query, containsString("\"must_not\":[{\"term\":{\"managed\":true}}]"));
    }

    public void testManagedExecutionReadOnlyBroadensSpacesWithBaseExecutionRead() {
        final Collection<IndicesPrivileges> privileges = provider.getImplicitIndicesPrivileges(
            roleWithPrivileges(
                ApplicationResourcePrivileges.builder()
                    .application("kibana-.kibana")
                    .privileges("feature_workflowsManagement.workflow_execution_read")
                    .resources("space:marketing", "space:sales")
                    .build(),
                ApplicationResourcePrivileges.builder()
                    .application("kibana-.kibana")
                    .privileges("feature_workflowsManagement.workflow_execution_read_managed")
                    .resources("space:marketing")
                    .build()
            ),
            Set.of(
                new ApplicationPrivilegeDescriptor(
                    "kibana-.kibana",
                    "feature_workflowsManagement.workflow_execution_read",
                    Set.of(KibanaWorkflowsImplicitPrivilegesProvider.WORKFLOWS_READ_EXECUTION_ACTION),
                    Map.of()
                ),
                new ApplicationPrivilegeDescriptor(
                    "kibana-.kibana",
                    "feature_workflowsManagement.workflow_execution_read_managed",
                    Set.of(KibanaWorkflowsImplicitPrivilegesProvider.WORKFLOWS_READ_MANAGED_EXECUTION_ACTION),
                    Map.of()
                )
            )
        );

        assertThat(privileges, hasSize(1));
        final String query = query(privileges.iterator().next());
        assertThat(query, containsString("\"spaceId\":[\"marketing\",\"sales\"]"));
        assertThat(query, containsString("\"must_not\":[{\"term\":{\"managed\":true}}]"));
        assertThat(query, containsString("\"spaceId\":[\"marketing\"]"));
    }

    public void testGlobalExecutionAndManagedReadDoesNotNeedDls() {
        final Collection<IndicesPrivileges> privileges = provider.getImplicitIndicesPrivileges(
            roleWithPrivileges(
                ApplicationResourcePrivileges.builder()
                    .application("kibana-.kibana")
                    .privileges(
                        "feature_workflowsManagement.workflow_execution_read",
                        "feature_workflowsManagement.workflow_execution_read_managed"
                    )
                    .resources("*")
                    .build()
            ),
            Set.of(
                new ApplicationPrivilegeDescriptor(
                    "kibana-.kibana",
                    "feature_workflowsManagement.workflow_execution_read",
                    Set.of(KibanaWorkflowsImplicitPrivilegesProvider.WORKFLOWS_READ_EXECUTION_ACTION),
                    Map.of()
                ),
                new ApplicationPrivilegeDescriptor(
                    "kibana-.kibana",
                    "feature_workflowsManagement.workflow_execution_read_managed",
                    Set.of(KibanaWorkflowsImplicitPrivilegesProvider.WORKFLOWS_READ_MANAGED_EXECUTION_ACTION),
                    Map.of()
                )
            )
        );

        assertThat(privileges, hasSize(1));
        assertThat(privileges.iterator().next().getQuery(), nullValue());
    }

    private static RoleDescriptor roleWithPrivileges(ApplicationResourcePrivileges... privileges) {
        return new RoleDescriptor("role", null, null, privileges);
    }

    private static String query(IndicesPrivileges privilege) {
        return privilege.getQuery().utf8ToString();
    }
}
