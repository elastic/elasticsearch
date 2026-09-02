/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ResolvedApplicationPrivilege;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.kibana.KibanaWorkflowsImplicitPrivilegesProvider.KIBANA_APPLICATION;
import static org.elasticsearch.xpack.kibana.KibanaWorkflowsImplicitPrivilegesProvider.READ_EXECUTION_ACTION;
import static org.elasticsearch.xpack.kibana.KibanaWorkflowsImplicitPrivilegesProvider.READ_MANAGED_EXECUTION_ACTION;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class KibanaWorkflowsImplicitPrivilegesProviderTests extends ESTestCase {

    private static final String WORKFLOW_EXECUTION_INDEX = ".workflows-executions*";
    private static final String STEP_EXECUTION_INDEX = ".workflows-step-executions*";

    private final KibanaWorkflowsImplicitPrivilegesProvider provider = new KibanaWorkflowsImplicitPrivilegesProvider();

    public void testActionStringsMatchKibanaDefinitions() {
        assertEquals("api:workflowsManagement:readExecution", READ_EXECUTION_ACTION);
        assertEquals("api:workflowsManagement:managed:readExecution", READ_MANAGED_EXECUTION_ACTION);
    }

    public void testBaseActionOnlyYieldsWorkflowAndStepGrants() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(
            resolve(role("wf_exec_read", "space:default"), stored)
        );

        assertThat(result, hasSize(2));
        RoleDescriptor.IndicesPrivileges grant = grantForSingleIndex(result, WORKFLOW_EXECUTION_INDEX);
        assertThat(grant.getIndices(), arrayContainingInAnyOrder(WORKFLOW_EXECUTION_INDEX));
        assertThat(grant.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(grant.getQuery(), is(notNullValue()));

        String query = grant.getQuery().utf8ToString();
        assertTrue("workflow grant must include spaceId filter", query.contains("spaceId"));
        assertTrue("workflow grant must include default space", query.contains("default"));
        assertTrue("workflow grant must carry must_not managed:true", query.contains("must_not"));
        assertTrue("workflow grant must carry must_not managed:true", query.contains("\"managed\":true"));

        String stepQuery = grantForSingleIndex(result, STEP_EXECUTION_INDEX).getQuery().utf8ToString();
        assertTrue("step grant must require explicit managed:false", stepQuery.contains("\"managed\":false"));
        assertFalse("legacy steps with no managed field must fail closed", stepQuery.contains("must_not"));
    }

    public void testBothActionsYieldManagedGrant() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "wf_exec_all",
                Set.of(READ_EXECUTION_ACTION, READ_MANAGED_EXECUTION_ACTION),
                Map.of()
            )
        );
        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(
            resolve(role("wf_exec_all", "space:default"), stored)
        );

        assertThat(result, hasSize(3));
        boolean sawWorkflowGrant = false;
        boolean sawManagedGrant = false;
        for (RoleDescriptor.IndicesPrivileges grant : result) {
            String query = grant.getQuery().utf8ToString();
            List<String> indices = Arrays.asList(grant.getIndices());
            if (indices.contains(WORKFLOW_EXECUTION_INDEX) && indices.contains(STEP_EXECUTION_INDEX)) {
                sawManagedGrant = true;
                assertTrue(query.contains("default"));
            } else if (query.contains("must_not")) {
                sawWorkflowGrant = true;
                assertTrue(query.contains("spaceId"));
            }
        }
        assertTrue("workflow grant expected", sawWorkflowGrant);
        assertTrue("managed grant expected", sawManagedGrant);
    }

    public void testAsymmetricSpacesYieldNoManagedGrant() {
        RoleDescriptor rd = new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges(READ_EXECUTION_ACTION)
                    .resources("space:marketing")
                    .build(),
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges(READ_MANAGED_EXECUTION_ACTION)
                    .resources("space:finance")
                    .build() },
            null,
            null,
            null,
            null
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(resolve(rd, List.of()));
        assertThat(result, hasSize(2));
        String query = grantForSingleIndex(result, WORKFLOW_EXECUTION_INDEX).getQuery().utf8ToString();
        assertTrue(query.contains("must_not"));
        assertTrue(query.contains("marketing"));
        assertFalse("finance must not appear when the intersection is empty", query.contains("finance"));
    }

    public void testWildcardBaseGrantHasNoSpaceIdFilter() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(
            resolve(role("wf_exec_read", "*"), stored)
        );

        assertThat(result, hasSize(2));
        String query = grantForSingleIndex(result, WORKFLOW_EXECUTION_INDEX).getQuery().utf8ToString();
        assertTrue(query.contains("must_not"));
        assertFalse("wildcard resource must not add a spaceId filter", query.contains("spaceId"));
    }

    public void testWildcardResourceManagedGrantIsMatchAll() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "wf_exec_all",
                Set.of(READ_EXECUTION_ACTION, READ_MANAGED_EXECUTION_ACTION),
                Map.of()
            )
        );
        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(
            resolve(role("wf_exec_all", "*"), stored)
        );

        assertThat(result, hasSize(3));
        for (RoleDescriptor.IndicesPrivileges grant : result) {
            String query = grant.getQuery().utf8ToString();
            List<String> indices = Arrays.asList(grant.getIndices());
            if (indices.contains(WORKFLOW_EXECUTION_INDEX) && indices.contains(STEP_EXECUTION_INDEX)) {
                assertTrue("wildcard managed grant should be match_all", query.contains("match_all"));
                assertFalse(query.contains("spaceId"));
            }
        }
    }

    public void testManagedActionAloneYieldsNoGrant() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_managed_only", Set.of(READ_MANAGED_EXECUTION_ACTION), Map.of())
        );
        assertThat(provider.getImplicitIndicesPrivileges(resolve(role("wf_managed_only", "space:default"), stored)), is(empty()));
    }

    public void testNonKibanaApplicationReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor("other-app", "exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        RoleDescriptor rd = roleWithApplication("other-app", "exec_read", "space:default");
        assertThat(provider.getImplicitIndicesPrivileges(resolve(rd, stored)), is(empty()));
    }

    public void testUnrelatedActionReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "cases_read", Set.of("cases:cases/getCase"), Map.of())
        );
        assertThat(provider.getImplicitIndicesPrivileges(resolve(role("cases_read", "space:default"), stored)), is(empty()));
    }

    public void testEmptyPrivilegesReturnsEmpty() {
        assertThat(provider.getImplicitIndicesPrivileges(List.of()), is(empty()));
    }

    public void testNonSpaceResourceReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        assertThat(provider.getImplicitIndicesPrivileges(resolve(role("wf_exec_read", "not-a-space"), stored)), is(empty()));
    }

    public void testAllGrantsHaveIdenticalGrantedFields() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "wf_exec_all",
                Set.of(READ_EXECUTION_ACTION, READ_MANAGED_EXECUTION_ACTION),
                Map.of()
            )
        );
        List<RoleDescriptor.IndicesPrivileges> result = new ArrayList<>(
            provider.getImplicitIndicesPrivileges(resolve(role("wf_exec_all", "space:default"), stored))
        );
        assertThat(result, hasSize(3));

        String[] workflowFields = result.get(0).getGrantedFields();
        String[] stepFields = result.get(1).getGrantedFields();
        String[] managedFields = result.get(2).getGrantedFields();
        assertNotNull("workflow grant must have grantedFields", workflowFields);
        assertNotNull("step grant must have grantedFields", stepFields);
        assertNotNull("managed grant must have grantedFields", managedFields);
        assertArrayEquals(
            "workflow and step grants must carry the same grantedFields",
            Arrays.stream(workflowFields).sorted().toArray(),
            Arrays.stream(stepFields).sorted().toArray()
        );
        assertArrayEquals(
            "managed grant must carry the same grantedFields",
            Arrays.stream(workflowFields).sorted().toArray(),
            Arrays.stream(managedFields).sorted().toArray()
        );
    }

    public void testGrantedFieldsIncludeObjectPatterns() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        RoleDescriptor.IndicesPrivileges grant = grantForSingleIndex(
            provider.getImplicitIndicesPrivileges(resolve(role("wf_exec_read", "space:default"), stored)),
            WORKFLOW_EXECUTION_INDEX
        );

        List<String> fields = Arrays.asList(grant.getGrantedFields());
        assertTrue("usage.* must be in grantedFields", fields.contains("usage.*"));
        assertTrue("stepUsage.* must be in grantedFields", fields.contains("stepUsage.*"));
        assertTrue("hitl.* must be in grantedFields", fields.contains("hitl.*"));
        assertFalse("bare 'usage' must not be in grantedFields; use usage.*", fields.contains("usage"));
        assertFalse("workflowDefinition must not be granted (enabled:false)", fields.contains("workflowDefinition"));
    }

    public void testRawActionPatternNoStoredDescriptors() {
        RoleDescriptor rd = role(READ_EXECUTION_ACTION, "space:default");
        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(resolve(rd, List.of()));
        assertThat(result, hasSize(2));
        assertTrue(grantForSingleIndex(result, WORKFLOW_EXECUTION_INDEX).getQuery().utf8ToString().contains("default"));
    }

    public void testWildcardApplicationName() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        RoleDescriptor rd = roleWithApplication("kibana-*", "wf_exec_read", "space:default");
        assertThat(provider.getImplicitIndicesPrivileges(resolve(rd, stored)), hasSize(2));
    }

    public void testNonKibanaWildcardApplicationDoesNotMatch() {
        RoleDescriptor rd = roleWithApplication("shield*", READ_EXECUTION_ACTION, "space:default");
        assertThat(provider.getImplicitIndicesPrivileges(resolve(rd, List.of())), is(empty()));
    }

    public void testWorkflowExecutionDlsQueryContainsNoBoost() {
        String query = KibanaWorkflowsImplicitPrivilegesProvider.buildWorkflowExecutionDlsQuery(false, Set.of("marketing"));
        assertFalse("DLS query must not contain boost", query.contains("boost"));
    }

    public void testStepDlsQueryContainsNoBoost() {
        String query = KibanaWorkflowsImplicitPrivilegesProvider.buildStepExecutionDlsQuery(false, Set.of("marketing"));
        assertFalse("DLS query must not contain boost", query.contains("boost"));
    }

    public void testManagedExecutionDlsQueryContainsNoBoost() {
        String query = KibanaWorkflowsImplicitPrivilegesProvider.buildManagedExecutionDlsQuery(false, Set.of("marketing"));
        assertFalse("DLS query must not contain boost", query.contains("boost"));
    }

    private static Collection<ResolvedApplicationPrivilege> resolve(
        RoleDescriptor roleDescriptor,
        Collection<ApplicationPrivilegeDescriptor> stored
    ) {
        final List<ResolvedApplicationPrivilege> resolved = new ArrayList<>();
        for (RoleDescriptor.ApplicationResourcePrivileges arp : roleDescriptor.getApplicationPrivileges()) {
            final Set<String> resources = new HashSet<>(Arrays.asList(arp.getResources()));
            ApplicationPrivilege.get(arp.getApplication(), new HashSet<>(Arrays.asList(arp.getPrivileges())), stored)
                .forEach(privilege -> resolved.add(new ResolvedApplicationPrivilege(privilege, resources)));
        }
        return resolved;
    }

    private static RoleDescriptor.IndicesPrivileges grantForSingleIndex(Collection<RoleDescriptor.IndicesPrivileges> grants, String index) {
        return grants.stream().filter(grant -> Arrays.equals(grant.getIndices(), new String[] { index })).findFirst().orElseThrow();
    }

    private static RoleDescriptor role(String privilegeName, String... resources) {
        return roleWithApplication(KIBANA_APPLICATION, privilegeName, resources);
    }

    private static RoleDescriptor roleWithApplication(String application, String privilegeName, String... resources) {
        return new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(application)
                    .privileges(privilegeName)
                    .resources(resources)
                    .build() },
            null,
            null,
            null,
            null
        );
    }
}
