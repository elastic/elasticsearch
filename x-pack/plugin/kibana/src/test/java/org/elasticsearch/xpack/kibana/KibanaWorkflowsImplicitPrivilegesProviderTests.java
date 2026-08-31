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

    // ---- Action string contract ----

    public void testActionStringsMatchKibanaDefinitions() {
        // These constants are constructed by Kibana's ApiActions and must stay in sync
        // with kbn-workflows/common/privileges.ts + feature_privilege_builder/api.ts.
        assertEquals("api:workflowsManagement:readExecution", READ_EXECUTION_ACTION);
        assertEquals("api:workflowsManagement:managed:readExecution", READ_MANAGED_EXECUTION_ACTION);
    }

    // ---- Base action only (no managed action) ----

    public void testBaseActionOnlyYieldsGrant1WithMustNotManaged() {
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
        assertTrue("grant 1 must include spaceId filter", query.contains("spaceId"));
        assertTrue("grant 1 must include default space", query.contains("default"));
        assertTrue("grant 1 must carry must_not managed:true", query.contains("must_not"));
        assertTrue("grant 1 must carry must_not managed:true", query.contains("managed"));
        assertFalse("grant 1 must not boost", query.contains("boost"));

        String stepQuery = grantForSingleIndex(result, STEP_EXECUTION_INDEX).getQuery().utf8ToString();
        assertTrue("step grant must require explicit managed:false", stepQuery.contains("\"managed\":false"));
        assertFalse("legacy steps with no managed field must fail closed", stepQuery.contains("must_not"));
    }

    public void testBaseActionOnlyYieldsNoGrant2() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        assertThat(provider.getImplicitIndicesPrivileges(resolve(role("wf_exec_read", "space:marketing"), stored)), hasSize(2));
    }

    // ---- Both actions, same space ----

    public void testBothActionsYieldTwoGrants() {
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
        boolean sawGrant1 = false;
        boolean sawGrant2 = false;
        for (RoleDescriptor.IndicesPrivileges g : result) {
            String query = g.getQuery().utf8ToString();
            if (Arrays.asList(g.getIndices()).contains(WORKFLOW_EXECUTION_INDEX)
                && Arrays.asList(g.getIndices()).contains(STEP_EXECUTION_INDEX)) {
                sawGrant2 = true;
                assertTrue(query.contains("default"));
            } else if (query.contains("must_not")) {
                sawGrant1 = true;
                assertTrue(query.contains("spaceId"));
            }
        }
        assertTrue("grant 1 (must_not) expected", sawGrant1);
        assertTrue("grant 2 (no must_not) expected", sawGrant2);
    }

    // ---- Asymmetric spaces: readExecution on marketing, readManaged on finance ----

    public void testAsymmetricSpacesYieldGrant1OnlyBecauseIntersectionIsEmpty() {
        // readExecution → marketing; readManagedExecution → finance
        // Intersection is empty → no grant 2.
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
        assertFalse("finance must not appear — intersection is empty", query.contains("finance"));
    }

    // ---- Wildcard resource ----

    public void testWildcardResourceGrant1HasNoSpaceIdFilter() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(
            resolve(role("wf_exec_read", "*"), stored)
        );

        assertThat(result, hasSize(2));
        String query = grantForSingleIndex(result, WORKFLOW_EXECUTION_INDEX).getQuery().utf8ToString();
        assertTrue(query.contains("must_not"));
        assertFalse("wildcard resource → no spaceId filter", query.contains("spaceId"));
    }

    public void testWildcardResourceBothActionsGrant2IsMatchAll() {
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
        for (RoleDescriptor.IndicesPrivileges g : result) {
            String query = g.getQuery().utf8ToString();
            if (Arrays.asList(g.getIndices()).contains(WORKFLOW_EXECUTION_INDEX)
                && Arrays.asList(g.getIndices()).contains(STEP_EXECUTION_INDEX)) {
                assertTrue("wildcard grant 2 should be match_all", query.contains("match_all"));
                assertFalse(query.contains("spaceId"));
            }
        }
    }

    // ---- Managed action alone yields no grant ----

    public void testManagedActionAloneYieldsNoGrant() {
        // Without the base action there is nothing to grant.
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_managed_only", Set.of(READ_MANAGED_EXECUTION_ACTION), Map.of())
        );
        assertThat(provider.getImplicitIndicesPrivileges(resolve(role("wf_managed_only", "space:default"), stored)), is(empty()));
    }

    // ---- Non-Kibana application ----

    public void testNonKibanaApplicationReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor("other-app", "exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        RoleDescriptor rd = roleWithApplication("other-app", "exec_read", "space:default");
        assertThat(provider.getImplicitIndicesPrivileges(resolve(rd, stored)), is(empty()));
    }

    // ---- Unrelated action ----

    public void testUnrelatedActionReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "cases_read", Set.of("cases:cases/getCase"), Map.of())
        );
        assertThat(provider.getImplicitIndicesPrivileges(resolve(role("cases_read", "space:default"), stored)), is(empty()));
    }

    // ---- Empty inputs ----

    public void testEmptyPrivilegesReturnsEmpty() {
        assertThat(provider.getImplicitIndicesPrivileges(List.of()), is(empty()));
    }

    public void testNonSpaceResourceReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "wf_exec_read", Set.of(READ_EXECUTION_ACTION), Map.of())
        );
        assertThat(provider.getImplicitIndicesPrivileges(resolve(role("wf_exec_read", "not-a-space"), stored)), is(empty()));
    }

    // ---- FLS: both grants carry identical grantedFields ----

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

        String[] fields0 = result.get(0).getGrantedFields();
        String[] fields1 = result.get(1).getGrantedFields();
        String[] fields2 = result.get(2).getGrantedFields();
        assertNotNull("grant 0 must have grantedFields", fields0);
        assertNotNull("grant 1 must have grantedFields", fields1);
        assertNotNull("grant 2 must have grantedFields", fields2);
        assertArrayEquals(
            "both grants must carry the same grantedFields so FLS is uniform",
            Arrays.stream(fields0).sorted().toArray(),
            Arrays.stream(fields1).sorted().toArray()
        );
        assertArrayEquals(
            "all grants must carry the same grantedFields so FLS is uniform",
            Arrays.stream(fields0).sorted().toArray(),
            Arrays.stream(fields2).sorted().toArray()
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
        assertFalse("bare 'usage' must not be in grantedFields — use usage.*", fields.contains("usage"));
        assertFalse("workflowDefinition must not be granted (enabled:false)", fields.contains("workflowDefinition"));
    }

    // ---- Raw action pattern (no stored descriptors) ----

    public void testRawActionPatternNoStoredDescriptors() {
        RoleDescriptor rd = role(READ_EXECUTION_ACTION, "space:default");
        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(resolve(rd, List.of()));
        assertThat(result, hasSize(2));
        assertTrue(grantForSingleIndex(result, WORKFLOW_EXECUTION_INDEX).getQuery().utf8ToString().contains("default"));
    }

    // ---- Wildcard application name ----

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

    // ---- No boost in DLS queries ----

    public void testGrant1DlsQueryContainsNoBoost() {
        String query = KibanaWorkflowsImplicitPrivilegesProvider.buildWorkflowExecutionDlsQuery(false, Set.of("marketing"));
        assertFalse("DLS query must not contain boost", query.contains("boost"));
    }

    public void testStepDlsQueryContainsNoBoost() {
        String query = KibanaWorkflowsImplicitPrivilegesProvider.buildStepExecutionDlsQuery(false, Set.of("marketing"));
        assertFalse("DLS query must not contain boost", query.contains("boost"));
    }

    public void testGrant2DlsQueryContainsNoBoost() {
        String query = KibanaWorkflowsImplicitPrivilegesProvider.buildGrant2DlsQuery(false, Set.of("marketing"));
        assertFalse("DLS query must not contain boost", query.contains("boost"));
    }

    // ---- Helpers (copied from KibanaCasesImplicitPrivilegesProviderTests) ----

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
