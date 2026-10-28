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

import static org.elasticsearch.xpack.kibana.KibanaCasesImplicitPrivilegesProvider.GET_CASE_ACTION_CASES;
import static org.elasticsearch.xpack.kibana.KibanaCasesImplicitPrivilegesProvider.GET_CASE_ACTION_OBSERVABILITY;
import static org.elasticsearch.xpack.kibana.KibanaCasesImplicitPrivilegesProvider.GET_CASE_ACTION_SECURITY_SOLUTION;
import static org.elasticsearch.xpack.kibana.KibanaCasesImplicitPrivilegesProvider.KIBANA_APPLICATION;
import static org.elasticsearch.xpack.kibana.KibanaCasesImplicitPrivilegesProvider.OWNER_CASES;
import static org.elasticsearch.xpack.kibana.KibanaCasesImplicitPrivilegesProvider.OWNER_OBSERVABILITY;
import static org.elasticsearch.xpack.kibana.KibanaCasesImplicitPrivilegesProvider.OWNER_SECURITY_SOLUTION;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class KibanaCasesImplicitPrivilegesProviderTests extends ESTestCase {

    private static final String[] CASES_INDICES = { ".cases*" };

    private final KibanaCasesImplicitPrivilegesProvider contributor = new KibanaCasesImplicitPrivilegesProvider();

    public void testSingleOwnerSingleSpaceGrantsOwnerAndSpaceDlsQuery() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_observability_cases_read",
                Set.of(GET_CASE_ACTION_OBSERVABILITY),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = role("feature_observability_cases_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(CASES_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery(), is(notNullValue()));

        String query = privilege.getQuery().utf8ToString();
        assertTrue(query.contains("\"owner\""));
        assertTrue(query.contains(OWNER_OBSERVABILITY));
        assertTrue(query.contains("\"space_id\""));
        assertTrue(query.contains("default"));
    }

    public void testMultipleSpacesForSameOwnerAreMerged() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "cases_read", Set.of(GET_CASE_ACTION_CASES), Map.of())
        );
        RoleDescriptor roleDescriptor = role("cases_read", "space:foo", "space:bar", "space:baz");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        String query = result.iterator().next().getQuery().utf8ToString();
        assertTrue(query.contains(OWNER_CASES));
        assertTrue(query.contains("foo"));
        assertTrue(query.contains("bar"));
        assertTrue(query.contains("baz"));
    }

    public void testWildcardResourceGrantsOwnerScopedAccessWithoutSpaceFilter() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_security_cases_read",
                Set.of(GET_CASE_ACTION_SECURITY_SOLUTION),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = role("feature_security_cases_read", "*");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(CASES_INDICES));
        String query = privilege.getQuery().utf8ToString();
        assertTrue(query.contains(OWNER_SECURITY_SOLUTION));
        // Wildcard resource means no space restriction, but the owner filter must still be present -
        // the wildcard only spans every space for THIS owner, not every owner.
        assertFalse(query.contains("space_id"));
    }

    public void testWildcardTakesPrecedenceOverSpecificSpacesForSameOwner() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "cases_read", Set.of(GET_CASE_ACTION_CASES), Map.of())
        );
        RoleDescriptor roleDescriptor = role("cases_read", "*", "space:foo");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertFalse(result.iterator().next().getQuery().utf8ToString().contains("space_id"));
    }

    public void testMultipleOwnersInSameRoleProduceIndependentEntries() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_observability_cases_read",
                Set.of(GET_CASE_ACTION_OBSERVABILITY),
                Map.of()
            ),
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_security_cases_read",
                Set.of(GET_CASE_ACTION_SECURITY_SOLUTION),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges("feature_observability_cases_read")
                    .resources("space:marketing")
                    .build(),
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges("feature_security_cases_read")
                    .resources("space:default")
                    .build() },
            null,
            null,
            null,
            null
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(2));

        boolean sawObservabilityMarketing = false;
        boolean sawSecuritySolutionDefault = false;
        for (RoleDescriptor.IndicesPrivileges privilege : result) {
            String query = privilege.getQuery().utf8ToString();
            if (query.contains(OWNER_OBSERVABILITY)) {
                assertTrue(query.contains("marketing"));
                assertFalse(query.contains("default"));
                sawObservabilityMarketing = true;
            } else if (query.contains(OWNER_SECURITY_SOLUTION)) {
                assertTrue(query.contains("default"));
                assertFalse(query.contains("marketing"));
                sawSecuritySolutionDefault = true;
            }
        }
        assertTrue(sawObservabilityMarketing);
        assertTrue(sawSecuritySolutionDefault);
    }

    public void testOwnerGrantDoesNotLeakToOtherOwners() {
        // Role only ever grants the observability action; the resulting query must not
        // mention securitySolution or the generic "cases" owner anywhere.
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_observability_cases_read",
                Set.of(GET_CASE_ACTION_OBSERVABILITY),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = role("feature_observability_cases_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        String query = result.iterator().next().getQuery().utf8ToString();
        assertFalse(query.contains(OWNER_SECURITY_SOLUTION));
        assertFalse(query.contains("\"" + OWNER_CASES + "\""));
    }

    public void testNonMatchingApplicationReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor("other-app", "cases_read", Set.of(GET_CASE_ACTION_CASES), Map.of())
        );
        RoleDescriptor roleDescriptor = new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("other-app")
                    .privileges("cases_read")
                    .resources("space:default")
                    .build() },
            null,
            null,
            null,
            null
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    public void testNonMatchingActionReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "cases_write", Set.of("cases:cases/createCase"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("cases_write", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    public void testResourcesWithoutSpacePrefixAreIgnored() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "cases_read", Set.of(GET_CASE_ACTION_CASES), Map.of())
        );
        RoleDescriptor roleDescriptor = role("cases_read", "no-prefix-resource");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    public void testEmptyStoredPrivilegesReturnsEmpty() {
        RoleDescriptor roleDescriptor = role("cases_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, is(empty()));
    }

    public void testEmptyResolvedApplicationPrivilegesReturnsEmpty() {
        // Mirrors what CompositeRolesStore passes when a role has no application privileges at
        // all (e.g. an index-privileges-only role) - the provider isn't even invoked in
        // production for that case, but it must degrade gracefully regardless.
        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(List.of());
        assertThat(result, is(empty()));
    }

    public void testStoredPrivilegeWithMultipleActionsIncludingGetCase() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_all_cases",
                Set.of(GET_CASE_ACTION_OBSERVABILITY, "cases:observability/createCase", "cases:observability/deleteCase"),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = role("feature_all_cases", "space:marketing");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("marketing"));
    }

    public void testStoredPrivilegeBundlingTwoOwnersContributesToBoth() {
        // A hypothetical stored privilege whose action set spans two owners' getCase actions.
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_multi_owner_cases",
                Set.of(GET_CASE_ACTION_OBSERVABILITY, GET_CASE_ACTION_SECURITY_SOLUTION),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = role("feature_multi_owner_cases", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(2));
        boolean sawObservability = false;
        boolean sawSecuritySolution = false;
        for (RoleDescriptor.IndicesPrivileges privilege : result) {
            String query = privilege.getQuery().utf8ToString();
            sawObservability |= query.contains(OWNER_OBSERVABILITY);
            sawSecuritySolution |= query.contains(OWNER_SECURITY_SOLUTION);
        }
        assertTrue(sawObservability);
        assertTrue(sawSecuritySolution);
    }

    public void testBuildOwnerDlsQuery() {
        String query = KibanaCasesImplicitPrivilegesProvider.buildOwnerDlsQuery(OWNER_CASES);
        assertTrue(query.contains("term"));
        assertTrue(query.contains("\"owner\""));
        assertTrue(query.contains(OWNER_CASES));
        assertFalse(query.contains("space_id"));
        // Hand-rolled via XContentBuilder rather than QueryBuilders.termQuery, which always adds a
        // "boost" field that would otherwise leak into the query stored on the role.
        assertFalse(query.contains("boost"));
    }

    public void testBuildOwnerAndSpaceIdsDlsQuery() {
        String query = KibanaCasesImplicitPrivilegesProvider.buildOwnerAndSpaceIdsDlsQuery(OWNER_OBSERVABILITY, Set.of("default"));
        assertTrue(query.contains("bool"));
        assertTrue(query.contains("filter"));
        assertTrue(query.contains("\"owner\""));
        assertTrue(query.contains(OWNER_OBSERVABILITY));
        assertTrue(query.contains("\"space_id\""));
        assertTrue(query.contains("default"));
        // Hand-rolled via XContentBuilder rather than QueryBuilders.termQuery/termsQuery, which
        // always add a "boost" field that would otherwise leak into the query stored on the role.
        assertFalse(query.contains("boost"));
    }

    public void testStoredPrivilegeWithWildcardActionPatternForOwner() {
        // Hypothetical "feature_security_cases_all" privilege whose actions cover cases:securitySolution/* .
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_security_cases_all",
                Set.of("cases:securitySolution/*"),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = role("feature_security_cases_all", "space:marketing");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("marketing"));
    }

    public void testRoleWithWildcardApplicationName() {
        // Role declares "kibana-*" instead of literal "kibana-.kibana".
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_cases_read", Set.of(GET_CASE_ACTION_CASES), Map.of())
        );
        RoleDescriptor roleDescriptor = roleWithApplication("kibana-*", "feature_cases_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("default"));
    }

    public void testRoleWithRawActionPatternNoStoredDescriptors() {
        // Role embeds the action name directly under privileges, NativePrivilegeStore returns nothing,
        // but the raw-pattern branch should still pick this up.
        RoleDescriptor roleDescriptor = role(GET_CASE_ACTION_CASES, "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("default"));
    }

    public void testRoleWithWildcardPrivilegePatternMatchesAllOwners() {
        // privileges=["cases:*"] matches all three owners' getCase actions.
        RoleDescriptor roleDescriptor = role("cases:*", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(3));
    }

    public void testRoleWithSuperWildcardPrivilegeGrantsAllOwnersWithWildcardResource() {
        // privileges=["*"] on resources=["*"] - matches every owner, no DLS space restriction, but each
        // entry still carries its own owner filter.
        RoleDescriptor roleDescriptor = role("*", "*");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(3));
        for (RoleDescriptor.IndicesPrivileges privilege : result) {
            assertThat(privilege.getQuery(), is(notNullValue()));
            assertFalse(privilege.getQuery().utf8ToString().contains("space_id"));
        }
    }

    public void testRoleWithWildcardAppAndRawActionPattern() {
        // Combined: application wildcard + privilege pattern, no stored descriptors needed.
        RoleDescriptor roleDescriptor = roleWithApplication("kibana-*", "cases:observability/*", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("default"));
    }

    public void testNonKibanaWildcardAppDoesNotMatch() {
        // "shield*" must not match "kibana-.kibana" even when the privilege pattern would otherwise match.
        RoleDescriptor roleDescriptor = roleWithApplication("shield*", "cases:*", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, is(empty()));
    }

    public void testStoredPrivilegeWithDifferentActionDoesNotMatch() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_alerts_only", Set.of("alerts:read"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("feature_alerts_only", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    public void testResultIndicesCoverAllThreeCasesSurfaces() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "cases_read", Set.of(GET_CASE_ACTION_CASES), Map.of())
        );
        RoleDescriptor roleDescriptor = role("cases_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getIndices(), arrayContainingInAnyOrder(CASES_INDICES));
    }

    /**
     * Resolves a role's declared application privileges into {@link ResolvedApplicationPrivilege}s exactly as
     * {@code CompositeRolesStore} does before invoking the provider: each {@code (application, privileges[])} grant is
     * resolved against the stored descriptors (which builds the action automaton), paired with the block's resources.
     */
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
