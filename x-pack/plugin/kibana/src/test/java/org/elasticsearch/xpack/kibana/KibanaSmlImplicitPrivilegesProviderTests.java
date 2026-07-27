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

import static org.elasticsearch.xpack.kibana.KibanaSmlImplicitPrivilegesProvider.KIBANA_APPLICATION;
import static org.elasticsearch.xpack.kibana.KibanaSmlImplicitPrivilegesProvider.LOGIN_ACTION;
import static org.elasticsearch.xpack.kibana.KibanaSmlImplicitPrivilegesProvider.PERMISSIONS_COUNT_FIELD;
import static org.elasticsearch.xpack.kibana.KibanaSmlImplicitPrivilegesProvider.PERMISSIONS_FIELD;
import static org.elasticsearch.xpack.kibana.KibanaSmlImplicitPrivilegesProvider.SML_INDICES;
import static org.elasticsearch.xpack.kibana.KibanaSmlImplicitPrivilegesProvider.SPACES_FIELD;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KibanaSmlImplicitPrivilegesProviderTests extends ESTestCase {

    private final KibanaSmlImplicitPrivilegesProvider contributor = new KibanaSmlImplicitPrivilegesProvider();

    /** User holds login: + a saved_object action on a single space → DLS query with space and privilege clauses. */
    public void testSingleSpaceGrantsDlsQuery() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_sml_read",
                Set.of(LOGIN_ACTION, "saved_object:dashboard/get"),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = role("feature_sml_read", "space:marketing");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(SML_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery(), is(notNullValue()));

        String query = privilege.getQuery().utf8ToString();
        assertTrue(query.contains("\"" + SPACES_FIELD + "\""));
        assertTrue(query.contains("marketing"));
        assertTrue(query.contains("terms_set"));
        assertTrue(query.contains(PERMISSIONS_FIELD));
    }

    /** User holds grants on multiple spaces → all space IDs are merged into a single DLS query. */
    public void testMultipleSpacesAreMerged() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "space:foo", "space:bar");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        String query = result.iterator().next().getQuery().utf8ToString();
        assertTrue(query.contains("\"" + SPACES_FIELD + "\""));
        assertTrue(query.contains("foo"));
        assertTrue(query.contains("bar"));
    }

    /** User holds the wildcard resource * → full access, no DLS. */
    public void testWildcardResourceGrantsFullAccessWithoutDls() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "*");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(SML_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery(), is(nullValue()));
    }

    /** When * and specific spaces both appear, wildcard wins → no DLS. */
    public void testWildcardTakesPrecedenceOverSpecificSpaces() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "*", "space:foo");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery(), is(nullValue()));
    }

    /** Privilege on a different application → empty (provider does not apply). */
    public void testNonMatchingApplicationReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor("other-app", "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("other-app")
                    .privileges("sml_read")
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

    /** Privilege action does not include login: → empty (provider only triggers on login:). */
    public void testNonMatchingActionReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_write", Set.of("saved_object:dashboard/create"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_write", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    /** Resources without the "space:" prefix are ignored; if no valid space IDs remain → empty. */
    public void testResourcesWithoutSpacePrefixAreIgnored() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "no-prefix-resource");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    /**
     * Role writes login: as the raw action pattern directly (no stored descriptor). The raw-pattern
     * branch in privilege resolution should still trigger the provider and produce a DLS query.
     */
    public void testEmptyStoredPrivilegesWithLoginActionStillWorks() {
        // No stored descriptors; login: is used as a raw action pattern.
        RoleDescriptor roleDescriptor = role(LOGIN_ACTION, "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery(), is(notNullValue()));
        assertTrue(result.iterator().next().getQuery().utf8ToString().contains("default"));
    }

    /** DLS query must contain a terms_set clause referencing permissions_count and the privilege actions. */
    public void testDlsQueryIncludesPrivilegeActions() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_sml", Set.of(LOGIN_ACTION, "saved_object:lens/get"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("feature_sml", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        String query = result.iterator().next().getQuery().utf8ToString();
        assertTrue("expected terms_set in query", query.contains("terms_set"));
        assertTrue("expected permissions field in query", query.contains(PERMISSIONS_FIELD));
        assertTrue("expected permissions_count field in query", query.contains(PERMISSIONS_COUNT_FIELD));
        assertTrue("expected privilege action saved_object:lens/get in query", query.contains("saved_object:lens/get"));
    }

    /** DLS query must include a must_not exists clause so documents with no permissions are visible. */
    public void testDlsQueryAllowsDocsWithNoPermissions() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        String query = result.iterator().next().getQuery().utf8ToString();
        assertTrue("expected must_not in query", query.contains("must_not"));
        assertTrue("expected exists in query", query.contains("exists"));
        assertTrue("expected permissions field in exists clause", query.contains(PERMISSIONS_FIELD));
    }

    /** When two grants with different action sets both match login:, all actions are unioned in the terms_set. */
    public void testMultiplePrivilegeActionsAreUnioned() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "sml_dashboard",
                Set.of(LOGIN_ACTION, "saved_object:dashboard/get"),
                Map.of()
            ),
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_lens", Set.of(LOGIN_ACTION, "saved_object:lens/get"), Map.of())
        );
        // Two separate grants, each on a different space, each contributing different actions.
        RoleDescriptor roleDescriptor = new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges("sml_dashboard")
                    .resources("space:foo")
                    .build(),
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges("sml_lens")
                    .resources("space:foo")
                    .build() },
            null,
            null,
            null,
            null
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        String query = result.iterator().next().getQuery().utf8ToString();
        assertTrue("expected saved_object:dashboard/get in query", query.contains("saved_object:dashboard/get"));
        assertTrue("expected saved_object:lens/get in query", query.contains("saved_object:lens/get"));
    }

    /** Static helper produces valid JSON with the required structural elements. */
    public void testBuildDlsQueryFormat() {
        String query = KibanaSmlImplicitPrivilegesProvider.buildDlsQuery(
            Set.of("marketing", "finance"),
            Set.of(LOGIN_ACTION, "saved_object:dashboard/get")
        );

        // Top-level bool/must structure
        assertTrue("expected bool in query", query.contains("bool"));
        assertTrue("expected must in query", query.contains("must"));

        // Space filter
        assertTrue("expected spaces field in query", query.contains("\"" + SPACES_FIELD + "\""));
        assertTrue("expected marketing in query", query.contains("marketing"));
        assertTrue("expected finance in query", query.contains("finance"));
        // Global wildcard term for "spaces: *" documents
        assertTrue("expected global wildcard * in space clause", query.contains("\"*\""));

        // Privilege filter
        assertTrue("expected terms_set in query", query.contains("terms_set"));
        assertTrue("expected permissions field in query", query.contains(PERMISSIONS_FIELD));
        assertTrue("expected permissions_count field in query", query.contains(PERMISSIONS_COUNT_FIELD));
        assertTrue("expected must_not/exists in query", query.contains("must_not"));
        assertTrue("expected exists in query", query.contains("exists"));

        // No boost noise from QueryBuilders
        assertFalse("unexpected boost field in hand-rolled query", query.contains("boost"));
    }

    // -------------------------------------------------------------------------------------
    // Helpers (mirrors the pattern from KibanaAlertsImplicitPrivilegesProviderTests)
    // -------------------------------------------------------------------------------------

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
