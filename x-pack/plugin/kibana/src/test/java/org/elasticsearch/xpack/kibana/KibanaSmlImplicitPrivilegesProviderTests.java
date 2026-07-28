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
import static org.elasticsearch.xpack.kibana.KibanaSmlImplicitPrivilegesProvider.TOKEN_SEPARATOR;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KibanaSmlImplicitPrivilegesProviderTests extends ESTestCase {

    private final KibanaSmlImplicitPrivilegesProvider contributor = new KibanaSmlImplicitPrivilegesProvider();

    /** User holds login: + a saved_object action on a single space → DLS query with composite token. */
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
        assertTrue(query.contains("\"" + PERMISSIONS_FIELD + "\""));
        assertTrue(query.contains("marketing" + TOKEN_SEPARATOR + "saved_object:dashboard/get"));
        assertTrue(query.contains("terms_set"));
        assertTrue(query.contains(PERMISSIONS_COUNT_FIELD));
    }

    /** User holds grants on multiple spaces → composite tokens for all space × action combinations in the DLS query. */
    public void testMultipleSpacesProduceCompositeTokens() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "space:foo", "space:bar");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        String query = result.iterator().next().getQuery().utf8ToString();
        assertTrue(query.contains("\"" + PERMISSIONS_FIELD + "\""));
        assertTrue(query.contains("foo" + TOKEN_SEPARATOR + LOGIN_ACTION));
        assertTrue(query.contains("bar" + TOKEN_SEPARATOR + LOGIN_ACTION));
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

    /** DLS query must contain a terms_set clause referencing dls_tokens_count and composite tokens. */
    public void testDlsQueryIncludesCompositeTokens() {
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
        assertTrue(
            "expected composite token default|saved_object:lens/get in query",
            query.contains("default" + TOKEN_SEPARATOR + "saved_object:lens/get")
        );
    }

    /** DLS query must include a must_not exists clause so documents with no permissions tokens are visible. */
    public void testDlsQueryAllowsDocsWithNoDlsTokens() {
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

    /**
     * Two grants with different action sets on different spaces produce the correct cross-product
     * of composite tokens — one token per space × action combination.
     */
    public void testMultiplePrivilegesAndSpacesProduceCrossProductTokens() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "sml_dashboard",
                Set.of(LOGIN_ACTION, "saved_object:dashboard/get"),
                Map.of()
            ),
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_lens", Set.of(LOGIN_ACTION, "saved_object:lens/get"), Map.of())
        );
        // Two separate grants: sml_dashboard on space:foo, sml_lens on space:bar.
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
                    .resources("space:bar")
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
        // foo space tokens from sml_dashboard
        assertTrue(
            "expected foo|saved_object:dashboard/get in query",
            query.contains("foo" + TOKEN_SEPARATOR + "saved_object:dashboard/get")
        );
        assertTrue("expected foo|login: in query", query.contains("foo" + TOKEN_SEPARATOR + LOGIN_ACTION));
        // bar space tokens from sml_lens
        assertTrue("expected bar|saved_object:lens/get in query", query.contains("bar" + TOKEN_SEPARATOR + "saved_object:lens/get"));
        assertTrue("expected bar|login: in query", query.contains("bar" + TOKEN_SEPARATOR + LOGIN_ACTION));
        // foo tokens from sml_dashboard should NOT appear for bar and vice versa
        assertFalse(
            "foo|saved_object:lens/get should NOT be in query (wrong space)",
            query.contains("foo" + TOKEN_SEPARATOR + "saved_object:lens/get")
        );
        assertFalse(
            "bar|saved_object:dashboard/get should NOT be in query (wrong space)",
            query.contains("bar" + TOKEN_SEPARATOR + "saved_object:dashboard/get")
        );
    }

    /** Static helper: buildDlsQuery produces valid JSON with the required structural elements. */
    public void testBuildDlsQueryFormat() {
        String query = KibanaSmlImplicitPrivilegesProvider.buildDlsQuery(
            Set.of("marketing" + TOKEN_SEPARATOR + LOGIN_ACTION, "marketing" + TOKEN_SEPARATOR + "saved_object:dashboard/get")
        );

        // Top-level bool/should structure
        assertTrue("expected bool in query", query.contains("bool"));
        assertTrue("expected should in query", query.contains("should"));

        // No must at top level (old structure removed)
        assertFalse("unexpected top-level must in new query", query.contains("\"must\""));

        // Public-document branch
        assertTrue("expected must_not in query", query.contains("must_not"));
        assertTrue("expected exists in query", query.contains("exists"));
        assertTrue("expected permissions field in exists clause", query.contains(PERMISSIONS_FIELD));

        // Token-match branch
        assertTrue("expected terms_set in query", query.contains("terms_set"));
        assertTrue("expected permissions_count field in query", query.contains(PERMISSIONS_COUNT_FIELD));
        assertTrue("expected composite token marketing|login: in query", query.contains("marketing" + TOKEN_SEPARATOR + LOGIN_ACTION));
        assertTrue(
            "expected composite token marketing|saved_object:dashboard/get in query",
            query.contains("marketing" + TOKEN_SEPARATOR + "saved_object:dashboard/get")
        );

        // No boost noise from QueryBuilders
        assertFalse("unexpected boost field in hand-rolled query", query.contains("boost"));

        // No old-style spaces or dls_tokens fields
        assertFalse("unexpected old spaces field", query.contains("\"spaces\""));
        assertFalse("unexpected old dls_tokens field", query.contains("\"dls_tokens\""));
        assertFalse("unexpected old dls_tokens_count field", query.contains("\"dls_tokens_count\""));
    }

    /** buildCompositeTokens correctly crosses space IDs with action strings. */
    public void testBuildCompositeTokens() {
        Map<String, Set<String>> resourcesAndActions = Map.of(
            "space:marketing",
            Set.of(LOGIN_ACTION, "saved_object:dashboard/get"),
            "space:finance",
            Set.of(LOGIN_ACTION),
            "no-prefix-resource",
            Set.of(LOGIN_ACTION)
        );

        Set<String> tokens = KibanaSmlImplicitPrivilegesProvider.buildCompositeTokens(resourcesAndActions);

        assertTrue("expected marketing|login:", tokens.contains("marketing" + TOKEN_SEPARATOR + LOGIN_ACTION));
        assertTrue(
            "expected marketing|saved_object:dashboard/get",
            tokens.contains("marketing" + TOKEN_SEPARATOR + "saved_object:dashboard/get")
        );
        assertTrue("expected finance|login:", tokens.contains("finance" + TOKEN_SEPARATOR + LOGIN_ACTION));
        // Resources without "space:" prefix must be excluded
        assertFalse("no-prefix-resource should not produce tokens", tokens.stream().anyMatch(t -> t.startsWith("no-prefix-resource")));
        assertThat(tokens, hasSize(3));
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
