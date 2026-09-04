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

import static org.elasticsearch.xpack.kibana.KibanaAlertsImplicitPrivilegesProvider.KIBANA_APPLICATION;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KibanaAlertsImplicitPrivilegesProviderTests extends ESTestCase {

    private static final String ALERTS_ACTION = "alerts:read";
    private static final String[] ALERTING_V2_INDICES = { ".alert-actions*", ".rule-events*" };

    private final KibanaAlertsImplicitPrivilegesProvider contributor = new KibanaAlertsImplicitPrivilegesProvider();

    public void testSingleSpaceGrantsDlsQuery() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_alerting_read", Set.of(ALERTS_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("feature_alerting_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(ALERTING_V2_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery(), is(notNullValue()));

        String query = privilege.getQuery().utf8ToString();
        assertTrue(query.contains("\"space_id\""));
        assertTrue(query.contains("default"));
        assertFalse(query.contains("kibana.space_ids"));
    }

    public void testMultipleSpacesInSingleRoleAreMerged() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "alerting_read", Set.of(ALERTS_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("alerting_read", "space:foo", "space:bar", "space:baz");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(ALERTING_V2_INDICES));

        String query = privilege.getQuery().utf8ToString();
        assertTrue(query.contains("\"space_id\""));
        assertTrue(query.contains("foo"));
        assertTrue(query.contains("bar"));
        assertTrue(query.contains("baz"));
    }

    public void testWildcardResourceGrantsFullAccessWithoutDls() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "alerting_read", Set.of(ALERTS_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("alerting_read", "*");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(ALERTING_V2_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery(), is(nullValue()));
    }

    public void testWildcardTakesPrecedenceOverSpecificSpaces() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "alerting_read", Set.of(ALERTS_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("alerting_read", "*", "space:foo");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery(), is(nullValue()));
    }

    public void testNonMatchingApplicationReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor("other-app", "alerting_read", Set.of(ALERTS_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application("other-app")
                    .privileges("alerting_read")
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
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "alerting_write", Set.of("alerts:write"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("alerting_write", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    public void testResourcesWithoutSpacePrefixAreIgnored() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "alerting_read", Set.of(ALERTS_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("alerting_read", "no-prefix-resource");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    public void testEmptyStoredPrivilegesReturnsEmpty() {
        RoleDescriptor roleDescriptor = role("alerting_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, is(empty()));
    }

    public void testPrivilegeWithMultipleActionsIncludingAlertsRead() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_all",
                Set.of(ALERTS_ACTION, "alerts:write", "rules:read"),
                Map.of()
            )
        );
        RoleDescriptor roleDescriptor = role("feature_all", "space:marketing");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(ALERTING_V2_INDICES));

        String query = privilege.getQuery().utf8ToString();
        assertTrue(query.contains("marketing"));
    }

    public void testBuildSpaceIdsDlsQuery() {
        String query = KibanaAlertsImplicitPrivilegesProvider.buildSpaceIdsDlsQuery(Set.of("default"));
        assertTrue(query.contains("terms"));
        assertTrue(query.contains("\"space_id\""));
        assertTrue(query.contains("default"));
    }

    public void testStoredPrivilegeWithWildcardActionPattern() {
        // Hypothetical "feature_kibana_all" privilege whose actions are a single wildcard covering alerts:*.
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_kibana_all", Set.of("alerts:*"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("feature_kibana_all", "space:marketing");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("marketing"));
    }

    public void testRoleWithWildcardApplicationName() {
        // Role declares "kibana-*" instead of literal "kibana-.kibana".
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_alerting_v2_alerts.read", Set.of(ALERTS_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = roleWithApplication("kibana-*", "feature_alerting_v2_alerts.read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("default"));
    }

    public void testRoleWithRawActionPatternNoStoredDescriptors() {
        // Role embeds the action name directly under privileges, NativePrivilegeStore returns nothing,
        // but the raw-pattern branch should still pick this up.
        RoleDescriptor roleDescriptor = role(ALERTS_ACTION, "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("default"));
    }

    public void testRoleWithWildcardPrivilegePattern() {
        // privileges=["alerts:*"] - wildcard pattern that matches the alerts action.
        RoleDescriptor roleDescriptor = role("alerts:*", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("default"));
    }

    public void testRoleWithSuperWildcardPrivilege() {
        // privileges=["*"] - matches everything including the alerts action.
        RoleDescriptor roleDescriptor = role("*", "*");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        // resources contains "*" → no DLS
        assertThat(result.iterator().next().getQuery(), is(nullValue()));
    }

    public void testRoleWithWildcardAppAndRawActionPattern() {
        // Combined: application wildcard + privilege pattern, no stored descriptors needed.
        RoleDescriptor roleDescriptor = roleWithApplication("kibana-*", "alerts:*", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery().utf8ToString(), containsString("default"));
    }

    public void testNonKibanaWildcardAppDoesNotMatch() {
        // "shield*" must not match "kibana-.kibana" even when the privilege pattern would otherwise match the alerts action.
        RoleDescriptor roleDescriptor = roleWithApplication("shield*", "alerts:*", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, is(empty()));
    }

    public void testStoredPrivilegeWithDifferentActionDoesNotMatch() {
        // A stored privilege whose action set is unrelated to alerts:read should not trigger the provider.
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_rules_only", Set.of("rules:read"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("feature_rules_only", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
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
