/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
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
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.xpack.kibana.KibanaNightshiftImplicitPrivilegesProvider.KIBANA_APPLICATION;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KibanaNightshiftImplicitPrivilegesProviderTests extends ESTestCase {

    private static final String READ_ACTION = "api:read_nightshift";
    private static final String MANAGE_ACTION = "api:manage_nightshift";
    private static final String[] SIGNIFICANT_EVENTS_INDICES = { ".significant_events-*" };

    private final KibanaNightshiftImplicitPrivilegesProvider provider = new KibanaNightshiftImplicitPrivilegesProvider();

    public void testSingleSpaceGrantsDlsQuery() {
        RoleDescriptor.IndicesPrivileges privilege = singleGrant(storedReadPrivilege("feature_nightshift.read"), "space:default");

        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(SIGNIFICANT_EVENTS_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery(), is(notNullValue()));
        assertThat(spaceIdsInQuery(privilege), containsInAnyOrder("default"));
    }

    public void testMultipleSpacesInSingleRoleAreMerged() {
        RoleDescriptor.IndicesPrivileges privilege = singleGrant(
            storedReadPrivilege("feature_nightshift.read"),
            "space:foo",
            "space:bar",
            "space:baz"
        );

        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(SIGNIFICANT_EVENTS_INDICES));
        assertThat(spaceIdsInQuery(privilege), containsInAnyOrder("foo", "bar", "baz"));
    }

    public void testWildcardResourceGrantsFullAccessWithoutDls() {
        RoleDescriptor.IndicesPrivileges privilege = singleGrant(storedReadPrivilege("feature_nightshift.read"), "*");

        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(SIGNIFICANT_EVENTS_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery(), is(nullValue()));
    }

    public void testWildcardTakesPrecedenceOverSpecificSpaces() {
        RoleDescriptor.IndicesPrivileges privilege = singleGrant(storedReadPrivilege("feature_nightshift.read"), "*", "space:foo");
        assertThat(privilege.getQuery(), is(nullValue()));
    }

    public void testPrivilegeWithMultipleActionsIncludingRead() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_nightshift.all", Set.of(READ_ACTION, MANAGE_ACTION), Map.of())
        );
        RoleDescriptor.IndicesPrivileges privilege = singleGrant(stored, "space:marketing");
        assertThat(spaceIdsInQuery(privilege), containsInAnyOrder("marketing"));
    }

    public void testNonMatchingActionReturnsEmpty() {
        // read_stream alone (Streams feature) does not authorize the Significant Events read routes.
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_streams.read", Set.of("api:read_stream"), Map.of())
        );
        assertThat(grants(role("feature_streams.read", "space:default"), stored), is(empty()));
    }

    public void testNonMatchingApplicationReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor("other-app", "feature_nightshift.read", Set.of(READ_ACTION), Map.of())
        );
        assertThat(grants(roleWithApplication("other-app", "feature_nightshift.read", "space:default"), stored), is(empty()));
    }

    public void testNonKibanaWildcardAppDoesNotMatch() {
        assertThat(grants(roleWithApplication("shield*", "api:*", "space:default"), List.of()), is(empty()));
    }

    public void testResourcesWithoutSpacePrefixAreIgnored() {
        assertThat(
            grants(role("feature_nightshift.read", "no-prefix-resource"), storedReadPrivilege("feature_nightshift.read")),
            is(empty())
        );
    }

    public void testEmptyStoredPrivilegesReturnsEmpty() {
        assertThat(grants(role("feature_nightshift.read", "space:default"), List.of()), is(empty()));
    }

    public void testRoleWithWildcardApplicationName() {
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(
            roleWithApplication("kibana-*", "feature_nightshift.read", "space:default"),
            storedReadPrivilege("feature_nightshift.read")
        );
        assertThat(result, hasSize(2));
        assertThat(spaceIdsInQuery(significantEventsGrant(result)), containsInAnyOrder("default"));
        assertThat(viewGrant(result, "read_view_metadata").isPresent(), is(true));
    }

    public void testRoleWithRawActionPatterns() {
        String pattern = randomFrom(READ_ACTION, "api:*", "api:read_*");
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(role(pattern, "space:default"), List.of());
        assertThat(spaceIdsInQuery(significantEventsGrant(result)), containsInAnyOrder("default"));
        assertThat(viewGrant(result, "read_view_metadata").isPresent(), is(true));
        assertThat(viewGrant(result, "manage_view").isPresent(), is(pattern.equals("api:*")));
    }

    public void testRoleWithSuperWildcardPrivilege() {
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(role("*", "*"), List.of());
        assertThat(result, hasSize(3));
        assertThat(significantEventsGrant(result).getQuery(), is(nullValue()));
        assertThat(viewGrant(result, "read_view_metadata").orElseThrow().getIndices(), arrayContainingInAnyOrder("$.nightshift.sources.*"));
        assertThat(viewGrant(result, "manage_view").orElseThrow().getIndices(), arrayContainingInAnyOrder("$.nightshift.sources.*"));
    }

    public void testReadInOneSpaceGrantsReadViewPrivilegesOnThatSpacePattern() {
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(
            role("feature_nightshift.read", "space:marketing"),
            storedReadPrivilege("feature_nightshift.read")
        );

        assertThat(result, hasSize(2));
        RoleDescriptor.IndicesPrivileges view = viewGrant(result, "read_view_metadata").orElseThrow();
        assertThat(view.getIndices(), arrayContainingInAnyOrder("$.nightshift.sources.marketing.*"));
        assertThat(view.getPrivileges(), arrayContainingInAnyOrder("read", "read_view_metadata"));
        assertThat(view.getQuery(), is(nullValue()));
        assertThat(viewGrant(result, "manage_view").isPresent(), is(false));
    }

    public void testManageInOneSpaceGrantsManageViewOnThatSpacePattern() {
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(
            role("feature_nightshift.manage", "space:marketing"),
            List.of(new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_nightshift.manage", Set.of(MANAGE_ACTION), Map.of()))
        );

        assertThat(result, hasSize(1));
        RoleDescriptor.IndicesPrivileges view = result.iterator().next();
        assertThat(view.getIndices(), arrayContainingInAnyOrder("$.nightshift.sources.marketing.*"));
        assertThat(view.getPrivileges(), arrayContainingInAnyOrder("manage_view"));
        assertThat(view.getQuery(), is(nullValue()));
    }

    public void testAllPrivilegeGrantsReadAndManageViewsOnTheSameSpace() {
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(
            role("feature_nightshift.all", "space:marketing"),
            List.of(
                new ApplicationPrivilegeDescriptor(
                    KIBANA_APPLICATION,
                    "feature_nightshift.all",
                    Set.of(READ_ACTION, MANAGE_ACTION),
                    Map.of()
                )
            )
        );

        assertThat(result, hasSize(3));
        assertThat(
            viewGrant(result, "read_view_metadata").orElseThrow().getIndices(),
            arrayContainingInAnyOrder("$.nightshift.sources.marketing.*")
        );
        assertThat(
            viewGrant(result, "manage_view").orElseThrow().getIndices(),
            arrayContainingInAnyOrder("$.nightshift.sources.marketing.*")
        );
    }

    public void testReadAndManageInDifferentSpacesGetSeparateViewGrants() {
        Collection<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_nightshift.read", Set.of(READ_ACTION), Map.of()),
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_nightshift.manage", Set.of(MANAGE_ACTION), Map.of())
        );
        RoleDescriptor role = new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] {
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges("feature_nightshift.read")
                    .resources("space:a")
                    .build(),
                RoleDescriptor.ApplicationResourcePrivileges.builder()
                    .application(KIBANA_APPLICATION)
                    .privileges("feature_nightshift.manage")
                    .resources("space:b")
                    .build() },
            null,
            null,
            null,
            null
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = grants(role, stored);

        assertThat(
            viewGrant(result, "read_view_metadata").orElseThrow().getIndices(),
            arrayContainingInAnyOrder("$.nightshift.sources.a.*")
        );
        assertThat(viewGrant(result, "manage_view").orElseThrow().getIndices(), arrayContainingInAnyOrder("$.nightshift.sources.b.*"));
    }

    public void testMultipleSpacesYieldOneViewPatternEach() {
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(
            role("feature_nightshift.read", "space:foo", "space:bar"),
            storedReadPrivilege("feature_nightshift.read")
        );
        assertThat(
            viewGrant(result, "read_view_metadata").orElseThrow().getIndices(),
            arrayContainingInAnyOrder("$.nightshift.sources.foo.*", "$.nightshift.sources.bar.*")
        );
    }

    public void testWildcardResourceGrantsAllSourceViewsAndDropsSpacePatterns() {
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(
            role("feature_nightshift.read", "*", "space:foo"),
            storedReadPrivilege("feature_nightshift.read")
        );
        assertThat(viewGrant(result, "read_view_metadata").orElseThrow().getIndices(), arrayContainingInAnyOrder("$.nightshift.sources.*"));
    }

    public void testSpaceIdsThatCouldMatchOtherSpacesProduceNoViewPattern() {
        String badSpace = randomFrom("space:*", "space:foo*", "space:a.b", "space:fo?", "space:Foo", "space:");
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(
            role("feature_nightshift.all", badSpace, "space:good"),
            List.of(
                new ApplicationPrivilegeDescriptor(
                    KIBANA_APPLICATION,
                    "feature_nightshift.all",
                    Set.of(READ_ACTION, MANAGE_ACTION),
                    Map.of()
                )
            )
        );
        assertThat(
            viewGrant(result, "read_view_metadata").orElseThrow().getIndices(),
            arrayContainingInAnyOrder("$.nightshift.sources.good.*")
        );
        assertThat(viewGrant(result, "manage_view").orElseThrow().getIndices(), arrayContainingInAnyOrder("$.nightshift.sources.good.*"));
    }

    public void testManageOnlyDoesNotGrantSignificantEventsRead() {
        Collection<RoleDescriptor.IndicesPrivileges> result = grants(
            role("feature_nightshift.manage", "space:marketing"),
            List.of(new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_nightshift.manage", Set.of(MANAGE_ACTION), Map.of()))
        );
        assertThat(result.stream().anyMatch(g -> Arrays.equals(g.getIndices(), SIGNIFICANT_EVENTS_INDICES)), is(false));
    }

    public void testBuildSpaceIdsDlsQueryIncludesSpaceAgnosticDocuments() {
        Map<String, Object> query = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            KibanaNightshiftImplicitPrivilegesProvider.buildSpaceIdsDlsQuery(Set.of("default")),
            false
        );
        assertThat(query, equalTo(XContentHelper.convertToMap(JsonXContent.jsonXContent, """
            {
              "bool": {
                "should": [
                  { "terms": { "kibana.space_ids": ["default"] } },
                  { "bool": { "must_not": [ { "exists": { "field": "kibana.space_ids" } } ] } }
                ],
                "minimum_should_match": 1
              }
            }""", false)));
    }

    private Collection<ApplicationPrivilegeDescriptor> storedReadPrivilege(String name) {
        return List.of(new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, name, Set.of(READ_ACTION), Map.of()));
    }

    private RoleDescriptor.IndicesPrivileges singleGrant(Collection<ApplicationPrivilegeDescriptor> stored, String... resources) {
        String privilegeName = stored.iterator().next().getName();
        return significantEventsGrant(grants(role(privilegeName, resources), stored));
    }

    private static RoleDescriptor.IndicesPrivileges significantEventsGrant(Collection<RoleDescriptor.IndicesPrivileges> grants) {
        List<RoleDescriptor.IndicesPrivileges> matching = grants.stream()
            .filter(g -> Arrays.equals(g.getIndices(), SIGNIFICANT_EVENTS_INDICES))
            .toList();
        assertThat(matching, hasSize(1));
        return matching.get(0);
    }

    private static Optional<RoleDescriptor.IndicesPrivileges> viewGrant(
        Collection<RoleDescriptor.IndicesPrivileges> grants,
        String privilege
    ) {
        return grants.stream()
            .filter(g -> Arrays.stream(g.getIndices()).allMatch(i -> i.startsWith("$.nightshift.sources.")))
            .filter(g -> Arrays.asList(g.getPrivileges()).contains(privilege))
            .findFirst();
    }

    private Collection<RoleDescriptor.IndicesPrivileges> grants(RoleDescriptor role, Collection<ApplicationPrivilegeDescriptor> stored) {
        return provider.getImplicitIndicesPrivileges(resolve(role, stored));
    }

    @SuppressWarnings("unchecked")
    private static List<String> spaceIdsInQuery(RoleDescriptor.IndicesPrivileges privilege) {
        Map<String, Object> query = XContentHelper.convertToMap(privilege.getQuery(), false, XContentType.JSON).v2();
        Map<String, Object> bool = (Map<String, Object>) query.get("bool");
        List<Map<String, Object>> should = (List<Map<String, Object>>) bool.get("should");
        Map<String, Object> terms = (Map<String, Object>) should.get(0).get("terms");
        return (List<String>) terms.get("kibana.space_ids");
    }

    /**
     * Resolves a role's declared application privileges into {@link ResolvedApplicationPrivilege}s exactly as
     * {@code CompositeRolesStore} does before invoking the provider.
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
