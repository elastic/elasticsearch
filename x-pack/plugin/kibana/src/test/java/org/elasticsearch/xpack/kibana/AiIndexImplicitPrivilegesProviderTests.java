/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
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
import java.util.Set;

import static org.elasticsearch.xpack.kibana.AiIndexImplicitPrivilegesProvider.AI_INDEX_INDICES;
import static org.elasticsearch.xpack.kibana.AiIndexImplicitPrivilegesProvider.KIBANA_APPLICATION;
import static org.elasticsearch.xpack.kibana.AiIndexImplicitPrivilegesProvider.PERMISSIONS_FIELD;
import static org.elasticsearch.xpack.kibana.AiIndexImplicitPrivilegesProvider.SCOPE_SEPARATOR;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class AiIndexImplicitPrivilegesProviderTests extends ESTestCase {

    private static final String LOGIN_ACTION = "login:";

    private final AiIndexImplicitPrivilegesProvider contributor = new AiIndexImplicitPrivilegesProvider();

    /** User holds login: + a saved_object action on a single space → DLS query with composite scoped privilege. */
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
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(AI_INDEX_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery(), is(notNullValue()));

        Map<String, Object> queryMap = parseQuery(privilege.getQuery());
        assertQueryContainsTerm(queryMap, "marketing" + SCOPE_SEPARATOR + "saved_object:dashboard/get");
        assertQueryHasTermsSet(queryMap);
        assertQueryHasPublicDocBranch(queryMap);
    }

    /** User holds grants on multiple spaces → composite scoped privileges for all space × action combinations in the DLS query. */
    public void testMultipleSpacesProduceCompositeScopedPrivileges() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "space:foo", "space:bar");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        Map<String, Object> queryMap = parseQuery(result.iterator().next().getQuery());
        assertQueryContainsTerm(queryMap, "foo" + SCOPE_SEPARATOR + LOGIN_ACTION);
        assertQueryContainsTerm(queryMap, "bar" + SCOPE_SEPARATOR + LOGIN_ACTION);
    }

    /**
     * User holds the wildcard resource * → DLS query with "*|action" scoped privileges.
     * The wildcard is NOT a bypass — it produces tokens with "*" as the space component.
     */
    public void testWildcardResourceProducesDlsQueryWithWildcardTokens() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "*");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(AI_INDEX_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        // Must NOT be null — wildcard is not a bypass
        assertThat(privilege.getQuery(), is(notNullValue()));

        Map<String, Object> queryMap = parseQuery(privilege.getQuery());
        assertQueryContainsTerm(queryMap, "*" + SCOPE_SEPARATOR + LOGIN_ACTION);
        assertQueryHasTermsSet(queryMap);
    }

    /** When * and specific spaces both appear, both produce tokens in the DLS query. */
    public void testWildcardAndSpecificSpacesBothProduceTokens() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "*", "space:foo");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        Map<String, Object> queryMap = parseQuery(result.iterator().next().getQuery());
        assertQueryContainsTerm(queryMap, "*" + SCOPE_SEPARATOR + LOGIN_ACTION);
        assertQueryContainsTerm(queryMap, "foo" + SCOPE_SEPARATOR + LOGIN_ACTION);
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

    /** A role with application "kibana-*" resolves to a residual privilege whose getApplication() is still "kibana-*". */
    public void testWildcardApplicationNameMatchesKibana() {
        // A role with application "kibana-*" resolves to a residual privilege
        // whose getApplication() is still "kibana-*". The provider must match it.
        RoleDescriptor roleDescriptor = roleWithApplication("kibana-*", "saved_object:dashboard/get", "space:default");
        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
    }

    /** Privilege without login: still triggers the provider — any kibana-.kibana application privilege is sufficient. */
    public void testNonLoginActionStillTriggersProvider() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_write", Set.of("saved_object:dashboard/create"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_write", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));
        Map<String, Object> queryMap = parseQuery(result.iterator().next().getQuery());
        assertQueryContainsTerm(queryMap, "default" + SCOPE_SEPARATOR + "saved_object:dashboard/create");
    }

    /** Resources without the "space:" prefix and not equal to "*" are ignored; if no valid resources remain → empty. */
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
     * Role writes a raw action pattern directly (no stored descriptor). The raw-pattern
     * branch in privilege resolution should still trigger the provider and produce a DLS query.
     */
    public void testEmptyStoredPrivilegesWithRawActionStillWorks() {
        // No stored descriptors; action pattern is used directly.
        RoleDescriptor roleDescriptor = role("saved_object:dashboard/get", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery(), is(notNullValue()));
        assertQueryContainsTerm(
            parseQuery(result.iterator().next().getQuery()),
            "default" + SCOPE_SEPARATOR + "saved_object:dashboard/get"
        );
    }

    /** DLS query must contain a terms_set clause referencing count field and composite scoped privileges. */
    public void testDlsQueryIncludesCompositeScopedPrivileges() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_sml", Set.of(LOGIN_ACTION, "saved_object:lens/get"), Map.of())
        );
        RoleDescriptor roleDescriptor = role("feature_sml", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        Map<String, Object> queryMap = parseQuery(result.iterator().next().getQuery());
        assertQueryHasTermsSet(queryMap);
        assertQueryHasPublicDocBranch(queryMap);
        assertQueryContainsTerm(queryMap, "default" + SCOPE_SEPARATOR + "saved_object:lens/get");
    }

    /** DLS query must include a must_not exists clause so documents with no permissions tokens are visible. */
    public void testDlsQueryAllowsDocsWithNoScopedPrivileges() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of(LOGIN_ACTION), Map.of())
        );
        RoleDescriptor roleDescriptor = role("sml_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        Map<String, Object> queryMap = parseQuery(result.iterator().next().getQuery());
        assertQueryHasPublicDocBranch(queryMap);
    }

    /**
     * Two grants with different action sets on different spaces produce the correct cross-product
     * of composite scoped privileges — one token per space × action combination.
     */
    public void testMultiplePrivilegesAndSpacesProduceCrossProductScopedPrivileges() {
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

        Map<String, Object> queryMap = parseQuery(result.iterator().next().getQuery());
        // foo space tokens from sml_dashboard
        assertQueryContainsTerm(queryMap, "foo" + SCOPE_SEPARATOR + "saved_object:dashboard/get");
        assertQueryContainsTerm(queryMap, "foo" + SCOPE_SEPARATOR + LOGIN_ACTION);
        // bar space tokens from sml_lens
        assertQueryContainsTerm(queryMap, "bar" + SCOPE_SEPARATOR + "saved_object:lens/get");
        assertQueryContainsTerm(queryMap, "bar" + SCOPE_SEPARATOR + LOGIN_ACTION);

        // foo tokens from sml_dashboard should NOT appear for bar and vice versa
        assertQueryDoesNotContainTerm(queryMap, "foo" + SCOPE_SEPARATOR + "saved_object:lens/get");
        assertQueryDoesNotContainTerm(queryMap, "bar" + SCOPE_SEPARATOR + "saved_object:dashboard/get");
    }

    /** Static helper: buildDlsQuery produces valid JSON with the required structural elements. */
    @SuppressWarnings("unchecked")
    public void testBuildDlsQueryFormat() {
        Set<String> tokens = Set.of(
            "marketing" + SCOPE_SEPARATOR + LOGIN_ACTION,
            "marketing" + SCOPE_SEPARATOR + "saved_object:dashboard/get"
        );
        String query = AiIndexImplicitPrivilegesProvider.buildDlsQuery(tokens);

        Map<String, Object> queryMap = parseQueryString(query);

        // Top-level bool/should structure
        assertThat(queryMap, hasKey("bool"));
        Map<String, Object> boolClause = (Map<String, Object>) queryMap.get("bool");
        assertThat(boolClause, hasKey("should"));

        List<Map<String, Object>> shouldClauses = (List<Map<String, Object>>) boolClause.get("should");
        assertThat(shouldClauses, hasSize(2));

        // Public-document branch: bool/must_not/exists
        Map<String, Object> publicBranch = shouldClauses.stream().filter(c -> c.containsKey("bool")).findFirst().orElse(null);
        assertThat("expected a bool/must_not branch", publicBranch, is(notNullValue()));
        // must_not is serialized as an array by BoolQueryBuilder
        List<Map<String, Object>> mustNotList = (List<Map<String, Object>>) ((Map<String, Object>) publicBranch.get("bool")).get(
            "must_not"
        );
        assertThat("expected exactly one must_not clause", mustNotList, hasSize(1));
        Map<String, Object> exists = (Map<String, Object>) mustNotList.get(0).get("exists");
        assertThat("expected exists in must_not clause", exists, is(notNullValue()));
        assertThat("expected PERMISSIONS_FIELD in exists", exists.get("field"), is(PERMISSIONS_FIELD));

        // Scoped-privilege-match branch: terms_set
        Map<String, Object> termsSetBranch = shouldClauses.stream().filter(c -> c.containsKey("terms_set")).findFirst().orElse(null);
        assertThat("expected a terms_set branch", termsSetBranch, is(notNullValue()));
        Map<String, Object> termsSetField = (Map<String, Object>) ((Map<String, Object>) termsSetBranch.get("terms_set")).get(
            PERMISSIONS_FIELD
        );
        List<String> terms = (List<String>) termsSetField.get("terms");
        assertThat(
            terms,
            containsInAnyOrder("marketing" + SCOPE_SEPARATOR + LOGIN_ACTION, "marketing" + SCOPE_SEPARATOR + "saved_object:dashboard/get")
        );

        // No old-style spaces or dls_tokens fields
        assertFalse("unexpected old spaces field", query.contains("\"spaces\""));
        assertFalse("unexpected old dls_tokens field", query.contains("\"dls_tokens\""));
        assertFalse("unexpected old dls_tokens_count field", query.contains("\"dls_tokens_count\""));
    }

    /** buildScopedPrivileges correctly crosses space IDs with action strings, including the wildcard resource. */
    public void testBuildScopedPrivileges() {
        Map<String, Set<String>> resourcesAndActions = Map.of(
            "space:marketing",
            Set.of(LOGIN_ACTION, "saved_object:dashboard/get"),
            "space:finance",
            Set.of(LOGIN_ACTION),
            "*",
            Set.of(LOGIN_ACTION),
            "no-prefix-resource",
            Set.of(LOGIN_ACTION)
        );

        Set<String> scopedPrivileges = AiIndexImplicitPrivilegesProvider.buildScopedPrivileges(resourcesAndActions);

        assertTrue("expected marketing|login:", scopedPrivileges.contains("marketing" + SCOPE_SEPARATOR + LOGIN_ACTION));
        assertTrue(
            "expected marketing|saved_object:dashboard/get",
            scopedPrivileges.contains("marketing" + SCOPE_SEPARATOR + "saved_object:dashboard/get")
        );
        assertTrue("expected finance|login:", scopedPrivileges.contains("finance" + SCOPE_SEPARATOR + LOGIN_ACTION));
        assertTrue("expected *|login: for wildcard resource", scopedPrivileges.contains("*" + SCOPE_SEPARATOR + LOGIN_ACTION));
        // Resources without "space:" prefix (and not "*") must be excluded
        assertFalse(
            "no-prefix-resource should not produce tokens",
            scopedPrivileges.stream().anyMatch(t -> t.startsWith("no-prefix-resource"))
        );
        assertThat(scopedPrivileges, hasSize(4));
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

    private Map<String, Object> parseQuery(BytesReference queryBytes) {
        return parseQueryString(queryBytes.utf8ToString());
    }

    private Map<String, Object> parseQueryString(String json) {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.map();
        } catch (Exception e) {
            throw new AssertionError("Failed to parse query JSON", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> extractTermsSetTerms(Map<String, Object> queryMap) {
        Map<String, Object> boolClause = (Map<String, Object>) queryMap.get("bool");
        List<Map<String, Object>> shouldClauses = (List<Map<String, Object>>) boolClause.get("should");
        for (Map<String, Object> clause : shouldClauses) {
            if (clause.containsKey("terms_set")) {
                Map<String, Object> termsSet = (Map<String, Object>) clause.get("terms_set");
                Map<String, Object> fieldMap = (Map<String, Object>) termsSet.get(PERMISSIONS_FIELD);
                return (List<String>) fieldMap.get("terms");
            }
        }
        return List.of();
    }

    private static void assertQueryContainsTerm(Map<String, Object> queryMap, String expectedTerm) {
        List<String> terms = extractTermsSetTerms(queryMap);
        assertTrue("expected term [" + expectedTerm + "] in terms_set query, got: " + terms, terms.contains(expectedTerm));
    }

    private static void assertQueryDoesNotContainTerm(Map<String, Object> queryMap, String unexpectedTerm) {
        List<String> terms = extractTermsSetTerms(queryMap);
        assertFalse("unexpected term [" + unexpectedTerm + "] in terms_set query", terms.contains(unexpectedTerm));
    }

    @SuppressWarnings("unchecked")
    private static void assertQueryHasTermsSet(Map<String, Object> queryMap) {
        Map<String, Object> boolClause = (Map<String, Object>) queryMap.get("bool");
        List<Map<String, Object>> shouldClauses = (List<Map<String, Object>>) boolClause.get("should");
        assertTrue("expected a terms_set clause in should", shouldClauses.stream().anyMatch(c -> c.containsKey("terms_set")));
    }

    @SuppressWarnings("unchecked")
    private static void assertQueryHasPublicDocBranch(Map<String, Object> queryMap) {
        Map<String, Object> boolClause = (Map<String, Object>) queryMap.get("bool");
        List<Map<String, Object>> shouldClauses = (List<Map<String, Object>>) boolClause.get("should");
        boolean found = shouldClauses.stream().anyMatch(c -> {
            if (c.containsKey("bool") == false) return false;
            Map<String, Object> inner = (Map<String, Object>) c.get("bool");
            return inner.containsKey("must_not");
        });
        assertTrue("expected a bool/must_not/exists public-doc branch in should", found);
    }
}
