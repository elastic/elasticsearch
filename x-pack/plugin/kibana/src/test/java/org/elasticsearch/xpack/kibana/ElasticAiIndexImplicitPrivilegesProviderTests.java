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

import static org.elasticsearch.xpack.kibana.ElasticAiIndexImplicitPrivilegesProvider.COUNT_FIELD;
import static org.elasticsearch.xpack.kibana.ElasticAiIndexImplicitPrivilegesProvider.ELASTIC_AI_INDEX;
import static org.elasticsearch.xpack.kibana.ElasticAiIndexImplicitPrivilegesProvider.KIBANA_APPLICATION;
import static org.elasticsearch.xpack.kibana.ElasticAiIndexImplicitPrivilegesProvider.NAME_FIELD;
import static org.elasticsearch.xpack.kibana.ElasticAiIndexImplicitPrivilegesProvider.SPACE_FIELD;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ElasticAiIndexImplicitPrivilegesProviderTests extends ESTestCase {

    private static final String ALL_SPACES = "*";

    private final ElasticAiIndexImplicitPrivilegesProvider contributor = new ElasticAiIndexImplicitPrivilegesProvider();

    /**
     * A user with actions in one space produces a nested query whose single should-clause
     * pairs a term on .space with a terms_set on .name gated by the per-element count.
     */
    public void testSingleSpaceProducesNestedSpaceClause() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_sml_read", Set.of("ai_index:dashboard/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("feature_sml_read", "space:marketing"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(ELASTIC_AI_INDEX));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));

        assertThat(privilege.getQuery().utf8ToString(), equalTo(EXPECTED_SINGLE_SPACE_QUERY));
    }

    /**
     * The public-document branch must be must_not(nested(match_all)), never must_not(exists(...)).
     * A root-level exists on a nested subfield matches every document — the values live on child
     * docs — which would make the first should-clause match everything and void the whole DLS query.
     */
    public void testPublicDocumentBranchUsesNestedMatchAllNotExists() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of("ai_index:dashboard/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_read", "space:marketing"), storedPrivileges)
        );

        String query = result.iterator().next().getQuery().utf8ToString();
        assertThat(query, containsString("\"must_not\""));
        assertThat(query, containsString("\"match_all\""));
        assertThat(query, not(containsString("\"exists\"")));
    }

    /**
     * Spaces sharing an identical effective action set are grouped into a single clause listing all
     * of them, rather than one clause per space: {@code (space=foo OR space=*) AND terms_set(A)} or-ed
     * with the same for {@code bar} is exactly {@code (space IN [foo,bar] OR space=*) AND terms_set(A)}.
     */
    public void testSpacesSharingActionSetShareOneClause() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of("ai_index:visualization/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_read", "space:foo", "space:bar"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(clauses, hasSize(1));
        assertThat(spacesOfClause(clauses.get(0)), containsInAnyOrder("foo", "bar", ALL_SPACES));
        assertThat(termsOfClause(clauses.get(0)), contains("ai_index:visualization/read"));
    }

    /**
     * A space whose effective action set equals the global set emits no clause of its own: the
     * space-less global clause applies the same {@code terms_set} with one restriction fewer, so it
     * already matches everything the space's clause would. Spaces adding actions keep their clause.
     */
    public void testSpaceClauseRedundantWithGlobalClauseIsDropped() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "global_read", Set.of("ai_index:dashboard/read"), Map.of()),
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "extra_read", Set.of("ai_index:workflow/read"), Map.of())
        );
        RoleDescriptor roleDescriptor = roleWithGrants(
            grant("global_read", "*"),
            grant("global_read", "space:marketing"),
            grant("extra_read", "space:finance")
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(clauses, hasSize(2));
        assertThat(termsOfClauseForSpace(clauses, "finance"), containsInAnyOrder("ai_index:dashboard/read", "ai_index:workflow/read"));
        assertThat(termsOfSpacelessClause(clauses), contains("ai_index:dashboard/read"));
        assertThat(
            "marketing adds nothing over the global grant, so no clause should name it",
            clauses.stream().anyMatch(clause -> spacesOfClause(clause).contains("marketing")),
            is(false)
        );
    }

    /**
     * The wildcard resource produces a <em>space-less</em> clause, not a clause whose space is the
     * literal string {@code "*"}. Documents live in real spaces, so a literal-{@code *} space term
     * would match nothing — a user with an all-spaces Kibana grant would see almost no documents.
     */
    public void testWildcardResourceProducesSpacelessClause() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of("ai_index:visualization/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_read", "*"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(ELASTIC_AI_INDEX));
        // Must NOT be null — the wildcard is not a bypass; the action check still applies.
        assertThat(privilege.getQuery(), is(notNullValue()));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(privilege.getQuery()));
        assertThat(clauses, hasSize(1));
        assertThat("wildcard clause must carry no .space filter", spacesOfClause(clauses.get(0)), is(empty()));
        assertThat(termsOfClause(clauses.get(0)), contains("ai_index:visualization/read"));
    }

    /**
     * A user holding action A globally and action B in 'marketing' then holds both in 'marketing',
     * so the 'marketing' clause must carry both. Without the union, a document requiring {A, B} in
     * marketing would be wrongly hidden. The additional space-less clause carries only the global actions.
     */
    public void testWildcardActionsAreUnionedIntoSpaceClauses() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "global_read", Set.of("ai_index:dashboard/read"), Map.of()),
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "space_read", Set.of("ai_index:workflow/read"), Map.of())
        );
        RoleDescriptor roleDescriptor = roleWithGrants(grant("global_read", "*"), grant("space_read", "space:marketing"));

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(clauses, hasSize(2));

        assertThat(termsOfClauseForSpace(clauses, "marketing"), containsInAnyOrder("ai_index:dashboard/read", "ai_index:workflow/read"));
        assertThat(termsOfSpacelessClause(clauses), contains("ai_index:dashboard/read"));
    }

    /** Privilege on a different application → empty (provider does not apply). */
    public void testNonMatchingApplicationReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor("other-app", "sml_read", Set.of("ai_index:visualization/read"), Map.of())
        );
        RoleDescriptor roleDescriptor = roleWithApplication("other-app", "sml_read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    /** A role with application "kibana-*" resolves to a residual privilege whose getApplication() is still "kibana-*". */
    public void testWildcardApplicationNameMatchesKibana() {
        RoleDescriptor roleDescriptor = roleWithApplication("kibana-*", "ai_index:dashboard/read", "space:default");
        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery(), is(notNullValue()));
    }

    /** A wildcard application (not Kibana) must not trigger the provider. */
    public void testNonMatchingWildcardApplicationReturnsEmpty() {
        RoleDescriptor roleDescriptor = roleWithApplication("shield*", "ai_index:dashboard/read", "space:default");
        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, is(empty()));
    }

    /** Wildcard and concrete actions in the same grant each become their own literal term. */
    public void testMixedWildcardAndConcreteActionsAllBecomeTerms() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "sml_mixed",
                Set.of("ai_index:dashboard/*", "ai_index:workflow/read"),
                Map.of()
            )
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_mixed", "space:marketing"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(termsOfClauseForSpace(clauses, "marketing"), containsInAnyOrder("ai_index:dashboard/*", "ai_index:workflow/read"));
    }

    /**
     * A bare {@code *} action matches every {@code ai_index:} action at authorization time, but it is
     * not an {@code ai_index:}-prefixed string, so it contributes nothing here: an all-actions Kibana
     * privilege does not implicitly unlock the Elastic AI Index.
     */
    public void testAllActionsWildcardDoesNotTriggerProvider() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "kibana_admin_like", Set.of("*"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("kibana_admin_like", "space:marketing"), storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    /** Privilege without login: still triggers the provider — holding an ai_index: action is what matters. */
    public void testNonLoginActionStillTriggersProvider() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_write", Set.of("ai_index:connector/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_write", "space:default"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(termsOfClauseForSpace(clauses, "default"), contains("ai_index:connector/read"));
    }

    /**
     * A Kibana grant that carries no {@code ai_index:} action does not unlock Elastic AI Index at all — not even
     * the public-document branch. Actions owned by other subsystems ({@code saved_object:}, {@code api:},
     * {@code login:}) must never grant Elastic AI Index visibility on their own.
     */
    public void testGrantWithoutAiIndexActionsReturnsEmpty() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_discover.read",
                Set.of("login:", "saved_object:dashboard/get", "api:discover_read"),
                Map.of()
            )
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("feature_discover.read", "space:marketing"), storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    /**
     * A grant mixing namespaces contributes only its {@code ai_index:} actions to the DLS query; actions
     * owned by other subsystems are dropped rather than inflating the {@code terms_set} clause.
     */
    public void testOnlyAiIndexActionsContributeTerms() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(
                KIBANA_APPLICATION,
                "feature_dashboards.read",
                Set.of("login:", "saved_object:dashboard/get", "api:dashboard_read", "ai_index:dashboard/read"),
                Map.of()
            )
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("feature_dashboards.read", "space:marketing"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(termsOfClauseForSpace(clauses, "marketing"), contains("ai_index:dashboard/read"));
    }

    /**
     * A wildcard action pattern is never expanded: {@code terms_set} matches exact keyword values, so
     * {@code ai_index:*} lands in the query verbatim and cannot match the concrete action names Kibana
     * writes on documents. A wildcard grant thus fails closed.
     */
    public void testWildcardActionPatternIsNotExpanded() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_all", Set.of("ai_index:*"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_all", "space:marketing"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(termsOfClauseForSpace(clauses, "marketing"), contains("ai_index:*"));
    }

    /** Resources without the "space:" prefix and not equal to "*" are ignored; if no valid resources remain → empty. */
    public void testResourcesWithoutSpacePrefixAreIgnored() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of("ai_index:visualization/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_read", "no-prefix-resource"), storedPrivileges)
        );
        assertThat(result, is(empty()));
    }

    /**
     * Role writes a raw action pattern directly (no stored descriptor). The raw-pattern
     * branch in privilege resolution should still trigger the provider and produce a DLS query.
     */
    public void testEmptyStoredPrivilegesWithRawActionStillWorks() {
        RoleDescriptor roleDescriptor = role("ai_index:dashboard/read", "space:default");

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(resolve(roleDescriptor, List.of()));
        assertThat(result, hasSize(1));
        assertThat(result.iterator().next().getQuery(), is(notNullValue()));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(termsOfClauseForSpace(clauses, "default"), contains("ai_index:dashboard/read"));
    }

    /** The terms_set terms are bare action strings — no space is baked into them. */
    public void testDlsQueryIncludesActionTerms() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_sml", Set.of("ai_index:workflow/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("feature_sml", "space:default"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        String query = result.iterator().next().getQuery().utf8ToString();
        assertThat(query, containsString("terms_set"));
        // No delimiter anywhere — space and action are separate fields now.
        assertThat(query, not(containsString("|")));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(termsOfClauseForSpace(clauses, "default"), contains("ai_index:workflow/read"));
    }

    /**
     * Two grants with different action sets on different spaces must produce one clause per space
     * carrying only that space's actions — not the (space x action) cross-product the flat
     * composite-token design emitted. The total term count is the sum of per-space action counts.
     */
    public void testNoCrossProductIsEmitted() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_dashboard", Set.of("ai_index:dashboard/read"), Map.of()),
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_workflow", Set.of("ai_index:workflow/read"), Map.of())
        );
        RoleDescriptor roleDescriptor = roleWithGrants(grant("sml_dashboard", "space:foo"), grant("sml_workflow", "space:bar"));

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );
        assertThat(result, hasSize(1));

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(clauses, hasSize(2));
        assertThat(termsOfClauseForSpace(clauses, "foo"), contains("ai_index:dashboard/read"));
        assertThat(termsOfClauseForSpace(clauses, "bar"), contains("ai_index:workflow/read"));

        // 1 action in foo + 1 action in bar = 2 terms total, not the 4 a cross-product would emit.
        int totalTerms = clauses.stream().mapToInt(clause -> termsOfClause(clause).size()).sum();
        assertThat(totalTerms, equalTo(2));

        assertThat(result.iterator().next().getQuery().utf8ToString(), not(containsString("|")));
    }

    /**
     * A space clause must also accept elements whose space is the all-spaces marker {@code "*"}.
     * <p>
     * The two meanings of {@code "*"} are easy to conflate. On the <em>grant</em> side it says the user
     * holds actions in every space, and produces the separate space-less clause. On the <em>document</em>
     * side it says the document lives in every space — which the Kibana SML indexer writes whenever an
     * entry is not space-scoped. A space-scoped user is in the space such a document lives in, so
     * matching only {@code term(space, "marketing")} would hide every all-spaces document from every
     * space-scoped user.
     */
    public void testSpaceClauseAlsoMatchesAllSpacesDocuments() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of("ai_index:dashboard/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_read", "space:marketing"), storedPrivileges)
        );

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(clauses, hasSize(1));
        assertThat(spacesOfClause(clauses.get(0)), containsInAnyOrder("marketing", ALL_SPACES));
    }

    /**
     * Every clause must admit elements that require no action at all ({@code count: 0}) — the shape the
     * Kibana indexer writes for a type that opts out of privilege gating. {@code terms_set} alone cannot express this.
     */
    public void testEveryClauseAdmitsElementsRequiringNoActions() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "global_read", Set.of("ai_index:dashboard/read"), Map.of()),
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "space_read", Set.of("ai_index:workflow/read"), Map.of())
        );
        RoleDescriptor roleDescriptor = roleWithGrants(grant("global_read", "*"), grant("space_read", "space:marketing"));

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(roleDescriptor, storedPrivileges)
        );

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(clauses, hasSize(2));
        for (Map<String, Object> clause : clauses) {
            assertThat("clause must carry the count:0 escape: " + clause, hasZeroCountEscape(clause), is(true));
        }
    }

    /**
     * The {@code count: 0} escape must not become a way around space scoping: a space clause still
     * filters on {@code .space}, so a zero-requirement element is public only within its own space.
     * (The space-less clause has no such filter by design — a wildcard grant means the user is in every
     * space, so a zero-requirement element anywhere is legitimately theirs to read.)
     */
    public void testZeroCountEscapeStaysInsideTheSpaceFilter() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "sml_read", Set.of("ai_index:dashboard/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = contributor.getImplicitIndicesPrivileges(
            resolve(role("sml_read", "space:marketing"), storedPrivileges)
        );

        List<Map<String, Object>> clauses = nestedSpaceClauses(parseQuery(result.iterator().next().getQuery()));
        assertThat(clauses, hasSize(1));
        assertThat(hasZeroCountEscape(clauses.get(0)), is(true));
        assertThat(spacesOfClause(clauses.get(0)), containsInAnyOrder("marketing", ALL_SPACES));
    }

    /** Exact serialisation of the query built directly from a resource-to-actions map. */
    public void testBuildDlsQueryFormat() {
        String query = ElasticAiIndexImplicitPrivilegesProvider.buildDlsQuery(Map.of("space:a", Set.of("ai_index:x/read")));
        assertThat(query, equalTo(EXPECTED_BUILD_DLS_QUERY));
    }

    /** No space-scoped and no global grant → no implicit privilege at all, rather than an unrestricted one. */
    public void testBuildDlsQueryReturnsNullWithoutAnyGrant() {
        assertThat(ElasticAiIndexImplicitPrivilegesProvider.buildDlsQuery(Map.of()), is(nullValue()));
        assertThat(ElasticAiIndexImplicitPrivilegesProvider.buildDlsQuery(Map.of("no-prefix", Set.of("ai_index:x/read"))), is(nullValue()));
    }

    // -------------------------------------------------------------------------------------
    // Expected serialisations. Pinned as exact strings deliberately: loose structural matchers
    // let regressions through in a security filter.
    // -------------------------------------------------------------------------------------

    private static final String EXPECTED_SINGLE_SPACE_QUERY = """
        {"bool":{"should":[\
        {"bool":{"must_not":[{"nested":{"query":{"match_all":{"boost":1.0}},\
        "path":"permissions.kibana.privileges","ignore_unmapped":false,"score_mode":"none","boost":1.0}}],"boost":1.0}},\
        {"nested":{"query":{"bool":{"should":[{"bool":{"filter":[\
        {"bool":{"should":[{"terms":{"permissions.kibana.privileges.space":["marketing"],"boost":1.0}},\
        {"term":{"permissions.kibana.privileges.space":{"value":"*"}}}],"minimum_should_match":"1","boost":1.0}},\
        {"bool":{"should":[{"term":{"permissions.kibana.privileges.count":{"value":0}}},\
        {"terms_set":{"permissions.kibana.privileges.name":{"terms":["ai_index:dashboard/read"],\
        "minimum_should_match_field":"permissions.kibana.privileges.count","boost":1.0}}}],\
        "minimum_should_match":"1","boost":1.0}}\
        ],"boost":1.0}}],"boost":1.0}},"path":"permissions.kibana.privileges","ignore_unmapped":false,\
        "score_mode":"none","boost":1.0}}],"boost":1.0}}""";

    private static final String EXPECTED_BUILD_DLS_QUERY = """
        {"bool":{"should":[\
        {"bool":{"must_not":[{"nested":{"query":{"match_all":{"boost":1.0}},\
        "path":"permissions.kibana.privileges","ignore_unmapped":false,"score_mode":"none","boost":1.0}}],"boost":1.0}},\
        {"nested":{"query":{"bool":{"should":[{"bool":{"filter":[\
        {"bool":{"should":[{"terms":{"permissions.kibana.privileges.space":["a"],"boost":1.0}},\
        {"term":{"permissions.kibana.privileges.space":{"value":"*"}}}],"minimum_should_match":"1","boost":1.0}},\
        {"bool":{"should":[{"term":{"permissions.kibana.privileges.count":{"value":0}}},\
        {"terms_set":{"permissions.kibana.privileges.name":{"terms":["ai_index:x/read"],\
        "minimum_should_match_field":"permissions.kibana.privileges.count","boost":1.0}}}],\
        "minimum_should_match":"1","boost":1.0}}\
        ],"boost":1.0}}],"boost":1.0}},"path":"permissions.kibana.privileges","ignore_unmapped":false,\
        "score_mode":"none","boost":1.0}}],"boost":1.0}}""";

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
        return roleWithGrants(
            RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application(application)
                .privileges(privilegeName)
                .resources(resources)
                .build()
        );
    }

    /** A single Kibana application grant, so a role can hold different privileges on different resources. */
    private static RoleDescriptor.ApplicationResourcePrivileges grant(String privilegeName, String... resources) {
        return RoleDescriptor.ApplicationResourcePrivileges.builder()
            .application(KIBANA_APPLICATION)
            .privileges(privilegeName)
            .resources(resources)
            .build();
    }

    private static RoleDescriptor roleWithGrants(RoleDescriptor.ApplicationResourcePrivileges... grants) {
        return new RoleDescriptor("test_role", null, null, grants, null, null, null, null);
    }

    private Map<String, Object> parseQuery(BytesReference queryBytes) {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, queryBytes.utf8ToString())) {
            return parser.map();
        } catch (Exception e) {
            throw new AssertionError("Failed to parse query JSON", e);
        }
    }

    /**
     * Extracts the space-clause list from the nested branch of the DLS query, so tests can assert on
     * clause structure without pinning the whole serialised string in every case.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> nestedSpaceClauses(Map<String, Object> queryMap) {
        Map<String, Object> topBool = (Map<String, Object>) queryMap.get("bool");
        List<Map<String, Object>> shoulds = (List<Map<String, Object>>) topBool.get("should");
        Map<String, Object> nestedBranch = shoulds.stream()
            .filter(s -> s.containsKey("nested"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no nested branch in " + queryMap));
        Map<String, Object> nested = (Map<String, Object>) nestedBranch.get("nested");
        Map<String, Object> innerBool = (Map<String, Object>) ((Map<String, Object>) nested.get("query")).get("bool");
        return (List<Map<String, Object>>) innerBool.get("should");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> filtersOfClause(Map<String, Object> clause) {
        return (List<Map<String, Object>>) ((Map<String, Object>) clause.get("bool")).get("filter");
    }

    /**
     * Every space value a clause matches on, or an empty list for the space-less (global-grant)
     * clause. A space clause matches its grouped spaces (a {@code terms} query) plus the all-spaces
     * marker {@code "*"} (a {@code term} query).
     */
    @SuppressWarnings("unchecked")
    private static List<String> spacesOfClause(Map<String, Object> clause) {
        // Both filters of a clause are bools (spaces, then required actions), so match on the field
        // rather than on position: the actions bool carries no .space and must yield nothing here.
        for (Map<String, Object> filter : filtersOfClause(clause)) {
            if (filter.containsKey("bool") == false) {
                continue;
            }
            List<Map<String, Object>> shoulds = (List<Map<String, Object>>) ((Map<String, Object>) filter.get("bool")).get("should");
            List<String> spaces = new ArrayList<>();
            for (Map<String, Object> should : shoulds) {
                if (should.containsKey("terms") && ((Map<String, Object>) should.get("terms")).containsKey(SPACE_FIELD)) {
                    spaces.addAll((List<String>) ((Map<String, Object>) should.get("terms")).get(SPACE_FIELD));
                } else if (should.containsKey("term") && ((Map<String, Object>) should.get("term")).containsKey(SPACE_FIELD)) {
                    Map<String, Object> field = (Map<String, Object>) ((Map<String, Object>) should.get("term")).get(SPACE_FIELD);
                    spaces.add((String) field.get("value"));
                }
            }
            if (spaces.isEmpty() == false) {
                return spaces;
            }
        }
        return List.of();
    }

    /** The action terms of a clause's terms_set, which sits inside the clause's required-actions bool. */
    @SuppressWarnings("unchecked")
    private static List<String> termsOfClause(Map<String, Object> clause) {
        for (Map<String, Object> should : requiredActionsShouldsOfClause(clause)) {
            if (should.containsKey("terms_set")) {
                Map<String, Object> field = (Map<String, Object>) ((Map<String, Object>) should.get("terms_set")).get(NAME_FIELD);
                return (List<String>) field.get("terms");
            }
        }
        throw new AssertionError("no terms_set in clause " + clause);
    }

    /**
     * The should-arms of a clause's required-actions bool: the count:0 escape and the terms_set. Found
     * by looking for the terms_set rather than by position, so a clause carrying a space filter and one
     * carrying none are handled the same way.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> requiredActionsShouldsOfClause(Map<String, Object> clause) {
        for (Map<String, Object> filter : filtersOfClause(clause)) {
            if (filter.containsKey("bool") == false) {
                continue;
            }
            List<Map<String, Object>> shoulds = (List<Map<String, Object>>) ((Map<String, Object>) filter.get("bool")).get("should");
            if (shoulds.stream().anyMatch(should -> should.containsKey("terms_set"))) {
                return shoulds;
            }
        }
        throw new AssertionError("no required-actions bool in clause " + clause);
    }

    /** True when a clause admits elements requiring no action at all, i.e. carries the count:0 escape. */
    @SuppressWarnings("unchecked")
    private static boolean hasZeroCountEscape(Map<String, Object> clause) {
        return requiredActionsShouldsOfClause(clause).stream().anyMatch(should -> {
            if (should.containsKey("term") == false) {
                return false;
            }
            Map<String, Object> term = (Map<String, Object>) should.get("term");
            if (term.containsKey(COUNT_FIELD) == false) {
                return false;
            }
            Object value = ((Map<String, Object>) term.get(COUNT_FIELD)).get("value");
            return value instanceof Number number && number.intValue() == 0;
        });
    }

    private static List<String> termsOfClauseForSpace(List<Map<String, Object>> clauses, String spaceId) {
        return clauses.stream()
            .filter(c -> spacesOfClause(c).contains(spaceId))
            .findFirst()
            .map(ElasticAiIndexImplicitPrivilegesProviderTests::termsOfClause)
            .orElseThrow(() -> new AssertionError("no clause for space [" + spaceId + "] in " + clauses));
    }

    private static List<String> termsOfSpacelessClause(List<Map<String, Object>> clauses) {
        return clauses.stream()
            .filter(c -> spacesOfClause(c).isEmpty())
            .findFirst()
            .map(ElasticAiIndexImplicitPrivilegesProviderTests::termsOfClause)
            .orElseThrow(() -> new AssertionError("no space-less clause in " + clauses));
    }
}
