/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsSetQueryBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.authz.privilege.ResolvedApplicationPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Implicitly grants read access to the Elastic AI Index ({@code ai-index-idx-sml-data}) for
 * users whose roles include a Kibana application privilege grant carrying at least one
 * {@code ai_index:} action.
 * <p>
 * {@code permissions.kibana.privileges} is a {@code nested} field holding one element per space the
 * document is visible in, each listing the {@code ai_index:} actions that space requires plus a
 * {@code count} of them; an element whose space is {@code "*"} means the document lives in every
 * space, and a document with no elements at all is public. This shape is currently owned by the Kibana
 * agent_builder_sml plugin's storage schema; the {@code ai-index-*} index template deliberately does
 * not declare it, so this Javadoc and {@code ElasticAiIndexImplicitPrivilegesIT} are the de-facto
 * contract.
 * <p>
 * The DLS query makes a document visible only when the user holds <em>all</em> the actions it
 * requires <em>within a single space</em>. See {@link #buildDlsQuery} for how the clauses are constructed.
 */
public class ElasticAiIndexImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String KIBANA_APPLICATION = "kibana-.kibana";
    // Index pattern mirrors the Kibana-side definition; keep in sync if it changes.
    static final String[] ELASTIC_AI_INDICES = { "ai-index-idx-sml-data" };
    static final String RESOURCE_PREFIX = "space:";
    // Action namespace owned by Elastic AI Index; mirrors the Kibana-side AiIndexActions definition, keep in sync if it changes.
    static final String AI_INDEX_ACTION_PREFIX = "ai_index:";
    static final String ALL_RESOURCES = "*";
    static final String INDEX_READ_PRIVILEGE = "read";
    static final String PRIVILEGES_PATH = "permissions.kibana.privileges";
    static final String NAME_FIELD = PRIVILEGES_PATH + ".name";
    static final String SPACE_FIELD = PRIVILEGES_PATH + ".space";
    static final String COUNT_FIELD = PRIVILEGES_PATH + ".count";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        Map<String, Set<String>> resourcesToActions = collectResourcesAndActions(applicationPrivileges);

        String dlsQuery = buildDlsQuery(resourcesToActions);
        if (dlsQuery == null) {
            return List.of();
        }

        return List.of(
            RoleDescriptor.IndicesPrivileges.builder().indices(ELASTIC_AI_INDICES).privileges(INDEX_READ_PRIVILEGE).query(dlsQuery).build()
        );
    }

    /**
     * Collects the union of resources mapped to their {@code ai_index:} action strings from every
     * resolved application-privilege grant that targets the Kibana application.
     *
     * The action strings (from {@link ApplicationPrivilege#getPatterns()}) are filtered down to the
     * {@code ai_index:} namespace before they populate the {@code terms_set} DLS clause.
     * A grant that contributes no {@code ai_index:} action is skipped entirely, so it cannot open up Elastic AI Index on its own.
     *
     * Returns a map from each resource string (e.g. {@code "space:marketing"} or {@code "*"}) to
     * the set of action strings held under that resource. Resources across multiple grants for the
     * same resource are unioned.
     */
    private static Map<String, Set<String>> collectResourcesAndActions(Collection<ResolvedApplicationPrivilege> applicationPrivileges) {
        Map<String, Set<String>> resourcesToActions = new HashMap<>();
        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            if (applicationMatchesKibana(privilege.getApplication())) {
                // Using getPatterns() rather than predicate().test(...) to allow for the open nature of `ai_index:` action namespace
                // (there could be more types created for `ai_index:<type>/read` in the future), whereas predicate is good for a known
                // fixed list of actions.
                Set<String> aiIndexActions = Arrays.stream(privilege.getPatterns())
                    .filter(pattern -> pattern.startsWith(AI_INDEX_ACTION_PREFIX))
                    .collect(Collectors.toSet());
                if (aiIndexActions.isEmpty()) {
                    continue;
                }
                for (String resource : resolved.resources()) {
                    resourcesToActions.computeIfAbsent(resource, k -> new HashSet<>()).addAll(aiIndexActions);
                }
            }
        }
        return resourcesToActions;
    }

    private static boolean applicationMatchesKibana(String application) {
        return application.contains("*")
            ? Automatons.predicate(application).test(KIBANA_APPLICATION)
            : KIBANA_APPLICATION.equals(application);
    }

    /**
     * Builds the DLS query making a document visible only when the user holds all the actions it
     * requires within a single space. Inside a single {@code nested} query it emits one clause per
     * distinct effective action set (a space's own actions unioned with any {@code *} grant's),
     * matching any of that set's spaces via {@link #spaceMatches} and gating the actions with
     * {@code terms_set} on the per-element {@code count}; a {@code *} grant adds one further
     * space-less clause.
     * <p>
     * Documents with no permission elements are public. This must be expressed as
     * {@code must_not nested(match_all)}, never {@code must_not exists} — a root-level {@code exists}
     * on a nested subfield matches every document, which would void the whole query. Likewise,
     * {@code ignore_unmapped} stays {@code false} so a missing nested mapping fails loudly instead of
     * silently failing open through the public-document branch.
     *
     * @return the serialised query, or {@code null} if the user holds no space-scoped or global grant,
     *         in which case no implicit privilege is granted at all.
     */
    static String buildDlsQuery(Map<String, Set<String>> resourcesToActions) {
        Set<String> globalActions = resourcesToActions.getOrDefault(ALL_RESOURCES, Set.of());

        // Group spaces by their effective action set so spaces sharing one share a clause. Spaces are
        // iterated in sorted order (the source map is a HashMap) so the grouping, the clause order and
        // the space lists are all deterministic.
        Map<Set<String>, List<String>> spacesByActions = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : new TreeMap<>(resourcesToActions).entrySet()) {
            String resource = entry.getKey();
            if (resource.startsWith(RESOURCE_PREFIX) == false) {
                // "*" is handled below; anything else is not a space resource and is ignored.
                continue;
            }
            Set<String> actions = new HashSet<>(entry.getValue());
            actions.addAll(globalActions);
            if (actions.equals(globalActions)) {
                // Subsumed by the space-less global clause below: same terms_set, one restriction fewer.
                continue;
            }
            spacesByActions.computeIfAbsent(actions, k -> new ArrayList<>()).add(resource.substring(RESOURCE_PREFIX.length()));
        }

        BoolQueryBuilder spaceClauses = QueryBuilders.boolQuery();
        spacesByActions.forEach(
            (actions, spaces) -> spaceClauses.should(QueryBuilders.boolQuery().filter(spaceMatches(spaces)).filter(termsSetOn(actions)))
        );
        if (globalActions.isEmpty() == false) {
            spaceClauses.should(QueryBuilders.boolQuery().filter(termsSetOn(globalActions)));
        }

        if (spaceClauses.should().isEmpty()) {
            return null;
        }

        return Strings.toString(
            QueryBuilders.boolQuery()
                .should(
                    QueryBuilders.boolQuery()
                        .mustNot(QueryBuilders.nestedQuery(PRIVILEGES_PATH, QueryBuilders.matchAllQuery(), ScoreMode.None))
                )
                .should(QueryBuilders.nestedQuery(PRIVILEGES_PATH, spaceClauses, ScoreMode.None))
        );
    }

    /**
     * Matches elements that apply in any of {@code spaceIds}: those spaces' own elements, plus any the
     * producer scoped to every space by writing {@code "*"} as the element's space.
     * <p>
     * The {@code "*"} arm is not symmetric with the wildcard <em>grant</em> handled in
     * {@link #buildDlsQuery}. There, {@code "*"} is a property of the user's role — "this user holds
     * these actions in every space". Here it is a property of the document — "this document lives in
     * every space". A user scoped to a single space is still in the space such a document lives in, so
     * omitting this arm would hide every all-spaces document from every space-scoped user.
     */
    private static BoolQueryBuilder spaceMatches(List<String> spaceIds) {
        return QueryBuilders.boolQuery()
            .should(QueryBuilders.termsQuery(SPACE_FIELD, spaceIds))
            .should(QueryBuilders.termQuery(SPACE_FIELD, ALL_RESOURCES))
            .minimumShouldMatch(1);
    }

    private static TermsSetQueryBuilder termsSetOn(Set<String> actions) {
        return new TermsSetQueryBuilder(NAME_FIELD, actions.stream().sorted().toList()).setMinimumShouldMatchField(COUNT_FIELD);
    }

}
