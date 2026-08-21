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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
 * <b>Document contract.</b> {@code permissions.kibana.privileges} is a {@code nested} field holding one
 * element per space the document is visible in. Each element lists the {@code ai_index:} actions that
 * space requires, plus a {@code count} of them. An element whose space is {@code "*"} means the
 * document lives in every space:
 * <pre>{@code
 * "permissions": { "kibana": { "privileges": [
 *   { "space": "marketing", "name": ["ai_index:dashboard/read", "ai_index:lens/read"], "count": 2 },
 *   { "space": "finance",   "name": ["ai_index:dashboard/read"],                       "count": 1 }
 * ]}}
 * }</pre>
 * A document with no elements at all is a public document, visible to every user this provider grants
 * an implicit privilege to. This shape is owned by the Kibana agent_builder_sml plugin's storage schema; the
 * {@code ai-index-*} index template deliberately does not declare it, so this Javadoc and
 * {@code ElasticAiIndexImplicitPrivilegesIT} are the de-facto contract.
 * <p>
 * <b>Why nested.</b> The required semantics are <em>OR across spaces, AND across actions within a
 * space</em>. A flat counted keyword field cannot express that: {@code terms_set} counts matching terms
 * with no awareness of which space each came from, so a user holding one required action in each of two
 * spaces would clear a threshold of two and see a document they are authorised for in neither. Nested
 * matching is existential — a root document matches when at least one <em>child</em> satisfies a clause,
 * and each child is evaluated alone — so matches structurally cannot accumulate across spaces.
 * <p>
 * <b>Clause construction.</b> {@link #buildDlsQuery} emits, inside a single {@code nested} query:
 * <ul>
 *   <li>one clause per space the user holds {@code ai_index:} actions in, pairing a match on
 *       {@code .space} with a {@code terms_set} on {@code .name} gated by
 *       {@code minimum_should_match_field: .count}. The terms are that space's actions <em>unioned
 *       with</em> the actions from any {@code *} grant — a user holding {@code A} globally and {@code B}
 *       in marketing genuinely holds both in marketing, and without the union neither clause alone
 *       would satisfy a document requiring both. The space match accepts the space id <em>or</em>
 *       {@code "*"}, so documents a producer scoped to every space stay visible to space-scoped
 *       users;</li>
 *   <li>if the user holds a {@code *} grant, one further clause with no {@code .space} filter, so
 *       documents in spaces the user has no explicit grant in are still reachable.</li>
 * </ul>
 * The wildcard resource is therefore never a bypass: it widens which elements are eligible, but the
 * action check still applies.
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
     * Builds the DLS query gating Elastic AI Index document visibility.
     * <p>
     * {@code permissions.kibana.privileges} is a {@code nested} field carrying one element per space,
     * each listing the actions that space requires plus a {@code count} of them. A {@code nested} query
     * matches a root document when <em>at least one</em> child matches, which gives OR-across-spaces for
     * free. Within a child, {@code terms_set} with {@code minimum_should_match_field: count} gives
     * AND-across-actions. Because each child is evaluated independently against a single clause, matches
     * can never accumulate across spaces — the cross-space leak a flat counted keyword field allows.
     * <p>
     * Clause construction:
     * <ul>
     *   <li>One clause per space the user holds actions in, carrying that space's actions
     *       <em>unioned with</em> the actions from any {@code *} grant. The union matters: a user with
     *       {@code A} globally and {@code B} in marketing holds both in marketing, and without the union
     *       neither clause alone would satisfy a document requiring both. The space match is
     *       {@link #spaceMatches}, which also accepts all-spaces ({@code "*"}) elements.</li>
     *   <li>If the user holds a {@code *} grant, one further clause with no space filter, so documents
     *       in spaces the user has no explicit grant in are still reachable.</li>
     * </ul>
     * A document with no {@code permissions.kibana.privileges} elements at all is public. Note this must
     * be expressed as {@code must_not nested(match_all)} — a root-level {@code must_not exists} on a
     * nested subfield matches <em>every</em> document (the values live on child docs), which would turn
     * the whole DLS query into a no-op.
     * <p>
     * {@code ignore_unmapped} is deliberately left at its default {@code false}. If the granted index
     * does not declare the nested mapping the search then fails loudly, rather than matching
     * the public-document branch for every document, which would be a silent fail-open.
     *
     * @return the serialised query, or {@code null} if the user holds no space-scoped or global grant,
     *         in which case no implicit privilege is granted at all.
     */
    static String buildDlsQuery(Map<String, Set<String>> resourcesToActions) {
        Set<String> globalActions = resourcesToActions.getOrDefault(ALL_RESOURCES, Set.of());

        BoolQueryBuilder spaceClauses = QueryBuilders.boolQuery();
        boolean hasClause = false;

        // Iterated in sorted order so the emitted clause order is deterministic: the source map is a
        // HashMap, and this query is asserted on as an exact string in tests and surfaced via the
        // get-role API.
        for (Map.Entry<String, Set<String>> entry : new TreeMap<>(resourcesToActions).entrySet()) {
            String resource = entry.getKey();
            if (resource.startsWith(RESOURCE_PREFIX) == false) {
                // "*" is handled below; anything else is not a space resource and is ignored.
                continue;
            }
            String spaceId = resource.substring(RESOURCE_PREFIX.length());
            Set<String> actions = new HashSet<>(entry.getValue());
            actions.addAll(globalActions);
            spaceClauses.should(QueryBuilders.boolQuery().filter(spaceMatches(spaceId)).filter(termsSetOn(actions)));
            hasClause = true;
        }

        if (globalActions.isEmpty() == false) {
            spaceClauses.should(QueryBuilders.boolQuery().filter(termsSetOn(globalActions)));
            hasClause = true;
        }

        if (hasClause == false) {
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
     * Matches elements that apply in {@code spaceId}: the space's own elements, plus any the producer
     * scoped to every space by writing {@code "*"} as the element's space.
     * <p>
     * The {@code "*"} arm is not symmetric with the wildcard <em>grant</em> handled in
     * {@link #buildDlsQuery}. There, {@code "*"} is a property of the user's role — "this user holds
     * these actions in every space". Here it is a property of the document — "this document lives in
     * every space". A user scoped to a single space is still in the space such a document lives in, so
     * omitting this arm would hide every all-spaces document from every space-scoped user.
     */
    private static BoolQueryBuilder spaceMatches(String spaceId) {
        return QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery(SPACE_FIELD, spaceId))
            .should(QueryBuilders.termQuery(SPACE_FIELD, ALL_RESOURCES))
            .minimumShouldMatch(1);
    }

    private static TermsSetQueryBuilder termsSetOn(Set<String> actions) {
        return new TermsSetQueryBuilder(NAME_FIELD, actions.stream().sorted().toList()).setMinimumShouldMatchField(COUNT_FIELD);
    }

}
