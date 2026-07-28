/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.common.Strings;
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

/**
 * Implicitly grants read access to AI Index ({@code ai-index-*}) for users whose roles include any
 * Kibana application privilege grant.
 * <p>
 * AI Index documents carry composite scoped-privileges in {@code permissions.kibana.privileges.name}
 * that bind a space and a privilege action together (e.g. {@code "marketing|saved_object:dashboard/get"}).
 * The wildcard resource ({@code *}) is treated as a literal space component, producing tokens like
 * {@code "*|saved_object:dashboard/get"} for global documents. The number of scoped privileges a
 * document requires is pre-computed and stored in {@code permissions.kibana.privileges.count}. A
 * document with no {@code permissions.kibana.privileges.name} field is a public document visible to
 * all authenticated users.
 * <p>
 * The provider builds the user's scoped-privilege set from the cross-product of their space IDs and
 * action strings across all matching grants. For each resource the user belongs to and each action
 * they hold in that resource, one composite scoped privilege {@code "<spaceId>|<action>"} is emitted.
 * The DLS query then uses a {@code terms_set} query requiring that every scoped privilege listed on
 * the document is present in the user's held set
 * ({@code minimum_should_match_field: permissions.kibana.privileges.count}).
 * <p>
 * The provider always builds a DLS query — the wildcard resource ({@code *}) flows through
 * {@link #buildScopedPrivileges} as a literal space component rather than short-circuiting to
 * unrestricted access. The DLS query:
 * <ul>
 *   <li>Allows documents that have no {@code permissions.kibana.privileges.name} field (public
 *       documents).</li>
 *   <li>Allows documents whose entire {@code permissions.kibana.privileges.name} set is a subset of
 *       the user's held composite scoped privileges (enforced via {@code terms_set} with
 *       {@code minimum_should_match_field: permissions.kibana.privileges.count}).</li>
 * </ul>
 */
public class AiIndexImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String KIBANA_APPLICATION = "kibana-.kibana";
    // Index pattern mirrors the Kibana-side definition; keep in sync if it changes.
    static final String[] AI_INDEX_INDICES = { "ai-index-*" };
    static final String RESOURCE_PREFIX = "space:";
    static final String ALL_RESOURCES = "*";
    static final String INDEX_READ_PRIVILEGE = "read";
    static final String PERMISSIONS_FIELD = "permissions.kibana.privileges.name";
    static final String PERMISSIONS_COUNT_FIELD = "permissions.kibana.privileges.count";
    static final String SCOPE_SEPARATOR = "|";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        Map<String, Set<String>> resourcesToActions = collectResourcesAndActions(applicationPrivileges);

        if (resourcesToActions.isEmpty()) {
            return List.of();
        }

        Set<String> scopedPrivileges = buildScopedPrivileges(resourcesToActions);

        if (scopedPrivileges.isEmpty()) {
            return List.of();
        }

        return List.of(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(AI_INDEX_INDICES)
                .privileges(INDEX_READ_PRIVILEGE)
                .query(buildDlsQuery(scopedPrivileges))
                .build()
        );
    }

    /**
     * Collects the union of resources mapped to their action strings from every resolved
     * application-privilege grant that targets the Kibana application.
     * <p>
     * The action strings (from {@link ApplicationPrivilege#getPatterns()}) are collected to
     * populate the {@code terms_set} DLS clause; including all patterns from the grant is safe
     * because extra terms that no document references are harmless.
     * <p>
     * Returns a map from each resource string (e.g. {@code "space:marketing"} or {@code "*"}) to
     * the set of action strings held under that resource. Resources across multiple grants for the
     * same resource are unioned.
     */
    private static Map<String, Set<String>> collectResourcesAndActions(Collection<ResolvedApplicationPrivilege> applicationPrivileges) {
        Map<String, Set<String>> resourcesToActions = new HashMap<>();
        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            if (applicationMatchesKibana(privilege.getApplication())) {
                Set<String> patterns = new HashSet<>(Arrays.asList(privilege.getPatterns()));
                for (String resource : resolved.resources()) {
                    resourcesToActions.computeIfAbsent(resource, k -> new HashSet<>()).addAll(patterns);
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
     * Builds the cross-product scoped-privilege set from the resource-to-actions map.
     * <p>
     * For each resource with a {@code "space:"} prefix, the space ID is extracted and combined
     * with each action string to produce a composite scoped privilege of the form
     * {@code "<spaceId>|<action>"}. The wildcard resource ({@code "*"}) is treated as a literal
     * space component, producing {@code "*|<action>"} tokens for global documents. Resources that
     * are neither {@code "*"} nor prefixed with {@code "space:"} are ignored.
     */
    static Set<String> buildScopedPrivileges(Map<String, Set<String>> resourcesAndActions) {
        Set<String> scopedPrivileges = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : resourcesAndActions.entrySet()) {
            String resource = entry.getKey();
            String spaceId;
            if (ALL_RESOURCES.equals(resource)) {
                spaceId = ALL_RESOURCES;
            } else if (resource.startsWith(RESOURCE_PREFIX)) {
                spaceId = resource.substring(RESOURCE_PREFIX.length());
            } else {
                continue;
            }
            for (String action : entry.getValue()) {
                scopedPrivileges.add(spaceId + SCOPE_SEPARATOR + action);
            }
        }
        return scopedPrivileges;
    }

    /**
     * Builds the DLS query that gates AI Index document visibility by composite scoped privileges
     * stored in {@code permissions.kibana.privileges.name}.
     * <p>
     * The query structure is a top-level {@code bool/should} with two branches:
     * <ol>
     *   <li>Public-document branch: {@code bool/must_not exists permissions.kibana.privileges.name}
     *       — matches documents that carry no scoped-privilege requirements (publicly visible to
     *       all authenticated users).</li>
     *   <li>Scoped-privilege-match branch: {@code terms_set} on
     *       {@code permissions.kibana.privileges.name} requiring the document's full scoped-privilege
     *       set to be a subset of the user's held scoped privileges, enforced via
     *       {@code minimum_should_match_field: permissions.kibana.privileges.count}.</li>
     * </ol>
     */
    static String buildDlsQuery(Set<String> scopedPrivileges) {
        return Strings.toString(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(PERMISSIONS_FIELD)))
                .should(
                    new TermsSetQueryBuilder(PERMISSIONS_FIELD, scopedPrivileges.stream().sorted().toList()).setMinimumShouldMatchField(
                        PERMISSIONS_COUNT_FIELD
                    )
                )
        );
    }

}
