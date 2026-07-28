/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.authz.privilege.ResolvedApplicationPrivilege;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implicitly grants read access to AI Index ({@code ai-index-*}) for users whose roles include a
 * Kibana application privilege granting {@code login:}.
 * <p>
 * AI Index documents carry composite scoped-privileges in {@code permissions.kibana.privileges.name}
 * that bind a space and a privilege action together (e.g. {@code "marketing|saved_object:dashboard/get"}).
 * The wildcard resource ({@code *}) is treated as a literal space component, producing tokens like
 * {@code "*|saved_object:dashboard/get"} for global documents. The number of scoped privileges a
 * document requires is pre-computed and stored in {@code permissions.kibana.privileges.count}. A
 * document with no {@code permissions.kibana.privileges.name} field is a public document visible to
 * all users who pass the login check.
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
    static final String LOGIN_ACTION = "login:";
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
     * application-privilege grant that targets the Kibana application <i>and</i> authorizes
     * {@link #LOGIN_ACTION}.
     * <p>
     * Each {@link ResolvedApplicationPrivilege} carries a resolved {@link ApplicationPrivilege}
     * whose {@link ApplicationPrivilege#predicate() predicate} already matches every action the
     * grant authorizes &mdash; both the actions of any stored privilege the role referenced by
     * name <em>and</em> any raw action patterns written directly under {@code privileges[]} (e.g.
     * {@code "login:"} or {@code "*"}) &mdash; so a single {@code predicate().test(...)} settles
     * whether the grant authorizes {@link #LOGIN_ACTION}. The action strings (from
     * {@link ApplicationPrivilege#getPatterns()}) are collected to populate the {@code terms_set}
     * DLS clause; including all patterns from the grant is safe because extra terms that no
     * document references are harmless.
     * <p>
     * Returns a map from each resource string (e.g. {@code "space:marketing"} or {@code "*"}) to
     * the set of action strings held under that resource. Resources across multiple grants for the
     * same resource are unioned.
     */
    private static Map<String, Set<String>> collectResourcesAndActions(Collection<ResolvedApplicationPrivilege> applicationPrivileges) {
        Map<String, Set<String>> resourcesToActions = new HashMap<>();
        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            if (applicationMatchesKibana(privilege.getApplication()) && privilege.predicate().test(LOGIN_ACTION)) {
                Set<String> patterns = new HashSet<>(Arrays.asList(privilege.getPatterns()));
                for (String resource : resolved.resources()) {
                    resourcesToActions.computeIfAbsent(resource, k -> new HashSet<>()).addAll(patterns);
                }
            }
        }
        return resourcesToActions;
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
     * Whether a resolved privilege's application targets the Kibana application.
     */
    private static boolean applicationMatchesKibana(String application) {
        return KIBANA_APPLICATION.equals(application);
    }

    /**
     * Builds the DLS query that gates AI Index document visibility by composite scoped privileges
     * stored in {@code permissions.kibana.privileges.name}.
     * <p>
     * The query structure is a top-level {@code bool/should} with two branches:
     * <ol>
     *   <li>Public-document branch: {@code bool/must_not exists permissions.kibana.privileges.name}
     *       — matches documents that carry no scoped-privilege requirements (publicly visible to
     *       any login: user).</li>
     *   <li>Scoped-privilege-match branch: {@code terms_set} on
     *       {@code permissions.kibana.privileges.name} requiring the document's full scoped-privilege
     *       set to be a subset of the user's held scoped privileges, enforced via
     *       {@code minimum_should_match_field: permissions.kibana.privileges.count}.</li>
     * </ol>
     * <p>
     * Hand-rolled via {@link XContentBuilder} rather than {@code QueryBuilders} because
     * {@code QueryBuilders} does not expose a {@code termsSetQuery} factory and would also emit
     * an unwanted {@code "boost":1.0} field that complicates DLS query caching.
     */
    static String buildDlsQuery(Set<String> scopedPrivileges) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("should");

            // Branch A: document has no permissions.kibana.privileges.name (public document)
            builder.startObject();
            builder.startObject("bool");
            builder.startObject("must_not");
            builder.startObject("exists");
            builder.field("field", PERMISSIONS_FIELD);
            builder.endObject(); // exists
            builder.endObject(); // must_not
            builder.endObject(); // bool
            builder.endObject(); // outer object

            // Branch B: user holds ALL of the document's required scoped privileges
            builder.startObject();
            builder.startObject("terms_set");
            builder.startObject(PERMISSIONS_FIELD);
            builder.array("terms", scopedPrivileges.toArray(new String[0]));
            builder.field("minimum_should_match_field", PERMISSIONS_COUNT_FIELD);
            builder.endObject(); // PERMISSIONS_FIELD
            builder.endObject(); // terms_set
            builder.endObject(); // outer object

            builder.endArray(); // should
            builder.endObject(); // bool
            builder.endObject(); // top-level

            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
