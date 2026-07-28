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
import org.elasticsearch.xpack.core.security.support.Automatons;

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
 * Implicitly grants read access to the Kibana SML (Semantic Machine Learning) index
 * ({@code ai-index-idx-sml-data}) for users whose roles include a Kibana application privilege
 * granting {@code login:}.
 * <p>
 * SML documents carry composite {@code dls_tokens} that bind a space and a privilege action
 * together (e.g. {@code "marketing|saved_object:dashboard/get"}). The number of tokens a document
 * requires is pre-computed and stored in {@code dls_tokens_count}. A document with no
 * {@code dls_tokens} field is a public document visible to all users who pass the login check.
 * <p>
 * The provider builds the user's token set from the cross-product of their space IDs and action
 * strings across all matching grants. For each space the user belongs to and each action they hold
 * in that space, one composite token {@code "<spaceId>|<action>"} is emitted. The DLS query then
 * uses a {@code terms_set} query requiring that every token listed on the document is present in
 * the user's held set ({@code minimum_should_match_field: dls_tokens_count}).
 * <p>
 * When the user holds the wildcard resource ({@code *}), full read access is granted with no DLS
 * restriction. Otherwise, the provider builds a DLS query that:
 * <ul>
 *   <li>Allows documents that have no {@code dls_tokens} field (public documents).</li>
 *   <li>Allows documents whose entire {@code dls_tokens} set is a subset of the user's held
 *       composite tokens (enforced via {@code terms_set} with
 *       {@code minimum_should_match_field: dls_tokens_count}).</li>
 * </ul>
 */
public class KibanaSmlImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String KIBANA_APPLICATION = "kibana-.kibana";
    static final String LOGIN_ACTION = "login:";
    // Index name mirrors the Kibana-side definition; keep in sync if it changes.
    static final String[] SML_INDICES = { "ai-index-idx-sml-data" };
    static final String RESOURCE_PREFIX = "space:";
    static final String ALL_RESOURCES = "*";
    static final String INDEX_READ_PRIVILEGE = "read";
    static final String DLS_TOKENS_FIELD = "dls_tokens";
    static final String DLS_TOKENS_COUNT_FIELD = "dls_tokens_count";
    static final String TOKEN_SEPARATOR = "|";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        Map<String, Set<String>> resourcesToActions = collectResourcesAndActions(applicationPrivileges);

        if (resourcesToActions.isEmpty()) {
            return List.of();
        }

        // If any resource is the wildcard, grant full access with no DLS.
        if (resourcesToActions.containsKey(ALL_RESOURCES)) {
            return List.of(RoleDescriptor.IndicesPrivileges.builder().indices(SML_INDICES).privileges(INDEX_READ_PRIVILEGE).build());
        }

        Set<String> tokens = buildCompositeTokens(resourcesToActions);

        if (tokens.isEmpty()) {
            return List.of();
        }

        return List.of(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(SML_INDICES)
                .privileges(INDEX_READ_PRIVILEGE)
                .query(buildDlsQuery(tokens))
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
     * Builds the cross-product composite token set from the resource-to-actions map.
     * <p>
     * For each resource with a {@code "space:"} prefix, the space ID is extracted and combined
     * with each action string to produce a composite token of the form
     * {@code "<spaceId>|<action>"}. Resources without the {@code "space:"} prefix are ignored.
     */
    static Set<String> buildCompositeTokens(Map<String, Set<String>> resourcesAndActions) {
        Set<String> tokens = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : resourcesAndActions.entrySet()) {
            String resource = entry.getKey();
            if (resource.startsWith(RESOURCE_PREFIX) == false) {
                continue;
            }
            String spaceId = resource.substring(RESOURCE_PREFIX.length());
            for (String action : entry.getValue()) {
                tokens.add(spaceId + TOKEN_SEPARATOR + action);
            }
        }
        return tokens;
    }

    /**
     * Whether a resolved privilege's application targets the Kibana application. Resolution
     * expands wildcard application names against the stored privileges, so the value is normally
     * concrete and settled by equality; a residual wildcard (e.g. {@code "kibana-*"} or
     * {@code "*"} with no matching stored descriptor) is matched with an automaton.
     */
    private static boolean applicationMatchesKibana(String application) {
        return application.contains("*")
            ? Automatons.predicate(application).test(KIBANA_APPLICATION)
            : KIBANA_APPLICATION.equals(application);
    }

    /**
     * Builds the DLS query that gates SML document visibility by composite {@code dls_tokens}.
     * <p>
     * The query structure is a top-level {@code bool/should} with two branches:
     * <ol>
     *   <li>Public-document branch: {@code bool/must_not exists dls_tokens} — matches documents
     *       that carry no token requirements (publicly visible to any login: user).</li>
     *   <li>Token-match branch: {@code terms_set} on {@code dls_tokens} requiring the document's
     *       full token set to be a subset of the user's held tokens, enforced via
     *       {@code minimum_should_match_field: dls_tokens_count}.</li>
     * </ol>
     * <p>
     * Hand-rolled via {@link XContentBuilder} rather than {@code QueryBuilders} to keep the
     * serialized DLS query free of the default {@code "boost":1.0} field that query builder
     * classes always emit.
     */
    static String buildDlsQuery(Set<String> tokens) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("should");

            // Branch A: document has no dls_tokens (public document)
            builder.startObject();
            builder.startObject("bool");
            builder.startObject("must_not");
            builder.startObject("exists");
            builder.field("field", DLS_TOKENS_FIELD);
            builder.endObject(); // exists
            builder.endObject(); // must_not
            builder.endObject(); // bool
            builder.endObject(); // outer object

            // Branch B: user holds ALL of the document's required tokens
            builder.startObject();
            builder.startObject("terms_set");
            builder.startObject(DLS_TOKENS_FIELD);
            builder.array("terms", tokens.toArray(new String[0]));
            builder.field("minimum_should_match_field", DLS_TOKENS_COUNT_FIELD);
            builder.endObject(); // DLS_TOKENS_FIELD
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
