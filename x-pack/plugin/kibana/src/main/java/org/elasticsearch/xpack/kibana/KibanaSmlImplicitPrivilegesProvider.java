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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implicitly grants read access to the Kibana SML (Semantic Machine Learning) index
 * ({@code ai-index-idx-sml-data}) for users whose roles include a Kibana application privilege
 * granting {@code login:}.
 * <p>
 * SML documents carry two independent access-control dimensions:
 * <ol>
 *   <li><b>Space</b> — the Kibana space(s) the document belongs to, stored in the {@code spaces}
 *       field. A document with {@code spaces: ["*"]} is visible in all spaces.</li>
 *   <li><b>Required privileges</b> — the set of Kibana privilege action strings a user must hold
 *       in order to see the document, stored as an array in
 *       {@code permissions.kibana.privileges.name}. The number of required privileges is
 *       pre-computed and stored in {@code permissions_count}. A document with no
 *       {@code permissions.kibana.privileges.name} field (or an empty array) is visible to anyone
 *       who passes the space check.</li>
 * </ol>
 * <p>
 * When the user holds the wildcard resource ({@code *}), full read access is granted with no DLS
 * restriction. Otherwise, the provider builds a DLS query that:
 * <ul>
 *   <li>Restricts to documents whose {@code spaces} field contains one of the user's space IDs
 *       <em>or</em> the global {@code "*"} sentinel.</li>
 *   <li>Restricts to documents that either have no required privileges, or whose entire required
 *       privilege set is a subset of the privileges the user holds (enforced via a
 *       {@code terms_set} query with {@code minimum_should_match_field: permissions_count}).</li>
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
    static final String SPACES_FIELD = "spaces";
    static final String PERMISSIONS_FIELD = "permissions.kibana.privileges.name";
    static final String PERMISSIONS_COUNT_FIELD = "permissions_count";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        ResourcesAndActions collected = collectResourcesAndActions(applicationPrivileges);
        Set<String> resources = collected.resources();
        Set<String> actions = collected.actions();

        if (resources.isEmpty()) {
            return List.of();
        }

        if (resources.contains(ALL_RESOURCES)) {
            return List.of(RoleDescriptor.IndicesPrivileges.builder().indices(SML_INDICES).privileges(INDEX_READ_PRIVILEGE).build());
        }

        Set<String> spaceIds = resources.stream()
            .filter(r -> r.startsWith(RESOURCE_PREFIX))
            .map(r -> r.substring(RESOURCE_PREFIX.length()))
            .collect(Collectors.toSet());

        if (spaceIds.isEmpty()) {
            return List.of();
        }

        return List.of(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(SML_INDICES)
                .privileges(INDEX_READ_PRIVILEGE)
                .query(buildDlsQuery(spaceIds, actions))
                .build()
        );
    }

    /**
     * Collects the union of resources and action strings from every resolved application-privilege
     * grant that targets the Kibana application <i>and</i> authorizes {@link #LOGIN_ACTION}.
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
     */
    private static ResourcesAndActions collectResourcesAndActions(Collection<ResolvedApplicationPrivilege> applicationPrivileges) {
        Set<String> resources = new HashSet<>();
        Set<String> actions = new HashSet<>();
        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            if (applicationMatchesKibana(privilege.getApplication()) && privilege.predicate().test(LOGIN_ACTION)) {
                resources.addAll(resolved.resources());
                actions.addAll(Arrays.asList(privilege.getPatterns()));
            }
        }
        return new ResourcesAndActions(resources, actions);
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
     * Builds the DLS query that gates SML document visibility by both space membership and
     * held privilege actions.
     * <p>
     * The query structure is a top-level {@code bool/must} with two clauses:
     * <ol>
     *   <li>Space filter: {@code bool/should} over {@code terms} for the user's space IDs and a
     *       {@code term} for the global sentinel {@code "*"}.</li>
     *   <li>Privilege filter: {@code bool/should} where one branch allows documents with no
     *       required privileges ({@code must_not exists}), and the other uses {@code terms_set}
     *       to require the document's full privilege set to be a subset of the user's held
     *       actions (using {@code minimum_should_match_field: permissions_count}).</li>
     * </ol>
     * <p>
     * Hand-rolled via {@link XContentBuilder} rather than {@code QueryBuilders} to keep the
     * serialized DLS query free of the default {@code "boost":1.0} field that query builder
     * classes always emit.
     */
    static String buildDlsQuery(Set<String> spaceIds, Set<String> actions) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("must");

            // Clause 1: space filter — spaces contains one of the user's space IDs, OR "*"
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("should");

            builder.startObject();
            builder.startObject("terms");
            builder.array(SPACES_FIELD, spaceIds.toArray(new String[0]));
            builder.endObject();
            builder.endObject();

            builder.startObject();
            builder.startObject("term");
            builder.field(SPACES_FIELD, ALL_RESOURCES);
            builder.endObject();
            builder.endObject();

            builder.endArray(); // should
            builder.endObject(); // bool
            builder.endObject(); // outer object

            // Clause 2: privilege filter — no permissions required, OR user holds all required ones
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("should");

            // Branch A: document has no required privileges (field absent or empty)
            builder.startObject();
            builder.startObject("bool");
            builder.startObject("must_not");
            builder.startObject("exists");
            builder.field("field", PERMISSIONS_FIELD);
            builder.endObject(); // exists
            builder.endObject(); // must_not
            builder.endObject(); // bool
            builder.endObject(); // outer object

            // Branch B: user holds ALL of the document's required privileges
            builder.startObject();
            builder.startObject("terms_set");
            builder.startObject(PERMISSIONS_FIELD);
            builder.array("terms", actions.toArray(new String[0]));
            builder.field("minimum_should_match_field", PERMISSIONS_COUNT_FIELD);
            builder.endObject(); // PERMISSIONS_FIELD
            builder.endObject(); // terms_set
            builder.endObject(); // outer object

            builder.endArray(); // should
            builder.endObject(); // bool
            builder.endObject(); // outer object

            builder.endArray(); // must
            builder.endObject(); // bool
            builder.endObject(); // top-level

            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Holds the union of resources (space references) and action strings collected from matching
     * application-privilege grants.
     */
    record ResourcesAndActions(Set<String> resources, Set<String> actions) {}
}
