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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implicitly grants read access to the Kibana Cases analytics indices ({@code .cases},
 * {@code .cases-activity}, {@code .cases-attachments}, matched via a single {@code .cases*}
 * pattern) for users whose roles include a solution-scoped Kibana application privilege granting
 * {@code cases:<owner>/getCase}.
 * <p>
 * Unlike Alerting V2, Cases documents carry two independent scoping dimensions: the Kibana
 * space they live in ({@code space_id}) and the owning solution ({@code owner}: {@code cases},
 * {@code observability}, or {@code securitySolution} &mdash; see
 * {@code x-pack/platform/plugins/shared/cases/common/constants/index.ts} in the Kibana repo).
 * Each owner has its own action, constructed by Kibana as {@code cases:<owner>/getCase} (see
 * {@code x-pack/platform/packages/private/security/authorization_core/src/actions/cases.ts}), so
 * a role granting only {@code cases:observability/getCase} on {@code space:marketing} implicitly
 * reads Observability-owned case documents in the marketing space only &mdash; never Security
 * Solution's or the generic stack's, and never other spaces. The DLS query for every grant
 * therefore always includes an {@code owner} filter, with an additional {@code space_id} filter
 * unless the role holds the wildcard resource ({@code *}) for that owner.
 * <p>
 * The index pattern is wildcarded so ES|QL queries like {@code FROM .cases-activity*} match.
 * None of the three indices are data streams today (they are plain indices; {@code .cases} is a
 * {@code index.mode: lookup} index used as the right-hand side of ES|QL {@code LOOKUP JOIN}s from
 * the other two), but the trailing wildcard keeps the pattern resilient to a future
 * reindex-with-suffix rename.
 */
public class KibanaCasesImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String KIBANA_APPLICATION = "kibana-.kibana";

    // Owner values mirror the Kibana-side `Owner` type in:
    // x-pack/platform/plugins/shared/cases/common/constants/index.ts
    // (SECURITY_SOLUTION_OWNER, OBSERVABILITY_OWNER, GENERAL_CASES_OWNER)
    static final String OWNER_SECURITY_SOLUTION = "securitySolution";
    static final String OWNER_OBSERVABILITY = "observability";
    static final String OWNER_CASES = "cases";

    // Action strings are constructed by Kibana as `cases:<owner>/<operation>`; see
    // x-pack/platform/packages/private/security/authorization_core/src/actions/cases.ts and the
    // `getCase` read operation in
    // x-pack/platform/packages/private/security/authorization_core/src/privileges/feature_privilege_builder/cases.ts
    static final String GET_CASE_ACTION_SECURITY_SOLUTION = "cases:securitySolution/getCase";
    static final String GET_CASE_ACTION_OBSERVABILITY = "cases:observability/getCase";
    static final String GET_CASE_ACTION_CASES = "cases:cases/getCase";

    static final Map<String, String> OWNER_BY_GET_CASE_ACTION = Map.of(
        GET_CASE_ACTION_SECURITY_SOLUTION,
        OWNER_SECURITY_SOLUTION,
        GET_CASE_ACTION_OBSERVABILITY,
        OWNER_OBSERVABILITY,
        GET_CASE_ACTION_CASES,
        OWNER_CASES
    );

    // A single ".cases*" pattern covers all three surfaces (.cases, .cases-activity,
    // .cases-attachments), since the latter two share the ".cases" prefix. Index/data-stream
    // names mirror the Kibana-side definitions in:
    // x-pack/platform/plugins/shared/cases/server/cases_analytics_v2/constants.ts
    // Keep this pattern in sync if those definitions change.
    static final String[] CASES_INDICES = { ".cases*" };
    static final String RESOURCE_PREFIX = "space:";
    static final String ALL_RESOURCES = "*";
    static final String INDEX_READ_PRIVILEGE = "read";
    static final String SPACE_ID_FIELD = "space_id";
    static final String OWNER_FIELD = "owner";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        Map<String, Set<String>> resourcesByOwner = collectResourcesByOwner(applicationPrivileges);
        if (resourcesByOwner.isEmpty()) {
            return List.of();
        }

        List<RoleDescriptor.IndicesPrivileges> result = new ArrayList<>(resourcesByOwner.size());
        for (Map.Entry<String, Set<String>> entry : resourcesByOwner.entrySet()) {
            String owner = entry.getKey();
            Set<String> resources = entry.getValue();

            if (resources.contains(ALL_RESOURCES)) {
                result.add(
                    RoleDescriptor.IndicesPrivileges.builder()
                        .indices(CASES_INDICES)
                        .privileges(INDEX_READ_PRIVILEGE)
                        .query(buildOwnerDlsQuery(owner))
                        .build()
                );
                continue;
            }

            Set<String> spaceIds = resources.stream()
                .filter(r -> r.startsWith(RESOURCE_PREFIX))
                .map(r -> r.substring(RESOURCE_PREFIX.length()))
                .collect(Collectors.toSet());
            if (spaceIds.isEmpty()) {
                continue;
            }

            result.add(
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(CASES_INDICES)
                    .privileges(INDEX_READ_PRIVILEGE)
                    .query(buildOwnerAndSpaceIdsDlsQuery(owner, spaceIds))
                    .build()
            );
        }
        return result;
    }

    /**
     * Union of resources, grouped by owner, from every resolved application-privilege grant that
     * targets the Kibana application <i>and</i> authorizes that owner's {@code getCase} action.
     * <p>
     * Each {@link ResolvedApplicationPrivilege} carries a resolved {@link ApplicationPrivilege}
     * whose {@link ApplicationPrivilege#predicate() predicate} already matches every action the
     * grant authorizes &mdash; both the actions of any stored privilege the role referenced by
     * name <em>and</em> any raw action patterns written directly under {@code privileges[]} (e.g.
     * {@code "cases:securitySolution/*"} or {@code "*"}) &mdash; so a single
     * {@code predicate().test(...)} per owner action settles whether the grant authorizes it. A
     * single grant whose predicate matches more than one owner's action (e.g. a {@code "*"}
     * pattern, or a stored privilege bundling multiple owners) contributes its resources to every
     * matching owner independently.
     */
    private static Map<String, Set<String>> collectResourcesByOwner(Collection<ResolvedApplicationPrivilege> applicationPrivileges) {
        Map<String, Set<String>> resourcesByOwner = new HashMap<>();
        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            if (applicationMatchesKibana(privilege.getApplication()) == false) {
                continue;
            }

            for (Map.Entry<String, String> actionAndOwner : OWNER_BY_GET_CASE_ACTION.entrySet()) {
                if (privilege.predicate().test(actionAndOwner.getKey())) {
                    resourcesByOwner.computeIfAbsent(actionAndOwner.getValue(), k -> new HashSet<>()).addAll(resolved.resources());
                }
            }
        }
        return resourcesByOwner;
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

    // Both query builders below are hand-rolled rather than QueryBuilders.termQuery/termsQuery so
    // the stored/surfaced DLS query stays free of the default "boost":1.0 that the query builder
    // classes always serialize - that field would otherwise show up in the query persisted on the
    // role and returned by GET /_security/role/<name>?include_implicit=true.

    static String buildOwnerDlsQuery(String owner) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("term");
            builder.field(OWNER_FIELD, owner);
            builder.endObject();
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String buildOwnerAndSpaceIdsDlsQuery(String owner, Set<String> spaceIds) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("filter");

            builder.startObject();
            builder.startObject("term");
            builder.field(OWNER_FIELD, owner);
            builder.endObject();
            builder.endObject();

            builder.startObject();
            builder.startObject("terms");
            builder.array(SPACE_ID_FIELD, spaceIds.toArray(new String[0]));
            builder.endObject();
            builder.endObject();

            builder.endArray();
            builder.endObject();
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
