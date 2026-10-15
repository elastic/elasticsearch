/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.authz.privilege.ResolvedApplicationPrivilege;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implicitly grants read access to the Elastic-managed memory AI indices ({@code .ai-memory-*}) for users whose
 * roles include a Kibana application privilege grant carrying at least one {@code ai_index:} action.
 * <p>
 * Memory documents carry the same {@code permissions.kibana.privileges} shape as every other Elastic-managed AI
 * index and, in addition, a multi-valued {@code scope_id} keyword listing the principals that may read the
 * document (mapped by the {@code ai-memory@mappings} component template). The DLS query is the space and
 * privilege check of {@link ElasticAiIndexImplicitPrivilegesProvider} <em>and</em> a scope check: either
 * {@code scope_id} is absent, so the memory is shared with everyone who can read the index, or it contains
 * {@code user://<profile uid>} for the authenticated user. Reserved scope kinds ({@code team://}, {@code space://},
 * {@code entity://}) never match, so documents carrying only those stay hidden until a later provider handles them.
 * <p>
 * The provider runs during role building and does not know who the user is, so the scope clause is emitted as a
 * mustache template over {@code _user.profile_uid}, which {@code SecurityQueryTemplateEvaluator} fills in per
 * request from the profile uid that authorization resolves for actions whose DLS needs it. A user without an activated profile renders an
 * empty uid and sees only unscoped memories. Kibana stamps the same uid on write, so a user's memories and Agent
 * Builder conversations share one identity. The grant is read-only: DLS does not constrain writes, so a user holding it could otherwise stamp any
 * {@code scope_id}. Kibana writes memory as {@code kibana_system}.
 */
public class ElasticAiMemoryImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String[] ELASTIC_MANAGED_AI_MEMORY_INDICES = { ".ai-memory-*" };
    static final String INDEX_READ_PRIVILEGE = "read";

    static final String SCOPE_ID_FIELD = "scope_id";
    static final String USER_SCOPE_PREFIX = "user://";
    static final String PROFILE_UID_TEMPLATE_VARIABLE = "{{_user.profile_uid}}";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        Map<String, Set<String>> resourcesToActions = ElasticAiIndexImplicitPrivilegesProvider.collectResourcesAndActions(
            applicationPrivileges
        );

        String dlsQuery = buildDlsQuery(resourcesToActions);
        if (dlsQuery == null) {
            return List.of();
        }

        return List.of(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(ELASTIC_MANAGED_AI_MEMORY_INDICES)
                .privileges(INDEX_READ_PRIVILEGE)
                .query(dlsQuery)
                .build()
        );
    }

    /**
     * Builds the DLS query as a mustache template: the space and privilege check from
     * {@link ElasticAiIndexImplicitPrivilegesProvider#buildDlsQueryBuilder} filtered by {@link #scopeMatches}.
     * <p>
     * {@code scope_id} is a root-level keyword, not a nested subfield, so unlike the privilege check a root-level
     * {@code exists} on it is sound and is what admits unscoped (shared) memories.
     *
     * @return the serialised template, or {@code null} if the user holds no space-scoped or global grant,
     *         in which case no implicit privilege is granted at all.
     */
    static String buildDlsQuery(Map<String, Set<String>> resourcesToActions) {
        BoolQueryBuilder spaceAndPrivilegeQuery = ElasticAiIndexImplicitPrivilegesProvider.buildDlsQueryBuilder(resourcesToActions);
        if (spaceAndPrivilegeQuery == null) {
            return null;
        }
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(spaceAndPrivilegeQuery).filter(scopeMatches());
        return "{\"template\":{\"source\":" + Strings.toString(query) + "}}";
    }

    /** Matches memories shared with everyone who can read the index, or scoped to the authenticated user. */
    private static BoolQueryBuilder scopeMatches() {
        return QueryBuilders.boolQuery()
            .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(SCOPE_ID_FIELD)))
            .should(QueryBuilders.termQuery(SCOPE_ID_FIELD, USER_SCOPE_PREFIX + PROFILE_UID_TEMPLATE_VARIABLE))
            .minimumShouldMatch(1);
    }
}
