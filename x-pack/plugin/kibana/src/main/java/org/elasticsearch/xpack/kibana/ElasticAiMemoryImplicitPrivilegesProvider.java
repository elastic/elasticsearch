/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.authz.privilege.ResolvedApplicationPrivilege;

import java.io.IOException;
import java.io.UncheckedIOException;
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
 * request from the uid that authorization resolves for actions whose DLS needs it. The user scope clause is only
 * rendered when a uid was resolved, so a user without an activated profile sees only unscoped memories. Kibana
 * stamps the same uid on write, so a user's memories and Agent Builder conversations share one identity.
 * <p>
 * The grant is read-only: DLS does not constrain writes, so a user holding it could otherwise stamp any
 * {@code scope_id}. Kibana writes memory as {@code kibana_system}.
 */
public class ElasticAiMemoryImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String[] ELASTIC_MANAGED_AI_MEMORY_INDICES = { ".ai-memory-*" };

    /** Unscoped memories always match; the user scope is only emitted when {@code _user.profile_uid} is set. */
    static final String SCOPE_CLAUSE = """
        {"bool":{"should":[\
        {"bool":{"must_not":[{"exists":{"field":"scope_id"}}]}}\
        {{#_user.profile_uid}},{"term":{"scope_id":"user://{{_user.profile_uid}}"}}{{/_user.profile_uid}}\
        ],"minimum_should_match":1}}""";

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
                .privileges(ElasticAiIndexImplicitPrivilegesProvider.INDEX_READ_PRIVILEGE)
                .query(dlsQuery)
                .build()
        );
    }

    /**
     * Builds the DLS query as a mustache template: the space and privilege check from
     * {@link ElasticAiIndexImplicitPrivilegesProvider#buildDlsQueryBuilder} filtered by {@link #SCOPE_CLAUSE}. The
     * source is a string rather than an object because the scope clause carries mustache section tags.
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
        String source = "{\"bool\":{\"filter\":[" + Strings.toString(spaceAndPrivilegeQuery) + "," + SCOPE_CLAUSE + "]}}";
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject().startObject("template").field("source", source).endObject().endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
