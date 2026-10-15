/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

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

import static org.elasticsearch.xpack.kibana.ElasticAiIndexImplicitPrivilegesProvider.KIBANA_APPLICATION;
import static org.elasticsearch.xpack.kibana.ElasticAiMemoryImplicitPrivilegesProvider.ELASTIC_MANAGED_AI_MEMORY_INDICES;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ElasticAiMemoryImplicitPrivilegesProviderTests extends ESTestCase {

    private final ElasticAiMemoryImplicitPrivilegesProvider provider = new ElasticAiMemoryImplicitPrivilegesProvider();

    /**
     * The grant targets the memory pattern only, is read-only, and its query is a template whose source ANDs the
     * AI index space/privilege check with the scope clause.
     */
    public void testGrantIsReadOnlyTemplateOverSpaceAndScope() {
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges = List.of(
            new ApplicationPrivilegeDescriptor(KIBANA_APPLICATION, "feature_memory_read", Set.of("ai_index:memory/read"), Map.of())
        );

        Collection<RoleDescriptor.IndicesPrivileges> result = provider.getImplicitIndicesPrivileges(
            resolve(role("feature_memory_read", "space:marketing"), storedPrivileges)
        );
        assertThat(result, hasSize(1));

        RoleDescriptor.IndicesPrivileges privilege = result.iterator().next();
        assertThat(privilege.getIndices(), arrayContainingInAnyOrder(ELASTIC_MANAGED_AI_MEMORY_INDICES));
        assertThat(privilege.getPrivileges(), arrayContainingInAnyOrder("read"));
        assertThat(privilege.getQuery().utf8ToString(), equalTo(EXPECTED_SINGLE_SPACE_TEMPLATE));
    }

    /** The space/privilege half is byte-for-byte the AI index provider's query, so the two grants cannot drift. */
    public void testSpaceClauseMatchesAiIndexProvider() throws Exception {
        Map<String, Set<String>> resourcesToActions = Map.of("space:a", Set.of("ai_index:x/read"));
        Map<String, Object> template = parse(ElasticAiMemoryImplicitPrivilegesProvider.buildDlsQuery(resourcesToActions));
        Map<String, Object> spaceClause = filtersOfSource(template).get(0);
        assertThat(spaceClause, equalTo(parse(ElasticAiIndexImplicitPrivilegesProvider.buildDlsQuery(resourcesToActions))));
    }

    /** Same gate as the AI index provider: no space-scoped or global grant yields no privilege at all. */
    public void testNoGrantReturnsEmpty() {
        assertThat(ElasticAiMemoryImplicitPrivilegesProvider.buildDlsQuery(Map.of()), is(nullValue()));
        assertThat(provider.getImplicitIndicesPrivileges(resolve(role("ai_index:memory/read", "no-prefix"), List.of())), is(empty()));
        assertThat(
            provider.getImplicitIndicesPrivileges(resolve(roleWithApplication("other-app", "ai_index:memory/read", "space:a"), List.of())),
            is(empty())
        );
    }

    // -------------------------------------------------------------------------------------
    // Expected serialisation. Pinned as an exact string deliberately: loose structural matchers
    // let regressions through in a security filter.
    // -------------------------------------------------------------------------------------

    private static final String EXPECTED_SINGLE_SPACE_TEMPLATE = """
        {"template":{"source":{"bool":{"filter":[\
        {"bool":{"should":[\
        {"bool":{"must_not":[{"nested":{"query":{"match_all":{"boost":1.0}},\
        "path":"permissions.kibana.privileges","ignore_unmapped":false,"score_mode":"none","boost":1.0}}],"boost":1.0}},\
        {"nested":{"query":{"bool":{"should":[{"bool":{"filter":[\
        {"bool":{"should":[{"terms":{"permissions.kibana.privileges.space":["marketing"],"boost":1.0}},\
        {"term":{"permissions.kibana.privileges.space":{"value":"*"}}}],"minimum_should_match":"1","boost":1.0}},\
        {"bool":{"should":[{"bool":{"filter":[{"term":{"permissions.kibana.privileges.count":{"value":0}}},\
        {"bool":{"must_not":[{"exists":{"field":"permissions.kibana.privileges.name","boost":1.0}}],"boost":1.0}}],\
        "boost":1.0}},\
        {"bool":{"filter":[{"range":{"permissions.kibana.privileges.count":{"gte":1,"boost":1.0}}},\
        {"terms_set":{"permissions.kibana.privileges.name":{"terms":["ai_index:memory/read"],\
        "minimum_should_match_field":"permissions.kibana.privileges.count","boost":1.0}}}],"boost":1.0}}],\
        "minimum_should_match":"1","boost":1.0}}\
        ],"boost":1.0}}],"boost":1.0}},"path":"permissions.kibana.privileges","ignore_unmapped":false,\
        "score_mode":"none","boost":1.0}}],"boost":1.0}},\
        {"bool":{"should":[{"bool":{"must_not":[{"exists":{"field":"scope_id","boost":1.0}}],"boost":1.0}},\
        {"term":{"scope_id":{"value":"user://{{_user.profile_uid}}"}}}],"minimum_should_match":"1","boost":1.0}}\
        ],"boost":1.0}}}}""";

    // -------------------------------------------------------------------------------------
    // Helpers (mirrors ElasticAiIndexImplicitPrivilegesProviderTests)
    // -------------------------------------------------------------------------------------

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
        RoleDescriptor.ApplicationResourcePrivileges grant = RoleDescriptor.ApplicationResourcePrivileges.builder()
            .application(application)
            .privileges(privilegeName)
            .resources(resources)
            .build();
        return new RoleDescriptor(
            "test_role",
            null,
            null,
            new RoleDescriptor.ApplicationResourcePrivileges[] { grant },
            null,
            null,
            null,
            null
        );
    }

    private Map<String, Object> parse(String json) throws Exception {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.map();
        }
    }

    /** The filter clauses of the template's source bool: [space-and-privilege, scope]. */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> filtersOfSource(Map<String, Object> template) {
        Map<String, Object> source = (Map<String, Object>) ((Map<String, Object>) template.get("template")).get("source");
        return (List<Map<String, Object>>) ((Map<String, Object>) source.get("bool")).get("filter");
    }
}
