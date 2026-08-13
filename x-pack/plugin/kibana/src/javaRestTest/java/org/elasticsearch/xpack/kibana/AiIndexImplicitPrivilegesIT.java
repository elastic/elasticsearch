/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * End-to-end coverage for {@code AiIndexImplicitPrivilegesProvider} against a real
 * default-distribution node. Unlike an in-JVM {@code internalClusterTest}, this exercises the
 * full production path: the plugin is bundled into the default distribution and auto-discovered
 * via the {@code SecurityExtension} SPI, so no test plugin is installed.
 * <p>
 * The happy path verifies that a role holding only the Kibana {@code feature_dashboards.read}
 * application privilege on {@code space:marketing} (with <b>no</b> explicit index privileges)
 * can read an {@code ai-index-*} index, and that the implicit document-level-security filter
 * restricts results using composite scoped privileges stored in
 * {@code permissions.kibana.privileges.name} that bind space and privilege together:
 * <ul>
 *   <li>Documents requiring a scoped privilege for a different space are hidden even if the
 *       privilege matches.</li>
 *   <li>Documents requiring a scoped privilege for a privilege the user does not hold are hidden
 *       even if the space matches.</li>
 *   <li>Documents requiring <em>multiple</em> composite scoped privileges where the user lacks one
 *       are hidden (AND semantics enforced by {@code terms_set} with
 *       {@code minimum_should_match_field: permissions_count}).</li>
 *   <li>Documents with no {@code permissions.kibana.privileges.name} field are always visible
 *       (public documents).</li>
 * </ul>
 * <p>
 * The registered privilege deliberately bundles {@code login:} and {@code saved_object:dashboard/get}
 * alongside the {@code ai_index:dashboard/read} action, so the surfaced DLS query also demonstrates
 * that actions outside the {@code ai_index:} namespace never become scoped-privilege terms.
 */
public class AiIndexImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static final String SML_USER = "kibana_sml_user";
    private static final String SML_USER_PASSWORD = "kibana-sml-password";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String DASHBOARDS_PRIVILEGE = "feature_dashboards.read";
    private static final String LOGIN_ACTION = "login:";
    private static final String AI_INDEX_DASHBOARD_READ_ACTION = "ai_index:dashboard/read";
    // Registered alongside the ai_index: action to prove non-ai_index: actions are filtered out of the DLS query.
    private static final String SAVED_OBJECT_GET_ACTION = "saved_object:dashboard/get";

    // Matches the ai-index-idx-* pattern so the stack plugin template auto-applies.
    private static final String AI_INDEX = "ai-index-idx-test";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-ai-index-implicit-privileges-cluster")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.ml.enabled", "false")
        .user(ADMIN_USER, ADMIN_PASSWORD)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", basicAuth(ADMIN_USER, ADMIN_PASSWORD)).build();
    }

    public void testSpaceAndPrivilegeScopedRoleImplicitlyReadsAiIndexDataWithDls() throws Exception {
        // 1. Register the Kibana application privilege.
        putKibanaDashboardsPrivilege();

        // 2. A role holding ONLY that application privilege, scoped to space:marketing — no explicit index privileges.
        putAiIndexReaderRole("ai_marketing_reader", "space:marketing");

        // 3. A user that holds the role.
        putUser(SML_USER, SML_USER_PASSWORD, "ai_marketing_reader");

        // 4. As admin, create the AI Index with explicit mappings so DLS term/terms queries match.
        createAiIndexWithDocs();

        // 5. The implicit grant surfaces through the get-role API, carrying the composite scoped-privileges DLS query.
        assertImplicitGrantSurfaced("ai_marketing_reader");

        // 6. The user can read the AI Index without any explicit index privilege, and DLS restricts the visible
        // documents to exactly the two that satisfy both the space and privilege dimensions.
        assertUserSeesExactlyMarketingDashboardAndGlobalNoPerms();
    }

    private void putKibanaDashboardsPrivilege() throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(Strings.format("""
            {
              "%s": {
                "%s": {
                  "actions": ["%s", "%s", "%s"]
                }
              }
            }
            """, KIBANA_APPLICATION, DASHBOARDS_PRIVILEGE, LOGIN_ACTION, SAVED_OBJECT_GET_ACTION, AI_INDEX_DASHBOARD_READ_ACTION));
        assertOK(client().performRequest(request));
    }

    private void putAiIndexReaderRole(String roleName, String resource) throws Exception {
        final Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity(Strings.format("""
            {
              "cluster": [],
              "applications": [
                {
                  "application": "%s",
                  "privileges": ["%s"],
                  "resources": ["%s"]
                }
              ]
            }
            """, KIBANA_APPLICATION, DASHBOARDS_PRIVILEGE, resource));
        assertOK(client().performRequest(request));
    }

    private void putUser(String username, String password, String role) throws Exception {
        final Request request = new Request("PUT", "/_security/user/" + username);
        request.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles": ["%s"]
            }
            """, password, role));
        assertOK(client().performRequest(request));
    }

    private void createAiIndexWithDocs() throws Exception {
        // No explicit mappings — the ai-index-idx template covers ai-index-idx-*
        // and provides the permissions.kibana.privileges.{name,count} mappings.
        final Request create = new Request("PUT", "/" + AI_INDEX);
        assertOK(client().performRequest(create));

        // Documents deliberately carry no title/description/content: the template maps a semantic_text
        // sub-field on each of those, and populating one would require an inference-capable license.
        // This test is about the DLS filter, and its assertions run off document ids only.

        // Should be visible: user holds marketing|ai_index:dashboard/read.
        indexDoc("marketing-dashboard", """
            {
              "permissions": {
                "kibana": {
                  "privileges": {
                    "name": "marketing|ai_index:dashboard/read",
                    "count": 1
                  }
                }
              },
              "type": "dashboard"
            }
            """);

        // Should NOT be visible: user does not hold finance|ai_index:dashboard/read (wrong space in token).
        indexDoc("finance-dashboard", """
            {
              "permissions": {
                "kibana": {
                  "privileges": {
                    "name": "finance|ai_index:dashboard/read",
                    "count": 1
                  }
                }
              },
              "type": "dashboard"
            }
            """);

        // Should NOT be visible: user doesn't hold marketing|ai_index:workflow/read (privilege not in grant).
        indexDoc("marketing-lens", """
            {
              "permissions": {
                "kibana": {
                  "privileges": {
                    "name": "marketing|ai_index:workflow/read",
                    "count": 1
                  }
                }
              },
              "type": "workflow"
            }
            """);

        // Should be visible: no permissions field → public document.
        indexDoc("global-no-perms", """
            {
              "type": "dashboard"
            }
            """);

        // Should NOT be visible: requires both tokens — AND semantics via terms_set;
        // user only holds marketing|ai_index:dashboard/read, not marketing|ai_index:workflow/read.
        indexDoc("multi-perm", """
            {
              "permissions": {
                "kibana": {
                  "privileges": {
                    "name": [
                      "marketing|ai_index:dashboard/read",
                      "marketing|ai_index:workflow/read"
                    ],
                    "count": 2
                  }
                }
              },
              "type": "dashboard"
            }
            """);
    }

    private void indexDoc(String id, String body) throws Exception {
        final Request request = new Request("PUT", "/" + AI_INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private void assertImplicitGrantSurfaced(String roleName) throws Exception {
        final Request request = new Request("GET", "/_security/role/" + roleName);
        request.addParameter("include_implicit", "true");
        final Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> role = (Map<String, Object>) body.get(roleName);
        final List<Map<String, Object>> indices = (List<Map<String, Object>>) role.get("indices");

        final List<Map<String, Object>> implicitEntries = indices.stream()
            .filter(entry -> Boolean.TRUE.equals(entry.get("implicitly_granted")))
            .filter(entry -> ((List<String>) entry.get("names")).stream().anyMatch(n -> n.startsWith("ai-index-")))
            .toList();
        assertThat("expected exactly one implicit ai-index-* grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));

        final String query = (String) implicit.get("query");
        assertThat(query, containsString("permissions.kibana.privileges.name"));
        assertThat(query, containsString("permissions.kibana.privileges.count"));
        assertThat(query, containsString("marketing|ai_index:dashboard/read"));
        assertThat(query, containsString("terms_set"));
        // Only ai_index: actions become DLS terms — the login: and saved_object: actions in the same grant are dropped.
        assertThat(query, not(containsString(LOGIN_ACTION)));
        assertThat(query, not(containsString(SAVED_OBJECT_GET_ACTION)));
    }

    @SuppressWarnings("unchecked")
    private void assertUserSeesExactlyMarketingDashboardAndGlobalNoPerms() throws Exception {
        final Request search = new Request("GET", "/" + AI_INDEX + "/_search");
        search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(SML_USER, SML_USER_PASSWORD)));
        final Response response = client().performRequest(search);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> hits = (Map<String, Object>) body.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertThat("DLS should restrict the user to marketing-dashboard and global-no-perms, got " + hitList, hitList, hasSize(2));

        final List<String> visibleIds = hitList.stream().map(h -> (String) h.get("_id")).sorted().toList();
        assertThat(visibleIds, equalTo(List.of("global-no-perms", "marketing-dashboard")));
    }

    private static String basicAuth(String username, String password) {
        final String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
