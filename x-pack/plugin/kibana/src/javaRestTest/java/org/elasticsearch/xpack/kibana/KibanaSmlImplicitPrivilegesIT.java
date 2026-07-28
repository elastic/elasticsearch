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

/**
 * End-to-end coverage for {@code KibanaSmlImplicitPrivilegesProvider} against a real
 * default-distribution node. Unlike an in-JVM {@code internalClusterTest}, this exercises the
 * full production path: the plugin is bundled into the default distribution and auto-discovered
 * via the {@code SecurityExtension} SPI, so no test plugin is installed.
 * <p>
 * The happy path verifies that a role holding only the Kibana {@code feature_dashboards.read}
 * application privilege on {@code space:marketing} (with <b>no</b> explicit index privileges)
 * can read the {@code ai-index-idx-sml-data} index, and that the implicit document-level-security
 * filter restricts results using composite tokens stored in {@code permissions.kibana.privileges.name}
 * that bind space and privilege together:
 * <ul>
 *   <li>Documents requiring a token for a different space are hidden even if the privilege matches.</li>
 *   <li>Documents requiring a token for a privilege the user does not hold are hidden even if the
 *       space matches.</li>
 *   <li>Documents requiring <em>multiple</em> composite tokens where the user lacks one are hidden
 *       (AND semantics enforced by {@code terms_set} with
 *       {@code minimum_should_match_field: permissions_count}).</li>
 *   <li>Documents with no {@code permissions.kibana.privileges.name} field are always visible
 *       (public documents).</li>
 * </ul>
 */
public class KibanaSmlImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static final String SML_USER = "kibana_sml_user";
    private static final String SML_USER_PASSWORD = "kibana-sml-password";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String DASHBOARDS_PRIVILEGE = "feature_dashboards.read";
    private static final String LOGIN_ACTION = "login:";
    private static final String DASHBOARD_GET_ACTION = "saved_object:dashboard/get";

    private static final String SML_INDEX = "ai-index-idx-sml-data";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-sml-implicit-privileges-cluster")
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

    public void testSpaceAndPrivilegeScopedRoleImplicitlyReadsSmlDataWithDls() throws Exception {
        // 1. Register the Kibana application privilege — actions must include login: for the provider to trigger.
        putKibanaDashboardsPrivilege();

        // 2. A role holding ONLY that application privilege, scoped to space:marketing — no explicit index
        // privileges.
        putSmlReaderRole("sml_marketing_reader", "space:marketing");

        // 3. A user that holds the role.
        putUser(SML_USER, SML_USER_PASSWORD, "sml_marketing_reader");

        // 4. As admin, create the SML index with explicit mappings so DLS term/terms queries match.
        createSmlIndexWithDocs();

        // 5. The implicit grant surfaces through the get-role API, carrying the composite dls_tokens DLS query.
        assertImplicitGrantSurfaced("sml_marketing_reader");

        // 6. The user can read the SML index without any explicit index privilege, and DLS restricts the visible
        // documents to exactly the two that satisfy both the space and privilege dimensions.
        assertUserSeesExactlyMarketingDashboardAndGlobalNoPerms();
    }

    private void putKibanaDashboardsPrivilege() throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(Strings.format("""
            {
              "%s": {
                "%s": {
                  "actions": ["%s", "%s"]
                }
              }
            }
            """, KIBANA_APPLICATION, DASHBOARDS_PRIVILEGE, LOGIN_ACTION, DASHBOARD_GET_ACTION));
        assertOK(client().performRequest(request));
    }

    private void putSmlReaderRole(String roleName, String resource) throws Exception {
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

    private void createSmlIndexWithDocs() throws Exception {
        // permissions.kibana.privileges.name must be keyword so the implicit terms_set DLS query matches;
        // permissions.kibana.privileges.count must be integer so the minimum_should_match_field works correctly.
        final Request create = new Request("PUT", "/" + SML_INDEX);
        create.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "spaces": { "type": "keyword" },
                  "permissions": {
                    "properties": {
                      "kibana": {
                        "properties": {
                          "privileges": {
                            "properties": {
                              "name": { "type": "keyword" },
                              "count": { "type": "integer" }
                            }
                          }
                        }
                      }
                    }
                  },
                  "title": { "type": "keyword" }
                }
              }
            }
            """);
        assertOK(client().performRequest(create));

        // Should be visible: user holds marketing|saved_object:dashboard/get.
        indexSmlDoc("marketing-dashboard", """
            {
              "spaces": ["marketing"],
              "permissions": {
                "kibana": {
                  "privileges": {
                    "name": "marketing|saved_object:dashboard/get",
                    "count": 1
                  }
                }
              },
              "title": "marketing dashboard"
            }
            """);

        // Should NOT be visible: user does not hold finance|saved_object:dashboard/get (wrong space in token).
        indexSmlDoc("finance-dashboard", """
            {
              "spaces": ["finance"],
              "permissions": {
                "kibana": {
                  "privileges": {
                    "name": "finance|saved_object:dashboard/get",
                    "count": 1
                  }
                }
              },
              "title": "finance dashboard"
            }
            """);

        // Should NOT be visible: user doesn't hold marketing|saved_object:lens/get (privilege not in grant).
        indexSmlDoc("marketing-lens", """
            {
              "spaces": ["marketing"],
              "permissions": {
                "kibana": {
                  "privileges": {
                    "name": "marketing|saved_object:lens/get",
                    "count": 1
                  }
                }
              },
              "title": "marketing lens"
            }
            """);

        // Should be visible: no permissions field → public document.
        indexSmlDoc("global-no-perms", """
            {
              "spaces": ["*"],
              "title": "global no perms"
            }
            """);

        // Should NOT be visible: requires both tokens — AND semantics via terms_set;
        // user only holds marketing|saved_object:dashboard/get, not marketing|saved_object:lens/get.
        indexSmlDoc("multi-perm", """
            {
              "spaces": ["marketing"],
              "permissions": {
                "kibana": {
                  "privileges": {
                    "name": [
                      "marketing|saved_object:dashboard/get",
                      "marketing|saved_object:lens/get"
                    ],
                    "count": 2
                  }
                }
              },
              "title": "multi perm"
            }
            """);
    }

    private void indexSmlDoc(String id, String body) throws Exception {
        final Request request = new Request("PUT", "/" + SML_INDEX + "/_doc/" + id);
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
            .filter(entry -> ((List<String>) entry.get("names")).contains(SML_INDEX))
            .toList();
        assertThat("expected exactly one implicit " + SML_INDEX + " grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("names"), equalTo(List.of(SML_INDEX)));
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));

        final String query = (String) implicit.get("query");
        assertThat(query, containsString("permissions.kibana.privileges.name"));
        assertThat(query, containsString("permissions.kibana.privileges.count"));
        assertThat(query, containsString("marketing|saved_object:dashboard/get"));
        assertThat(query, containsString("terms_set"));
    }

    @SuppressWarnings("unchecked")
    private void assertUserSeesExactlyMarketingDashboardAndGlobalNoPerms() throws Exception {
        final Request search = new Request("GET", "/" + SML_INDEX + "/_search");
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
