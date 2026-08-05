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
import org.elasticsearch.client.WarningsHandler;
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
 * End-to-end coverage for {@code KibanaCasesImplicitPrivilegesProvider} against a real
 * default-distribution node. Unlike an in-JVM {@code internalClusterTest}, this exercises the
 * full production path: the plugin is bundled into the default distribution and auto-discovered
 * via the {@code SecurityExtension} SPI, so no test plugin is installed.
 * <p>
 * The happy path verifies that a role holding only the Kibana
 * {@code cases:observability/getCase} application privilege on {@code space:marketing} (with
 * <b>no</b> explicit index privileges) can read the {@code .cases-activity*} indices, and that
 * the implicit document-level-security filter restricts results to documents matching
 * <em>both</em> the granted {@code owner} and the granted {@code space_id} - a doc matching only
 * one of the two dimensions must not be visible.
 */
public class KibanaCasesImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static final String CASES_USER = "kibana_cases_user";
    private static final String CASES_USER_PASSWORD = "kibana-cases-password";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String OBSERVABILITY_CASES_PRIVILEGE = "feature_observability_cases.read";
    private static final String OBSERVABILITY_GET_CASE_ACTION = "cases:observability/getCase";
    private static final String CASES_ACTIVITY_INDEX = ".cases-activity-000001";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-cases-implicit-privileges-cluster")
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

    public void testSpaceAndOwnerScopedRoleImplicitlyReadsCasesActivityWithDls() throws Exception {
        // 1. Register the Kibana application privilege whose stored action set the provider keys off of.
        putKibanaObservabilityCasesPrivilege();

        // 2. A role holding ONLY that application privilege, scoped to space:marketing - no explicit index
        // privileges.
        putCasesReaderRole("observability_cases_reader", "space:marketing");

        // 3. A user that holds the role.
        putUser(CASES_USER, CASES_USER_PASSWORD, "observability_cases_reader");

        // 4. As admin, create a .cases-activity index with documents spanning every owner/space combination.
        createCasesActivityIndexWithDocs();

        // 5. The implicit grant surfaces through the get-role API, carrying the owner+space_id DLS query.
        assertImplicitGrantSurfaced("observability_cases_reader");

        // 6. The user can read .cases-activity* without any explicit index privilege, and DLS restricts the
        // visible documents to the granted owner AND the granted space - not either dimension alone.
        assertUserSeesOnlyObservabilityMarketingDoc();
    }

    private void putKibanaObservabilityCasesPrivilege() throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(Strings.format("""
            {
              "%s": {
                "%s": {
                  "actions": ["%s"]
                }
              }
            }
            """, KIBANA_APPLICATION, OBSERVABILITY_CASES_PRIVILEGE, OBSERVABILITY_GET_CASE_ACTION));
        assertOK(client().performRequest(request));
    }

    private void putCasesReaderRole(String roleName, String resource) throws Exception {
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
            """, KIBANA_APPLICATION, OBSERVABILITY_CASES_PRIVILEGE, resource));
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

    private void createCasesActivityIndexWithDocs() throws Exception {
        // owner and space_id must be keywords so the implicit term/terms DLS queries match; dynamic mapping
        // would make them text fields and the filters would never match.
        final Request create = new Request("PUT", "/" + CASES_ACTIVITY_INDEX);
        create.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "owner": { "type": "keyword" },
                  "space_id": { "type": "keyword" },
                  "message": { "type": "keyword" }
                }
              }
            }
            """);
        // Creating a dot-prefixed index emits a deprecation warning that is irrelevant to this test.
        create.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(create));

        // Only the (observability, marketing) doc satisfies both DLS dimensions the role is granted.
        indexDoc("observability-marketing", "observability", "marketing", "observability marketing case");
        indexDoc("observability-finance", "observability", "finance", "observability finance case");
        indexDoc("securitySolution-marketing", "securitySolution", "marketing", "security solution marketing case");
        indexDoc("securitySolution-finance", "securitySolution", "finance", "security solution finance case");
    }

    private void indexDoc(String id, String owner, String spaceId, String message) throws Exception {
        final Request request = new Request("PUT", "/" + CASES_ACTIVITY_INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.format("""
            {
              "owner": "%s",
              "space_id": "%s",
              "message": "%s"
            }
            """, owner, spaceId, message));
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
            .filter(entry -> ((List<String>) entry.get("names")).contains(".cases*"))
            .toList();
        assertThat("expected exactly one implicit .cases* grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("names"), equalTo(List.of(".cases*")));
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));

        final String query = (String) implicit.get("query");
        assertThat(query, containsString("owner"));
        assertThat(query, containsString("observability"));
        assertThat(query, containsString("space_id"));
        assertThat(query, containsString("marketing"));
    }

    @SuppressWarnings("unchecked")
    private void assertUserSeesOnlyObservabilityMarketingDoc() throws Exception {
        final Request search = new Request("GET", "/.cases-activity*/_search");
        search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(CASES_USER, CASES_USER_PASSWORD)));
        final Response response = client().performRequest(search);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> hits = (Map<String, Object>) body.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertThat("DLS should restrict the user to the granted owner AND space, got " + hitList, hitList, hasSize(1));

        final Map<String, Object> source = (Map<String, Object>) hitList.get(0).get("_source");
        assertThat((String) source.get("owner"), equalTo("observability"));
        assertThat((String) source.get("space_id"), equalTo("marketing"));
    }

    private static String basicAuth(String username, String password) {
        final String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
