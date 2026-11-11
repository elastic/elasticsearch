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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end coverage for {@code KibanaAlertsImplicitPrivilegesProvider} against a real default-distribution
 * node. Unlike an in-JVM {@code internalClusterTest}, this exercises the full production path: the plugin is
 * bundled into the default distribution and auto-discovered via the {@code SecurityExtension} SPI, so no test
 * plugin is installed.
 * <p>
 * The happy path verifies that a role holding only the Kibana {@code alerts:read} application privilege on
 * {@code space:marketing} (with <b>no</b> explicit index privileges) can read the {@code .rule-events*} indices
 * and that the implicit document-level-security filter restricts results to the matching {@code space_id}.
 */
public class KibanaAlertsImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";

    private static final String ALERTS_USER = "kibana_alerts_user";
    private static final String ALERTS_USER_PASSWORD = "kibana-alerts-password";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String ALERTS_PRIVILEGE = "feature_alerting_v2_alerts.read";
    private static final String RULE_EVENTS_INDEX = ".rule-events-000001";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-implicit-privileges-cluster")
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

    public void testSpaceScopedRoleImplicitlyReadsRuleEventsWithDls() throws Exception {
        // 1. Register the Kibana application privilege whose stored action set the provider keys off of.
        putKibanaAlertsPrivilege();

        // 2. A role holding ONLY that application privilege, scoped to space:marketing - no explicit index privileges.
        putAlertsReaderRole("alerts_reader", "space:marketing");

        // 3. A user that holds the role.
        putUser(ALERTS_USER, ALERTS_USER_PASSWORD, "alerts_reader");

        // 4. As admin, create a .rule-events index with documents in two different spaces.
        createRuleEventsIndexWithDocs();

        // 5. The implicit grant surfaces through the get-role API, carrying the space_id DLS query.
        assertImplicitGrantSurfaced("alerts_reader");

        // 6. The user can read .rule-events* without any explicit index privilege, and DLS restricts the
        // visible documents to the granted space.
        assertUserSeesOnlyMarketingDocs();
    }

    private void putKibanaAlertsPrivilege() throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(Strings.format("""
            {
              "%s": {
                "%s": {
                  "actions": ["alerts:read"]
                }
              }
            }
            """, KIBANA_APPLICATION, ALERTS_PRIVILEGE));
        assertOK(client().performRequest(request));
    }

    private void putAlertsReaderRole(String roleName, String resource) throws Exception {
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
            """, KIBANA_APPLICATION, ALERTS_PRIVILEGE, resource));
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

    private void createRuleEventsIndexWithDocs() throws Exception {
        // space_id must be a keyword so the implicit terms DLS query matches; dynamic mapping would make it
        // a text field and the filter would never match.
        final Request create = new Request("PUT", "/" + RULE_EVENTS_INDEX);
        create.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "space_id": { "type": "keyword" },
                  "message": { "type": "keyword" }
                }
              }
            }
            """);
        // Creating a dot-prefixed index emits a deprecation warning that is irrelevant to this test.
        create.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(create));

        indexDoc("marketing-1", "marketing", "marketing event");
        indexDoc("finance-1", "finance", "finance event");
    }

    private void indexDoc(String id, String spaceId, String message) throws Exception {
        final Request request = new Request("PUT", "/" + RULE_EVENTS_INDEX + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.format("""
            {
              "space_id": "%s",
              "message": "%s"
            }
            """, spaceId, message));
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
            .filter(entry -> ((List<String>) entry.get("names")).contains(".rule-events*"))
            .toList();
        assertThat("expected exactly one implicit .rule-events* grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("names"), hasItem(".alert-actions*"));
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));

        final String query = (String) implicit.get("query");
        assertThat(query, containsString("space_id"));
        assertThat(query, containsString("marketing"));
    }

    @SuppressWarnings("unchecked")
    private void assertUserSeesOnlyMarketingDocs() throws Exception {
        final Request search = new Request("GET", "/.rule-events*/_search");
        search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(ALERTS_USER, ALERTS_USER_PASSWORD)));
        final Response response = client().performRequest(search);
        assertOK(response);

        final Map<String, Object> body = entityAsMap(response);
        final Map<String, Object> hits = (Map<String, Object>) body.get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        assertThat("DLS should restrict the user to the granted space, got " + hitList, hitList, hasSize(1));

        final Map<String, Object> source = (Map<String, Object>) hitList.get(0).get("_source");
        assertThat((String) source.get("space_id"), equalTo("marketing"));
    }

    private static String basicAuth(String username, String password) {
        final String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
