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
import org.elasticsearch.client.ResponseException;
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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * End-to-end coverage for {@code KibanaNightshiftImplicitPrivilegesProvider} against a real default-distribution
 * node, where the provider is auto-discovered via the {@code SecurityExtension} SPI.
 * <p>
 * Verifies that a role holding only the Kibana {@code api:read_nightshift} application privilege on
 * {@code space:marketing} (with <b>no</b> explicit index privileges) can read the {@code .significant_events-*}
 * detections and knowledge indicators, that DLS restricts space-aware documents to the marketing space while
 * space-agnostic ones (such as knowledge indicators) stay visible, and that a role holding an unrelated Kibana action
 * gets no access.
 */
public class KibanaNightshiftImplicitPrivilegesIT extends ESRestTestCase {

    private static final String ADMIN_USER = "test-admin";
    private static final String ADMIN_PASSWORD = "x-pack-test-password";
    private static final String USER_PASSWORD = "kibana-nightshift-password";

    private static final String KIBANA_APPLICATION = "kibana-.kibana";
    private static final String NIGHTSHIFT_READ_PRIVILEGE = "feature_nightshift.read";
    private static final String STREAMS_READ_PRIVILEGE = "feature_streams.read";
    private static final String DETECTIONS_INDEX = ".significant_events-detections-000001";
    private static final String KNOWLEDGE_INDICATORS_INDEX = ".significant_events-knowledge_indicators-000001";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("kibana-nightshift-implicit-privileges-cluster")
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

    public void testSpaceScopedRoleImplicitlyReadsNightshiftDataWithDls() throws Exception {
        putKibanaPrivileges();
        putRole("nightshift_reader", NIGHTSHIFT_READ_PRIVILEGE, "space:marketing");
        putUser("nightshift_reader_user", "nightshift_reader");
        putRole("streams_reader", STREAMS_READ_PRIVILEGE, "space:marketing");
        putUser("streams_reader_user", "streams_reader");
        createIndicesWithDocs();

        assertImplicitGrantSurfaced("nightshift_reader");

        // Marketing-scoped and space-agnostic documents are visible; the finance detection is filtered out by DLS.
        assertThat(
            searchMessagesAs("nightshift_reader_user"),
            containsInAnyOrder("marketing detection", "space agnostic detection", "knowledge indicator")
        );

        // read_stream does not authorize the Significant Events read routes in Kibana, so it grants no index access.
        final ResponseException e = expectThrows(ResponseException.class, () -> searchMessagesAs("streams_reader_user"));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
    }

    private void putKibanaPrivileges() throws Exception {
        final Request request = new Request("PUT", "/_security/privilege");
        request.setJsonEntity(Strings.format("""
            {
              "%s": {
                "%s": { "actions": ["api:read_nightshift"] },
                "%s": { "actions": ["api:read_stream"] }
              }
            }
            """, KIBANA_APPLICATION, NIGHTSHIFT_READ_PRIVILEGE, STREAMS_READ_PRIVILEGE));
        assertOK(client().performRequest(request));
    }

    private void putRole(String roleName, String privilege, String resource) throws Exception {
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
            """, KIBANA_APPLICATION, privilege, resource));
        assertOK(client().performRequest(request));
    }

    private void putUser(String username, String role) throws Exception {
        final Request request = new Request("PUT", "/_security/user/" + username);
        request.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles": ["%s"]
            }
            """, USER_PASSWORD, role));
        assertOK(client().performRequest(request));
    }

    private void createIndicesWithDocs() throws Exception {
        createNightshiftIndex(DETECTIONS_INDEX);
        createNightshiftIndex(KNOWLEDGE_INDICATORS_INDEX);

        indexDoc(DETECTIONS_INDEX, "marketing-1", """
            { "kibana": { "space_ids": ["marketing"] }, "message": "marketing detection" }""");
        indexDoc(DETECTIONS_INDEX, "finance-1", """
            { "kibana": { "space_ids": ["finance"] }, "message": "finance detection" }""");
        indexDoc(DETECTIONS_INDEX, "agnostic-1", """
            { "message": "space agnostic detection" }""");
        // Knowledge indicators are not space-scoped, so Kibana never writes kibana.space_ids on them.
        indexDoc(KNOWLEDGE_INDICATORS_INDEX, "ki-1", """
            { "message": "knowledge indicator" }""");
    }

    private void createNightshiftIndex(String index) throws Exception {
        // kibana.space_ids must be a keyword (as in Kibana's data streams mappings) so the terms DLS query matches.
        final Request create = new Request("PUT", "/" + index);
        create.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "kibana": { "properties": { "space_ids": { "type": "keyword" } } },
                  "message": { "type": "keyword" }
                }
              }
            }
            """);
        // Creating a dot-prefixed index emits a deprecation warning that is irrelevant to this test.
        create.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        assertOK(client().performRequest(create));
    }

    private void indexDoc(String index, String id, String source) throws Exception {
        final Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.addParameter("refresh", "true");
        request.setJsonEntity(source);
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private void assertImplicitGrantSurfaced(String roleName) throws Exception {
        final Request request = new Request("GET", "/_security/role/" + roleName);
        request.addParameter("include_implicit", "true");
        final Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> role = (Map<String, Object>) entityAsMap(response).get(roleName);
        final List<Map<String, Object>> indices = (List<Map<String, Object>>) role.get("indices");
        final List<Map<String, Object>> implicitEntries = indices.stream()
            .filter(entry -> Boolean.TRUE.equals(entry.get("implicitly_granted")))
            .toList();
        assertThat("expected exactly one implicit grant, got " + indices, implicitEntries, hasSize(1));

        final Map<String, Object> implicit = implicitEntries.get(0);
        assertThat((List<String>) implicit.get("names"), equalTo(List.of(".significant_events-*")));
        assertThat((List<String>) implicit.get("privileges"), equalTo(List.of("read")));
        final String query = (String) implicit.get("query");
        assertThat(query, containsString("kibana.space_ids"));
        assertThat(query, containsString("marketing"));
    }

    @SuppressWarnings("unchecked")
    private List<String> searchMessagesAs(String username) throws Exception {
        // Concrete index names so an unauthorized user gets a 403 rather than an empty wildcard expansion.
        final Request search = new Request("GET", "/" + DETECTIONS_INDEX + "," + KNOWLEDGE_INDICATORS_INDEX + "/_search");
        search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuth(username, USER_PASSWORD)));
        final Response response = client().performRequest(search);
        assertOK(response);

        final Map<String, Object> hits = (Map<String, Object>) entityAsMap(response).get("hits");
        final List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        return hitList.stream().map(hit -> (String) ((Map<String, Object>) hit.get("_source")).get("message")).toList();
    }

    private static String basicAuth(String username, String password) {
        final String token = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
    }
}
