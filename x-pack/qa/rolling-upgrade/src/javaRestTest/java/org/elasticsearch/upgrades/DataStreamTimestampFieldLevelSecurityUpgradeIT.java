/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamTimestampFieldLevelSecurityUpgradeIT extends AbstractXpackRollingUpgradeWithSecurityTestCase {

    private static final String FEATURE = "fls_mandatory_timestamp";
    private static final SecureString PASSWORD = new SecureString("ts-upgrade-password".toCharArray());
    private static final String TSDB_DATA_STREAM = "ts-upgrade-metrics";
    private static final String STANDARD_DATA_STREAM = "ts-upgrade-logs";
    private static final String TSDB_INDEX = "ts-upgrade-standalone";
    private static final String STANDARD_INDEX = "ts-upgrade-plain";
    private static final List<String> QUALIFYING = List.of(TSDB_DATA_STREAM, STANDARD_DATA_STREAM, TSDB_INDEX);
    private static final List<String> EXPLICIT_USERS = List.of("ts_explicit", "ts_pattern");
    private static final List<String> GRANTED_USERS = List.of("ts_granted", "ts_union");
    private static final String MAPPINGS = """
        {"properties": {
          "@timestamp": {"type": "date"},
          "host": {"type": "keyword", "time_series_dimension": true},
          "cpu": {"type": "double"}
        }}""";

    // NOTE: phases run in sequence in one JVM, so keys created on the old cluster stay usable in later phases
    private static String implicitApiKey;
    private static String excludingApiKey;

    public DataStreamTimestampFieldLevelSecurityUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testTimestampVisibilityAcrossUpgrade() throws IOException {
        if (isOldCluster()) {
            createIndices();
            createOldClusterRolesAndUsers();
            implicitApiKey = createApiKey("ts_implicit_key", """
                {"grant": ["host", "cpu"]}""");
            excludingApiKey = createApiKey("ts_excluding_key", """
                {"grant": ["*"], "except": ["@timestamp"]}""");
        } else if (isFirstMixedCluster()) {
            changeRolesInMixedCluster();
        } else if (isUpgradedCluster()) {
            assertTrue("upgraded cluster has " + FEATURE, clusterHasFeature(FEATURE));
            rolloverAndIndexIntoNewBackingIndex(TSDB_DATA_STREAM);
            rolloverAndIndexIntoNewBackingIndex(STANDARD_DATA_STREAM);
        }

        final boolean enforced = oldClusterHasFeature(FEATURE) || isUpgradedCluster();
        final List<RestClient> clients = nodeClients();
        try {
            for (RestClient client : clients) {
                assertTimestampVisibility(client, enforced);
            }
        } finally {
            IOUtils.close(clients);
        }
    }

    private void createOldClusterRolesAndUsers() throws IOException {
        createRoleAndUser("ts_implicit", """
            {"grant": ["host", "cpu"]}""");
        createRoleAndUser("ts_explicit", """
            {"grant": ["*"], "except": ["@timestamp"]}""");
        createRoleAndUser("ts_pattern", """
            {"grant": ["*"], "except": ["@*"]}""");
        createRoleAndUser("ts_granted", """
            {"grant": ["@timestamp", "host"]}""");
        createUser("ts_union", List.of("ts_explicit", "ts_granted"));
    }

    private void changeRolesInMixedCluster() throws IOException {
        createRoleAndUser("ts_mixed_implicit", """
            {"grant": ["host"]}""");
        putRole("ts_explicit", """
            {"grant": ["*"], "except": ["@timestamp", "cpu"]}""");
    }

    private void assertTimestampVisibility(RestClient client, boolean enforced) throws IOException {
        for (String target : QUALIFYING) {
            final int total = totalDocuments(target);
            final List<String> implicitCredentials = new ArrayList<>(implicitUsers().stream().map(this::basicAuth).toList());
            implicitCredentials.add(apiKeyAuth(implicitApiKey));
            for (String credentials : implicitCredentials) {
                assertTimestampCount(client, credentials, target, enforced ? total : 0);
            }
            final List<String> explicitCredentials = new ArrayList<>(EXPLICIT_USERS.stream().map(this::basicAuth).toList());
            explicitCredentials.add(apiKeyAuth(excludingApiKey));
            for (String credentials : explicitCredentials) {
                if (enforced) {
                    assertTimestampExclusionRejected(client, credentials, target);
                } else {
                    assertTimestampCount(client, credentials, target, 0);
                }
            }
            for (String user : GRANTED_USERS) {
                assertTimestampCount(client, basicAuth(user), target, total);
            }
        }
        for (String user : List.of("ts_implicit", "ts_explicit", "ts_pattern")) {
            assertTimestampCount(client, basicAuth(user), STANDARD_INDEX, 0);
        }
        for (String apiKey : List.of(implicitApiKey, excludingApiKey)) {
            assertTimestampCount(client, apiKeyAuth(apiKey), STANDARD_INDEX, 0);
        }
    }

    private String basicAuth(String user) {
        return basicAuthHeaderValue(user, PASSWORD);
    }

    private static String apiKeyAuth(String encodedApiKey) {
        return "ApiKey " + encodedApiKey;
    }

    private List<String> implicitUsers() {
        return isOldCluster() ? List.of("ts_implicit") : List.of("ts_implicit", "ts_mixed_implicit");
    }

    private void assertTimestampCount(RestClient client, String credentials, String target, int expected) throws IOException {
        assertThat(describe(credentials, target), timestampCount(client, credentials, target), equalTo(expected));
    }

    private void assertTimestampExclusionRejected(RestClient client, String credentials, String target) {
        final ResponseException e = expectThrows(ResponseException.class, () -> timestampCount(client, credentials, target));
        assertThat(describe(credentials, target), e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(describe(credentials, target), e.getMessage(), containsString("because field level security excludes [@timestamp]"));
    }

    private static String describe(String credentials, String target) {
        final int separator = credentials.indexOf(' ');
        final String decoded = new String(Base64.getDecoder().decode(credentials.substring(separator + 1)), StandardCharsets.UTF_8);
        return credentials.substring(0, separator) + " [" + decoded.substring(0, decoded.indexOf(':')) + "] on " + target;
    }

    private void rolloverAndIndexIntoNewBackingIndex(String dataStream) throws IOException {
        final Response rollover = adminClient().performRequest(new Request("POST", "/" + dataStream + "/_rollover"));
        final String newIndex = ObjectPath.createFromResponse(rollover).evaluate("new_index");
        // NOTE: a new time series backing index starts at the end time of the previous one, which lies in the future
        final long timestamp = dataStream.equals(TSDB_DATA_STREAM) ? timeSeriesStartTime(newIndex) : System.currentTimeMillis();
        assertThat(indexDocument(dataStream, timestamp), equalTo(newIndex));
    }

    private long timeSeriesStartTime(String index) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        final Map<?, ?> indexSettings = (Map<?, ?>) entityAsMap(adminClient().performRequest(request)).get(index);
        final Map<?, ?> settings = (Map<?, ?>) indexSettings.get("settings");
        return Instant.parse((String) settings.get("index.time_series.start_time")).toEpochMilli();
    }

    private void createIndices() throws IOException {
        final Request tsdbTemplate = new Request("PUT", "/_index_template/" + TSDB_DATA_STREAM);
        tsdbTemplate.setJsonEntity(Strings.format("""
            {"index_patterns": ["%s"], "data_stream": {}, "priority": 500, "template": {
              "settings": {"index.mode": "time_series", "index.routing_path": ["host"], "number_of_shards": 3, "number_of_replicas": 1},
              "mappings": %s}}""", TSDB_DATA_STREAM, MAPPINGS));
        assertOK(adminClient().performRequest(tsdbTemplate));
        final Request standardTemplate = new Request("PUT", "/_index_template/" + STANDARD_DATA_STREAM);
        standardTemplate.setJsonEntity(Strings.format("""
            {"index_patterns": ["%s"], "data_stream": {}, "priority": 500, "template": {
              "settings": {"number_of_shards": 3, "number_of_replicas": 1}, "mappings": %s}}""", STANDARD_DATA_STREAM, MAPPINGS));
        assertOK(adminClient().performRequest(standardTemplate));

        final Request tsdbIndex = new Request("PUT", "/" + TSDB_INDEX);
        tsdbIndex.setJsonEntity(Strings.format("""
            {"settings": {"index.mode": "time_series", "index.routing_path": ["host"], "number_of_shards": 3, "number_of_replicas": 1,
              "index.time_series.start_time": "2020-01-01T00:00:00Z", "index.time_series.end_time": "2100-01-01T00:00:00Z"},
             "mappings": %s}""", MAPPINGS));
        assertOK(adminClient().performRequest(tsdbIndex));
        final Request standardIndex = new Request("PUT", "/" + STANDARD_INDEX);
        standardIndex.setJsonEntity(Strings.format("""
            {"settings": {"number_of_shards": 3, "number_of_replicas": 1}, "mappings": %s}""", MAPPINGS));
        assertOK(adminClient().performRequest(standardIndex));

        for (String target : List.of(TSDB_DATA_STREAM, STANDARD_DATA_STREAM, TSDB_INDEX, STANDARD_INDEX)) {
            for (int i = 0; i < 6; i++) {
                indexDocument(target, System.currentTimeMillis());
            }
        }
    }

    private String indexDocument(String target, long timestamp) throws IOException {
        final Request request = new Request("POST", "/" + target + "/_doc");
        request.addParameter("refresh", "true");
        request.setJsonEntity(Strings.format("""
            {"@timestamp": %d, "host": "%s", "cpu": 1.0}""", timestamp, randomAlphaOfLength(6)));
        final Response response = adminClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
        return ObjectPath.createFromResponse(response).evaluate("_index");
    }

    private String createApiKey(String name, String fieldSecurity) throws IOException {
        final Request request = new Request("POST", "/_security/api_key");
        request.setJsonEntity(Strings.format("""
            {"name": "%s", "role_descriptors": {"%s": {"indices": [{"names": ["ts-upgrade-*"], "privileges": ["read"],
              "field_security": %s}]}}}""", name, name, fieldSecurity));
        return ObjectPath.createFromResponse(adminClient().performRequest(request)).evaluate("encoded");
    }

    private void createRoleAndUser(String name, String fieldSecurity) throws IOException {
        putRole(name, fieldSecurity);
        createUser(name, List.of(name));
    }

    private void putRole(String name, String fieldSecurity) throws IOException {
        final Request request = new Request("PUT", "/_security/role/" + name);
        request.setJsonEntity(Strings.format("""
            {"indices": [{"names": ["ts-upgrade-*"], "privileges": ["read"], "field_security": %s}]}""", fieldSecurity));
        assertOK(adminClient().performRequest(request));
    }

    private void createUser(String name, List<String> roles) throws IOException {
        final Request request = new Request("PUT", "/_security/user/" + name);
        request.setJsonEntity(Strings.format("""
            {"password": "%s", "roles": [%s]}""", PASSWORD, String.join(",", roles.stream().map(r -> "\"" + r + "\"").toList())));
        assertOK(adminClient().performRequest(request));
    }

    private List<RestClient> nodeClients() throws IOException {
        final List<RestClient> clients = new ArrayList<>();
        for (NodeInfo node : NodeInfo.getAll(adminClient())) {
            clients.add(buildClient(restClientSettings(), new HttpHost[] { HttpHost.create(node.restEndpoint()) }));
        }
        return clients;
    }

    private int totalDocuments(String target) throws IOException {
        final Request request = new Request("GET", "/" + target + "/_count");
        return ObjectPath.createFromResponse(adminClient().performRequest(request)).evaluate("count");
    }

    private int timestampCount(RestClient client, String credentials, String target) throws IOException {
        final Request request = new Request("GET", "/" + target + "/_search");
        request.setJsonEntity("""
            {"size": 0, "track_total_hits": true, "query": {"exists": {"field": "@timestamp"}}}""");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", credentials));
        final Response response = client.performRequest(request);
        final Map<String, Object> total = ObjectPath.createFromResponse(response).evaluate("hits.total");
        return (Integer) total.get("value");
    }
}
