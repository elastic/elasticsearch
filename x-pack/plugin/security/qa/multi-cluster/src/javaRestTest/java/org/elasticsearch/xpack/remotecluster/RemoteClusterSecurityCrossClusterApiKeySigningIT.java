/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import io.netty.handler.codec.http.HttpMethod;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.audit.AuditLevel;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityCrossClusterApiKeySigningIT extends AbstractRemoteClusterSecurityTestCase {

    // Date Time Formatter used for audit log timestamps.
    // Copied from AuditIT for consistent parsing.
    private static final java.time.format.DateTimeFormatter TSTAMP_FORMATTER = java.time.format.DateTimeFormatter.ofPattern(
        "yyyy-MM-dd'T'HH:mm:ss,SSSZ"
    );

    private static final AtomicReference<Map<String, Object>> MY_REMOTE_API_KEY_MAP_REF = new AtomicReference<>();
    private static final String TEST_ACCESS_JSON = """
        {
            "search": [
              {
                "names": ["index*", "not_found_index"]
              }
            ]
        }""";
    private static final String[] MATCHING_CERTIFICATE_IDENTITY_PATTERNS = new String[] {
        "CN=instance",
        "^CN=instance$",
        "(?i)" + "^CN=instance$",
        "^CN=[A-Za-z0-9_]+$" };

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.audit.enabled", "true")
            .setting(
                "xpack.security.audit.logfile.events.include",
                "[authentication_success, authentication_failed, access_denied, access_granted]"
            )
            .configFile("signing_ca.crt", Resource.fromClasspath("signing/root.crt"))
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .configFile("signing.crt", Resource.fromClasspath("signing/signing.crt"))
            .configFile("signing.key", Resource.fromClasspath("signing/signing.key"))
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (MY_REMOTE_API_KEY_MAP_REF.get() == null) {
                    MY_REMOTE_API_KEY_MAP_REF.set(
                        createCrossClusterAccessApiKey(TEST_ACCESS_JSON, randomFrom(MATCHING_CERTIFICATE_IDENTITY_PATTERNS))
                    );
                }
                return (String) MY_REMOTE_API_KEY_MAP_REF.get().get("encoded");
            })
            .keystore("cluster.remote.invalid_remote.credentials", randomEncodedApiKey())
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testCrossClusterSearchWithCrossClusterApiKeySigning() throws Exception {
        updateClusterSettings(
            Settings.builder()
                .put("cluster.remote.my_remote_cluster.signing.certificate", "signing.crt")
                .put("cluster.remote.my_remote_cluster.signing.key", "signing.key")
                .build()
        );

        updateClusterSettingsFulfillingCluster(
            Settings.builder().put("cluster.remote.signing.certificate_authorities", "signing_ca.crt").build()
        );

        indexTestData();

        // Make sure we can search if cert trusted
        {
            assertCrossClusterSearchSuccessfulWithResult();
        }

        // Test CA that does not trust cert
        {
            // Change the CA to something that doesn't trust the signing cert
            updateClusterSettingsFulfillingCluster(
                Settings.builder().put("cluster.remote.signing.certificate_authorities", "transport-ca.crt").build()
            );
            assertCrossClusterAuthFail("Failed to verify cross cluster api key signature certificate from [(");

            // Change the CA to the default trust store
            updateClusterSettingsFulfillingCluster(Settings.builder().putNull("cluster.remote.signing.certificate_authorities").build());
            assertCrossClusterAuthFail("Failed to verify cross cluster api key signature certificate from [(");

            // Update settings on query cluster to ignore unavailable remotes
            updateClusterSettings(
                Settings.builder().put("cluster.remote.my_remote_cluster.skip_unavailable", Boolean.toString(true)).build()
            );
            assertCrossClusterSearchSuccessfulWithoutResult();

            // Reset skip_unavailable
            updateClusterSettings(
                Settings.builder().put("cluster.remote.my_remote_cluster.skip_unavailable", Boolean.toString(false)).build()
            );

            // Reset ca cert
            updateClusterSettingsFulfillingCluster(
                Settings.builder().put("cluster.remote.signing.certificate_authorities", "signing_ca.crt").build()
            );
            // Confirm reset was successful
            assertCrossClusterSearchSuccessfulWithResult();
        }

        // Test no signature provided
        {
            updateClusterSettings(
                Settings.builder()
                    .putNull("cluster.remote.my_remote_cluster.signing.certificate")
                    .putNull("cluster.remote.my_remote_cluster.signing.key")
                    .build()
            );

            assertCrossClusterAuthFail(
                "API key (type:[cross_cluster], id:["
                    + MY_REMOTE_API_KEY_MAP_REF.get().get("id")
                    + "]) requires certificate identity matching ["
            );

            // Reset
            updateClusterSettings(
                Settings.builder()
                    .put("cluster.remote.my_remote_cluster.signing.certificate", "signing.crt")
                    .put("cluster.remote.my_remote_cluster.signing.key", "signing.key")
                    .build()
            );
        }

        // Test API key without certificate identity and send signature anyway
        {
            updateCrossClusterAccessApiKey(null);
            assertCrossClusterSearchSuccessfulWithResult();

            // Change the CA to the default trust store to make sure untrusted signature fails auth even if it's not required
            updateClusterSettingsFulfillingCluster(Settings.builder().putNull("cluster.remote.signing.certificate_authorities").build());
            assertCrossClusterAuthFail("Failed to verify cross cluster api key signature certificate from [(");
            // Reset
            updateClusterSettingsFulfillingCluster(
                Settings.builder().put("cluster.remote.signing.certificate_authorities", "signing_ca.crt").build()
            );
            updateCrossClusterAccessApiKey(randomFrom(MATCHING_CERTIFICATE_IDENTITY_PATTERNS));
        }

        // Test API key with non-matching certificate identity is rejected
        {
            var nonMatchingCertificateIdentity = randomFrom("", "no-match", "^CN= instance$", "^CN=instance.$", "^cn=instance$");
            updateCrossClusterAccessApiKey(nonMatchingCertificateIdentity);
            assertCrossClusterAuthFail(
                "DN from provided certificate [CN=instance] does not match API Key certificate identity pattern ["
                    + nonMatchingCertificateIdentity
                    + "]"
            );
            // Reset
            updateCrossClusterAccessApiKey(randomFrom(MATCHING_CERTIFICATE_IDENTITY_PATTERNS));
        }
    }

    private void assertCrossClusterAuthFail(String expectedMessage) {
        final long startTimeMillis = System.currentTimeMillis();
        var responseException = assertThrows(ResponseException.class, () -> simpleCrossClusterSearch(randomBoolean()));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(401));
        assertThat(responseException.getMessage(), containsString(expectedMessage));

        try {
            assertAuditLogContainsNewEvent(startTimeMillis, AuditLevel.AUTHENTICATION_FAILED.name().toLowerCase(Locale.ROOT));
        } catch (Exception e) {
            fail(e, "Audit log assertion failed due to an underlying exception (e.g. IOException) when reading the log file.");
        }
    }

    private void assertCrossClusterSearchSuccessfulWithoutResult() throws IOException {
        final long startTimeMillis = System.currentTimeMillis();
        boolean alsoSearchLocally = randomBoolean();
        final Response response = simpleCrossClusterSearch(alsoSearchLocally);
        assertOK(response);
        try {
            assertAuditLogContainsNewEvent(startTimeMillis, AuditLevel.AUTHENTICATION_FAILED.name().toLowerCase(Locale.ROOT));
        } catch (Exception e) {
            fail(e, "Audit log assertion failed due to an underlying exception (e.g. IOException) when reading the log file.");
        }

    }

    private void assertCrossClusterSearchSuccessfulWithResult() throws IOException {
        final long startTimeMillis = System.currentTimeMillis();
        boolean alsoSearchLocally = randomBoolean();
        final Response response = simpleCrossClusterSearch(alsoSearchLocally);
        assertOK(response);

        try {
            assertAuditLogContainsNewEvent(startTimeMillis, AuditLevel.AUTHENTICATION_SUCCESS.name().toLowerCase(Locale.ROOT));
        } catch (Exception e) {
            fail(e, "Audit log assertion failed due to an underlying exception (e.g. IOException) when reading the log file.");
        }

        final SearchResponse searchResponse;
        try (var parser = responseAsParser(response)) {
            searchResponse = SearchResponseUtils.parseSearchResponse(parser);
        }
        try {
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            if (alsoSearchLocally) {
                assertThat(actualIndices, containsInAnyOrder("index1", "local_index"));
            } else {
                assertThat(actualIndices, containsInAnyOrder("index1"));
            }
        } finally {
            searchResponse.decRef();
        }
    }

    private Response simpleCrossClusterSearch(boolean alsoSearchLocally) throws IOException {
        final var searchRequest = new Request(
            "GET",
            String.format(
                Locale.ROOT,
                "/%s%s:%s/_search?ccs_minimize_roundtrips=%s",
                alsoSearchLocally ? "local_index," : "",
                randomFrom("my_remote_cluster", "*", "my_remote_*"),
                randomFrom("index1", "*"),
                randomBoolean()
            )
        );
        return performRequestWithRemoteAccessUser(searchRequest);
    }

    private void indexTestData() throws Exception {
        configureRemoteCluster();

        // Fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "index1" } }
                { "foo": "bar" }
                { "index": { "_index": "index2" } }
                { "bar": "foo" }
                { "index": { "_index": "prefixed_index" } }
                { "baz": "fee" }\n"""));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Query cluster
        {
            // Index some documents, to use them in a mixed-cluster search
            final var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
            assertOK(client().performRequest(indexDocRequest));

            // Create user role with privileges for remote and local indices
            final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "description": "role with privileges for remote and local indices",
                  "cluster": ["manage_own_api_key"],
                  "indices": [
                    {
                      "names": ["local_index"],
                      "privileges": ["read"]
                    }
                  ],
                  "remote_indices": [
                    {
                      "names": ["index1", "not_found_index", "prefixed_index"],
                      "privileges": ["read", "read_cross_cluster"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRoleRequest));
            final var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["remote_search"]
                }""");
            assertOK(adminClient().performRequest(putUserRequest));
        }
    }

    private void updateClusterSettingsFulfillingCluster(Settings settings) throws IOException {
        final var request = newXContentRequest(HttpMethod.PUT, "/_cluster/settings", (builder, params) -> {
            builder.startObject("persistent");
            settings.toXContent(builder, params);
            return builder.endObject();
        });

        performRequestWithAdminUser(fulfillingClusterClient, request);
    }

    private Response performRequestWithRemoteAccessUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }

    private String extractAuditLogTimestamp(String jsonLine, String fieldName) {
        Map<String, Object> jsonMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), jsonLine, false);
        Object value = jsonMap.get(fieldName);
        if (value == null) {
            throw new IllegalArgumentException("Field [" + fieldName + "] not found in log line: " + jsonLine);
        }

        return value.toString();
    }

    private long parseLogTimestamp(String timestamp) {
        try {
            return java.time.ZonedDateTime.parse(timestamp, TSTAMP_FORMATTER).toInstant().toEpochMilli();
        } catch (java.time.format.DateTimeParseException e) {
            throw new RuntimeException("Failed to parse log timestamp: " + timestamp, e);
        }
    }

    private void assertAuditLogContainsNewEvent(long startTimeMillis, String eventAction) throws Exception {
        assertBusy(() -> {
            // Iterate over all nodes in the fulfilling cluster
            for (int i = 0; i < fulfillingCluster.getNumNodes(); i++) {
                try (var auditLog = fulfillingCluster.getNodeLog(i, LogType.AUDIT)) {
                    final List<String> allLines = Streams.readAllLines(auditLog);

                    String expectedLogFragment = "event.action\":\"" + eventAction + "\"";

                    boolean foundNewDetailedLog = allLines.stream()
                        .filter(line -> line.contains(expectedLogFragment))
                        .filter(line -> line.contains("request.name"))
                        .anyMatch(line -> {
                            try {
                                String tsString = extractAuditLogTimestamp(line, "timestamp");
                                long logTimeMillis = parseLogTimestamp(tsString);

                                // Make sure log occurred after the test had started
                                return logTimeMillis >= startTimeMillis;
                            } catch (Exception e) {
                                return false;
                            }
                        });

                    assertThat(
                        "Audit log must contain the expected NEW detailed " + eventAction.replace("_", " ") + " entry.",
                        foundNewDetailedLog,
                        equalTo(true)
                    );
                } catch (IOException e) {
                    logger.warn("Failed to read audit log for node [{}].", i, e);
                    throw e;
                }
            }
        });
    }

    protected static Map<String, Object> createCrossClusterAccessApiKey(String accessJson, String certificateIdentity) {
        initFulfillingClusterClient();
        final var createCrossClusterApiKeyRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createCrossClusterApiKeyRequest.setJsonEntity(Strings.format("""
            {
              "name": "cross_cluster_access_key",
              "certificate_identity": "%s",
              "access": %s
            }""", certificateIdentity, accessJson));
        try {
            final Response createCrossClusterApiKeyResponse = performRequestWithAdminUser(
                fulfillingClusterClient,
                createCrossClusterApiKeyRequest
            );
            assertOK(createCrossClusterApiKeyResponse);
            return responseAsMap(createCrossClusterApiKeyResponse);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected static Map<String, Object> updateCrossClusterAccessApiKey(String certificateIdentity) {
        initFulfillingClusterClient();
        final var createCrossClusterApiKeyRequest = new Request(
            "PUT",
            "/_security/cross_cluster/api_key/" + MY_REMOTE_API_KEY_MAP_REF.get().get("id")
        );
        createCrossClusterApiKeyRequest.setJsonEntity(
            "{\"certificate_identity\": " + (certificateIdentity != null ? "\"" + certificateIdentity + "\"" : "null") + "}"
        );
        try {
            final Response createCrossClusterApiKeyResponse = performRequestWithAdminUser(
                fulfillingClusterClient,
                createCrossClusterApiKeyRequest
            );
            assertOK(createCrossClusterApiKeyResponse);
            return responseAsMap(createCrossClusterApiKeyResponse);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
