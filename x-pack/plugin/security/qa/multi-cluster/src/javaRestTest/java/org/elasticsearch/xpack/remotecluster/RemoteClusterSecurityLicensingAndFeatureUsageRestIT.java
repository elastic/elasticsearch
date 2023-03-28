/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class RemoteClusterSecurityLicensingAndFeatureUsageRestIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    public static final String CONFIGURABLE_CROSS_CLUSTER_ACCESS_FEATURE_NAME = "configurable-cross-cluster-access";

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(1)
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .nodes(1)
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        [
                          {
                             "names": ["remote_index"],
                             "privileges": ["read", "read_cross_cluster"]
                          }
                        ]""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testCrossClusterAccessFeatureTrackingAndLicensing() throws Exception {
        // Check that feature is not tracked before we configure remote clusters.
        // The moment we configure remote clusters, they will establish a connection
        // and the feature usage will be tracked.
        assertFeatureNotTracked(fulfillingClusterClient);
        assertFeatureNotTracked(client());

        configureRemoteClusters();

        // Fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "remote_index" } }
                { "foo": "bar" }\n"""));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Querying cluster
        {
            // Create user role with privileges for remote indices
            final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "remote_indices": [
                    {
                      "names": ["remote_index"],
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

            // Check that we can search the fulfilling cluster from the querying cluster
            final var searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("remote_index", "*"),
                    randomBoolean()
                )
            );
            final Response response = performRequestWithRemoteSearchUser(searchRequest);
            assertOK(response);
            final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
            assertSearchResultContainsIndices(searchResponse, "remote_index");

            // Check that the feature is tracked on both QC and FC.
            assertFeatureTracked(client());
            assertFeatureTracked(fulfillingClusterClient);

            final String licenseDowngradeTestCase = randomFrom("downgrade-fc-only", "downgrade-qc-only", "downgrade-both");
            switch (licenseDowngradeTestCase) {
                case "downgrade-fc-only" -> {
                    // Downgrade fulfilling cluster license to BASIC and run CCS
                    deleteLicenseFromCluster(fulfillingClusterClient);
                    assertRequestFailsDueToUnsupportedLicense(() -> performRequestWithRemoteSearchUser(searchRequest));
                }
                case "downgrade-qc-only" -> {
                    // Downgrade querying cluster license to BASIC and run CCS
                    deleteLicenseFromCluster(client());
                    assertRequestFailsDueToUnsupportedLicense(() -> performRequestWithRemoteSearchUser(searchRequest));
                }
                case "downgrade-both" -> {
                    // Downgrade both fulfilling and querying cluster licenses to BASIC and run CCS
                    deleteLicenseFromCluster(fulfillingClusterClient);
                    deleteLicenseFromCluster(client());
                    assertRequestFailsDueToUnsupportedLicense(() -> performRequestWithRemoteSearchUser(searchRequest));
                }
                default -> throw new IllegalStateException("Unexpected value: " + licenseDowngradeTestCase);
            }
        }
    }

    private void assertSearchResultContainsIndices(SearchResponse searchResponse, String... indices) {
        final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
            .map(SearchHit::getIndex)
            .collect(Collectors.toList());
        assertThat(actualIndices, containsInAnyOrder(indices));
    }

    private void assertRequestFailsDueToUnsupportedLicense(ThrowingRunnable runnable) {
        var exception = expectThrows(ResponseException.class, runnable);
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            exception.getMessage(),
            containsString("current license is non-compliant for [" + CONFIGURABLE_CROSS_CLUSTER_ACCESS_FEATURE_NAME + "]")
        );
    }

    private void deleteLicenseFromCluster(RestClient client) throws IOException {
        Request request = new Request(HttpDelete.METHOD_NAME, "_license");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
        Response response = client.performRequest(request);
        assertOK(response);
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }

    private static void assertFeatureTracked(RestClient client) throws IOException {
        Set<String> features = fetchFeatureUsageFromNode(client);
        assertThat(CONFIGURABLE_CROSS_CLUSTER_ACCESS_FEATURE_NAME, is(in(features)));
    }

    private static void assertFeatureNotTracked(RestClient client) throws IOException {
        Set<String> features = fetchFeatureUsageFromNode(client);
        assertThat(CONFIGURABLE_CROSS_CLUSTER_ACCESS_FEATURE_NAME, not(is(in(features))));
    }

    private static Set<String> fetchFeatureUsageFromNode(RestClient client) throws IOException {
        final Set<String> result = new HashSet<>();
        Request request = new Request(HttpGet.METHOD_NAME, "_license/feature_usage");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
        Response response = client.performRequest(request);
        ObjectPath path = ObjectPath.createFromResponse(response);
        List<Map<String, Object>> features = path.evaluate("features");
        for (var feature : features) {
            result.add((String) feature.get("name"));
        }
        return result;
    }

}
