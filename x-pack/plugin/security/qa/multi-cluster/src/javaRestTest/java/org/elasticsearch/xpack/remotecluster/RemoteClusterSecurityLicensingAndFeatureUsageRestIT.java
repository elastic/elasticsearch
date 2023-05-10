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
import org.elasticsearch.common.settings.Settings;
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

    private static final String REMOTE_INDEX_NAME = "remote_index";
    public static final String ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE_NAME = "advanced-remote-cluster-security";

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(1)
            .apply(commonClusterConfig)
            .setting("xpack.license.self_generated.type", "basic")
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
            .setting("xpack.license.self_generated.type", "basic")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey(Strings.format("""
                        {
                            "search": [
                              {
                                  "names": ["%s"]
                              }
                            ]
                        }""", REMOTE_INDEX_NAME));
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    /**
     * Note: This method is overridden in order to avoid waiting for the successful connection.
     * We start with the basic license which does not support the cross cluster access feature,
     * hence we don't expect the remote cluster handshake to succeed when remote cluster is configured.
     *
     * @param isProxyMode {@code true} if proxy mode should be configured, {@code false} if sniff mode should be configured
     * @throws Exception in case of unexpected errors
     */
    @Override
    protected void configureRemoteCluster(boolean isProxyMode) throws Exception {
        // This method assume the cross cluster access API key is already configured in keystore
        final Settings.Builder builder = Settings.builder();
        if (isProxyMode) {
            builder.put("cluster.remote.my_remote_cluster.mode", "proxy")
                .put("cluster.remote.my_remote_cluster.proxy_address", fulfillingCluster.getRemoteClusterServerEndpoint(0));
        } else {
            builder.put("cluster.remote.my_remote_cluster.mode", "sniff")
                .putList("cluster.remote.my_remote_cluster.seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0));
        }
        updateClusterSettings(builder.build());
    }

    public void testCrossClusterAccessFeatureTrackingAndLicensing() throws Exception {
        assertBasicLicense(fulfillingClusterClient);
        assertBasicLicense(client());

        final boolean useProxyMode = randomBoolean();
        configureRemoteCluster(useProxyMode);

        // Fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "%s" } }
                { "foo": "bar" }\n""", REMOTE_INDEX_NAME));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Querying cluster
        {
            // Create user role with privileges for remote indices
            final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity(Strings.format("""
                {
                  "remote_indices": [
                    {
                      "names": ["%s"],
                      "privileges": ["read", "read_cross_cluster"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""", REMOTE_INDEX_NAME));
            assertOK(adminClient().performRequest(putRoleRequest));
            final var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserRequest.setJsonEntity(Strings.format("""
                {
                  "password": "%s",
                  "roles" : ["%s"]
                }""", PASS.toString(), REMOTE_SEARCH_ROLE));
            assertOK(adminClient().performRequest(putUserRequest));

            final Request searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom(REMOTE_INDEX_NAME, "*"),
                    randomBoolean()
                )
            );

            // Check that CCS fails because we cannot establish connection due to the license check.
            assertRequestFailsDueToUnsupportedLicense(() -> performRequestWithRemoteSearchUser(searchRequest));

            // We start the trial license which supports all features.
            startTrialLicense(fulfillingClusterClient);
            startTrialLicense(client());

            // Check that feature is not tracked before we send CCS request.
            assertFeatureNotTracked(fulfillingClusterClient);
            assertFeatureNotTracked(client());

            // Check that we can search the fulfilling cluster from the querying cluster after license upgrade to trial.
            final Response response = performRequestWithRemoteSearchUser(searchRequest);
            assertOK(response);
            final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
            assertSearchResultContainsIndices(searchResponse, REMOTE_INDEX_NAME);

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
            containsString("current license is non-compliant for [" + ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE_NAME + "]")
        );
    }

    private void assertBasicLicense(RestClient client) throws Exception {
        final var request = new Request("GET", "/_license");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
        assertBusy(() -> {
            final Response response;
            try {
                response = client.performRequest(request);
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
            assertThat(ObjectPath.createFromResponse(response).evaluate("license.type"), equalTo("basic"));
        });
    }

    private void deleteLicenseFromCluster(RestClient client) throws IOException {
        Request request = new Request(HttpDelete.METHOD_NAME, "_license");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
        Response response = client.performRequest(request);
        assertOK(response);
    }

    private void startTrialLicense(RestClient client) throws IOException {
        Request request = new Request("POST", "/_license/start_trial?acknowledge=true");
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
        assertThat(ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE_NAME, is(in(features)));
    }

    private static void assertFeatureNotTracked(RestClient client) throws IOException {
        Set<String> features = fetchFeatureUsageFromNode(client);
        assertThat(ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE_NAME, not(is(in(features))));
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
