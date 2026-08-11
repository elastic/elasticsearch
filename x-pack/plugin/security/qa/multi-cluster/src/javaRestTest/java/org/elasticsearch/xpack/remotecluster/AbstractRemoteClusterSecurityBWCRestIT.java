/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * A set of BWC tests that can be executed with either RCS 1 or RCS 2 against an older fulfilling cluster.
 */
public abstract class AbstractRemoteClusterSecurityBWCRestIT extends AbstractRemoteClusterSecurityTestCase {

    protected abstract boolean isRCS2();

    public void testBwcCCSViaRCS1orRCS2() throws Exception {

        // Fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "remote_index1" } }
                { "foo": "bar" }
                { "index": { "_index": "remote_index2" } }
                { "bar": "foo" }
                """));
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
                  "description": "This description should not be sent to remote clusters.",
                  "cluster": ["manage_own_api_key"],
                  "indices": [
                    {
                      "names": ["local_index", "remote_index1"],
                      "privileges": ["read", "read_cross_cluster"]
                    }
                  ],
                  "remote_indices": [
                    {
                      "names": ["remote_index1"],
                      "privileges": ["read", "read_cross_cluster"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ],
                  "remote_cluster": [
                    {
                      "privileges": ["monitor_enrich"],
                      "clusters": ["*"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRoleRequest));
            if (isRCS2() == false) {
                // We need to define the same role on QC and FC in order for CCS to work.
                final var putRoleRequestFulfilling = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
                putRoleRequestFulfilling.setJsonEntity("""
                    {
                      "cluster": ["manage_own_api_key"],
                      "indices": [
                        {
                          "names": ["remote_index1"],
                          "privileges": ["read", "read_cross_cluster"]
                        }
                      ]
                    }""");
                assertOK(performRequestAgainstFulfillingCluster(putRoleRequestFulfilling));
            }

            final var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["remote_search"]
                }""");
            assertOK(adminClient().performRequest(putUserRequest));

            // Create API key (with REMOTE_SEARCH_USER as owner) which can be used for remote cluster search.
            final var createApiKeyRequest = new Request("PUT", "/_security/api_key");
            // Note: remote_indices should be ignored when sending a request to FC which is on an unsupported version
            createApiKeyRequest.setJsonEntity(randomBoolean() ? """
                {
                  "name": "qc_api_key_with_remote_access",
                  "role_descriptors": {
                    "my_remote_access_role": {
                      "indices": [
                        {
                          "names": ["local_index", "remote_index1", "remote_index2"],
                          "privileges": ["read", "read_cross_cluster"]
                        }
                      ],
                      "remote_indices": [
                        {
                          "names": ["remote_index1", "remote_index2"],
                          "privileges": ["read", "read_cross_cluster"],
                          "clusters": ["my_remote_*", "non_existing_remote_cluster"]
                        }
                      ],
                      "remote_cluster": [
                        {
                          "privileges": ["monitor_enrich", "monitor_stats"],
                          "clusters": ["*"]
                        }
                      ]
                    }
                  }
                }""" : """
                {
                  "name": "qc_api_key_with_remote_access",
                  "role_descriptors": {}
                }""");
            final var createApiKeyResponse = performRequestWithRemoteAccessUser(createApiKeyRequest);
            assertOK(createApiKeyResponse);
            var createApiKeyResponsePath = ObjectPath.createFromResponse(createApiKeyResponse);
            final String apiKeyEncoded = createApiKeyResponsePath.evaluate("encoded");
            final String apiKeyId = createApiKeyResponsePath.evaluate("id");
            assertThat(apiKeyEncoded, notNullValue());
            assertThat(apiKeyId, notNullValue());

            // Check that we can search the fulfilling cluster from the querying cluster
            final boolean alsoSearchLocally = randomBoolean();
            final String remoteClusterName = randomFrom("my_remote_cluster", "*", "my_remote_*");
            final String remoteIndexName = randomFrom("remote_index1", "*");
            final var searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s%s:%s/_search?ccs_minimize_roundtrips=%s",
                    alsoSearchLocally ? "local_index," : "",
                    remoteClusterName,
                    remoteIndexName,
                    randomBoolean()
                )
            );
            String esqlCommand = String.format(Locale.ROOT, "FROM %s,%s:%s | LIMIT 10", "local_index", remoteClusterName, remoteIndexName);
            // send request with user
            Response response = performRequestWithRemoteAccessUser(searchRequest);
            assertOK(response);
            try (var parser = responseAsParser(response)) {
                assertSearchResponse(SearchResponseUtils.parseSearchResponse(parser), alsoSearchLocally);
            }
            assertEsqlResponse(performRequestWithRemoteAccessUser(esqlRequest(esqlCommand)));

            // send request with apikey
            response = performRequestWithApiKey(searchRequest, apiKeyEncoded);
            assertOK(response);
            try (var parser = responseAsParser(response)) {
                assertSearchResponse(SearchResponseUtils.parseSearchResponse(parser), alsoSearchLocally);
            }
            assertEsqlResponse(performRequestWithApiKey(esqlRequest(esqlCommand), apiKeyEncoded));
        }
    }

    /**
     * Managed service accounts carry role names in the authentication object, which fulfilling
     * clusters older than the feature cannot represent: under RCS 2.0 the subject-info validation
     * rejects the forwarded authentication outright, and under RCS 1.0 the remote fails to resolve
     * a role for the unknown service account. Either way the request fails closed. This test pins
     * that behavior for both models. Note: it relies on every wire-compatible BWC version predating
     * managed service accounts; once the feature ships in a release, runs against versions at or
     * after it must assert success instead.
     */
    public void testManagedServiceAccountCcsFailsClosedAgainstOlderFulfillingCluster() throws Exception {
        // Fulfilling cluster (older version): index a document to target
        {
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity("""
                { "index": { "_index": "remote_index_managed" } }
                { "foo": "bar" }
                """);
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Querying cluster (current version): a managed service account with remote privileges
        final String roleName = "managed_bwc_role";
        final var putRoleRequest = new Request("PUT", "/_security/role/" + roleName);
        putRoleRequest.setJsonEntity("""
            {
              "remote_indices": [
                {
                  "names": ["remote_index_managed"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));

        final var putManagedAccountRequest = new Request("PUT", "/_security/service/bwc_poc/worker");
        putManagedAccountRequest.setJsonEntity(Strings.format("""
            { "roles": ["%s"], "enabled": true }""", roleName));
        assertOK(adminClient().performRequest(putManagedAccountRequest));

        final var createTokenRequest = new Request("PUT", "/_security/service/bwc_poc/worker/credential/token/t1");
        final String serviceToken = ObjectPath.createFromResponse(adminClient().performRequest(createTokenRequest)).evaluate("token.value");
        final RequestOptions bearerAuth = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + serviceToken).build();

        try {
            // sanity check: the managed account authenticates locally on the querying cluster
            final Request authenticateRequest = new Request("GET", "/_security/_authenticate");
            authenticateRequest.setOptions(bearerAuth);
            assertOK(client().performRequest(authenticateRequest));

            // CCS against the pre-feature fulfilling cluster must fail closed
            final Request searchRequest = new Request(
                "GET",
                "/my_remote_cluster:remote_index_managed/_search?ccs_minimize_roundtrips=" + randomBoolean()
            );
            searchRequest.setOptions(bearerAuth);
            final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), greaterThanOrEqualTo(400));
            if (isRCS2()) {
                // the fulfilling cluster rejects the forwarded authentication during subject-info validation
                assertThat(exception.getMessage(), containsString("must have no role"));
            } else {
                // with assertions enabled (as in these test clusters) the fulfilling cluster trips the
                // consistency check while deserializing the authentication header, which surfaces as a
                // wrapped header-verification error; without assertions (production) deserialization
                // succeeds and the failure surfaces at role resolution instead
                assertThat(
                    exception.getMessage(),
                    anyOf(
                        containsString("failed to verify signed authentication information"),
                        containsString("cannot load role for service account"),
                        containsString("must have no role")
                    )
                );
            }
        } finally {
            final Request deleteTokenRequest = new Request("DELETE", "/_security/service/bwc_poc/worker/credential/token/t1");
            assertOK(adminClient().performRequest(deleteTokenRequest));
            final Request deleteAccountRequest = new Request("DELETE", "/_security/service/bwc_poc/worker");
            assertOK(adminClient().performRequest(deleteAccountRequest));
            final Request deleteRoleRequest = new Request("DELETE", "/_security/role/" + roleName);
            assertOK(adminClient().performRequest(deleteRoleRequest));
        }
    }

    private void ensureRemoteFulfillingClusterIsConnected(boolean useProxyMode) throws Exception {
        final int numberOfFcNodes = fulfillingCluster.getHttpAddresses().split(",").length;
        final Request remoteInfoRequest = new Request("GET", "/_remote/info");
        assertBusy(() -> {
            final Response remoteInfoResponse = adminClient().performRequest(remoteInfoRequest);
            assertOK(remoteInfoResponse);
            final Map<String, Object> remoteInfoMap = responseAsMap(remoteInfoResponse);
            assertThat(remoteInfoMap, hasKey("my_remote_cluster"));
            assertThat(org.elasticsearch.xcontent.ObjectPath.eval("my_remote_cluster.connected", remoteInfoMap), is(true));
            if (isRCS2()) {
                assertThat(
                    org.elasticsearch.xcontent.ObjectPath.eval("my_remote_cluster.cluster_credentials", remoteInfoMap),
                    is("::es_redacted::") // RCS 2.0
                );
            } else {
                assertThat(org.elasticsearch.xcontent.ObjectPath.eval("my_remote_cluster.cluster_credentials", remoteInfoMap), nullValue());
            }
            if (false == useProxyMode) {
                assertThat(
                    org.elasticsearch.xcontent.ObjectPath.eval("my_remote_cluster.num_nodes_connected", remoteInfoMap),
                    equalTo(numberOfFcNodes)
                );
            }
        });
    }

    private Response performRequestWithRemoteAccessUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }

    private Response performRequestWithApiKey(final Request request, final String encoded) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encoded));
        return client().performRequest(request);
    }

    private void setupQueryClusterRCS1(boolean useProxyMode) throws IOException {
        final Settings.Builder builder = Settings.builder();
        if (useProxyMode) {
            builder.put("cluster.remote.my_remote_cluster.mode", "proxy")
                .put("cluster.remote.my_remote_cluster.proxy_address", fulfillingCluster.getTransportEndpoint(0));
        } else {
            builder.put("cluster.remote.my_remote_cluster.mode", "sniff")
                .putList("cluster.remote.my_remote_cluster.seeds", fulfillingCluster.getTransportEndpoint(0));
        }
        updateClusterSettings(builder.build());
    }

    private Request esqlRequest(String command) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder();
        body.startObject();
        body.field("query", command);
        body.field("include_ccs_metadata", true);
        body.endObject();
        Request request = new Request("POST", "_query");
        request.setJsonEntity(org.elasticsearch.common.Strings.toString(body));
        return request;
    }

    private void assertSearchResponse(SearchResponse searchResponse, boolean alsoSearchLocally) {
        try {
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            if (alsoSearchLocally) {
                assertThat(actualIndices, containsInAnyOrder("remote_index1", "local_index"));
            } else {
                assertThat(actualIndices, containsInAnyOrder("remote_index1"));
            }
        } finally {
            searchResponse.decRef();
        }
    }

    private void assertEsqlResponse(Response response) throws IOException {
        assertOK(response);
        String responseAsString = EntityUtils.toString(response.getEntity());
        assertThat(responseAsString, containsString("\"my_remote_cluster\":{\"status\":\"successful\""));
        assertThat(responseAsString, containsString("local_bar"));
        assertThat(responseAsString, containsString("bar"));
    }
}
