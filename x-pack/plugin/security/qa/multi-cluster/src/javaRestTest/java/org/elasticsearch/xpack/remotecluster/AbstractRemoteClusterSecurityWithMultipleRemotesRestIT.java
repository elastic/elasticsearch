/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.ObjectPath;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public abstract class AbstractRemoteClusterSecurityWithMultipleRemotesRestIT extends AbstractRemoteClusterSecurityTestCase {

    protected static ElasticsearchCluster otherFulfillingCluster;
    protected static RestClient otherFulfillingClusterClient;

    @BeforeClass
    public static void initOtherFulfillingClusterClient() {
        if (otherFulfillingClusterClient != null) {
            return;
        }
        otherFulfillingClusterClient = buildRestClient(otherFulfillingCluster);
    }

    @AfterClass
    public static void closeOtherFulfillingClusterClient() throws IOException {
        try {
            IOUtils.close(otherFulfillingClusterClient);
        } finally {
            otherFulfillingClusterClient = null;
        }
    }

    public void testCrossClusterSearch() throws Exception {
        configureRemoteCluster();
        configureRolesOnClusters();

        // Fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "cluster1_index1" } }
                { "name": "doc1" }
                { "index": { "_index": "cluster1_index2" } }
                { "name": "doc2" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Other fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "cluster2_index1" } }
                { "name": "doc1" }
                { "index": { "_index": "cluster2_index2" } }
                { "name": "doc2" }
                """));
            assertOK(performRequestAgainstOtherFulfillingCluster(bulkRequest));
        }

        // Query cluster
        {
            // Index some documents, to use them in a multi-cluster search
            final var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"name\": \"doc1\"}");
            assertOK(client().performRequest(indexDocRequest));

            // Search across local cluster and both remotes
            searchAndAssertIndicesFound(
                String.format(
                    Locale.ROOT,
                    "/local_index,%s:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_*", "*"),
                    randomFrom("*_index1", "*"),
                    randomBoolean()
                ),
                "cluster1_index1",
                "cluster2_index1",
                "local_index"
            );

            // Search across both remotes using cluster alias wildcard
            searchAndAssertIndicesFound(
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_*", "*"),
                    randomFrom("*_index1", "*"),
                    randomBoolean()
                ),
                "cluster1_index1",
                "cluster2_index1"
            );

            // Search across both remotes using explicit cluster aliases
            searchAndAssertIndicesFound(
                String.format(
                    Locale.ROOT,
                    "/my_remote_cluster:%s,my_remote_cluster_2:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("cluster1_index1", "*_index1", "*"),
                    randomFrom("cluster2_index1", "*_index1", "*"),
                    randomBoolean()
                ),
                "cluster1_index1",
                "cluster2_index1"
            );

            // Search single remote
            final boolean searchFirstCluster = randomBoolean();
            final String index1 = searchFirstCluster ? "cluster1_index1" : "cluster2_index1";
            searchAndAssertIndicesFound(
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    searchFirstCluster ? "my_remote_cluster" : "my_remote_cluster_2",
                    randomFrom(index1, "*_index1", "*"),
                    randomBoolean()
                ),
                index1
            );

            // To simplify the test setup, we only ever (randomly) set skip_unavailable on the other remote, not on both,
            // i.e. the first remote cluster always has skip_unavailable = false.
            // This impacts below failure scenarios; in some cases, skipping the other remote results in overall request success
            final boolean skipUnavailableOnOtherCluster = isSkipUnavailable("my_remote_cluster_2");

            // Search when one cluster throws 403
            // No permissions for this index name, so searching for it on either remote will result in 403
            final String missingIndex = "missingIndex";
            final boolean missingIndexOnFirstCluster = randomBoolean();
            // Make sure we search for missing index on at least one remote, possibly both
            final boolean missingIndexOnSecondCluster = false == missingIndexOnFirstCluster || randomBoolean();
            final String searchPath1 = String.format(
                Locale.ROOT,
                "/my_remote_cluster:%s,my_remote_cluster_2:%s/_search?ccs_minimize_roundtrips=%s",
                missingIndexOnFirstCluster ? missingIndex : randomFrom("cluster1_index1", "*_index1", "*"),
                missingIndexOnSecondCluster ? missingIndex : randomFrom("cluster2_index1", "*_index1", "*"),
                randomBoolean()
            );
            if (skipUnavailableOnOtherCluster && false == missingIndexOnFirstCluster) {
                // 403 from other cluster is skipped, so we get a result
                searchAndAssertIndicesFound(searchPath1, "cluster1_index1");
            } else {
                searchAndExpect403(searchPath1);
            }

            // Search with cluster alias wildcard matching both remotes, where index is authorized on one but not the other
            final String index2 = randomFrom("cluster1_index1", "cluster2_index1");
            final String searchPath2 = String.format(
                Locale.ROOT,
                "/my_remote_cluster*:%s/_search?ccs_minimize_roundtrips=%s",
                index2,
                randomBoolean()
            );
            if (skipUnavailableOnOtherCluster && index2.equals("cluster1_index1")) {
                // 403 from other cluster is skipped, so we get a result
                searchAndAssertIndicesFound(searchPath2, index2);
            } else {
                searchAndExpect403(searchPath2);
            }

            // Search when both clusters throw 403; in this case we always fail because first cluster is not skipped
            searchAndExpect403(String.format(Locale.ROOT, "/*:%s/_search?ccs_minimize_roundtrips=%s", "missingIndex", randomBoolean()));
        }
    }

    private static boolean isSkipUnavailable(String clusterAlias) throws IOException {
        final Request remoteInfoRequest = new Request("GET", "/_remote/info");
        final Response remoteInfoResponse = adminClient().performRequest(remoteInfoRequest);
        assertOK(remoteInfoResponse);
        final Map<String, Object> remoteInfoMap = responseAsMap(remoteInfoResponse);
        assertThat(remoteInfoMap, hasKey(clusterAlias));
        assertThat(ObjectPath.eval(clusterAlias + ".connected", remoteInfoMap), is(true));
        return ObjectPath.eval(clusterAlias + ".skip_unavailable", remoteInfoMap);
    }

    private static void searchAndExpect403(String searchPath) {
        final ResponseException exception = expectThrows(
            ResponseException.class,
            () -> performRequestWithRemoteSearchUser(new Request("GET", searchPath))
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
    }

    protected abstract void configureRolesOnClusters() throws IOException;

    static void searchAndAssertIndicesFound(String searchPath, String... expectedIndices) throws IOException {
        final Response response = performRequestWithRemoteSearchUser(new Request("GET", searchPath));
        assertOK(response);
        final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
        final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
            .map(SearchHit::getIndex)
            .collect(Collectors.toList());
        assertThat(actualIndices, containsInAnyOrder(expectedIndices));
    }

    static Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }

    static Response performRequestAgainstOtherFulfillingCluster(Request putRoleRequest) throws IOException {
        return performRequestWithAdminUser(otherFulfillingClusterClient, putRoleRequest);
    }
}
