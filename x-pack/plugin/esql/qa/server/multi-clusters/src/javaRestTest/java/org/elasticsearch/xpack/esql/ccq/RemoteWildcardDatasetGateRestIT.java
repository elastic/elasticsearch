/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.ccq.Clusters.REMOTE_CLUSTER_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * The flag-off twin of {@code CrossClusterDatasetIT} (which runs with wildcards matching datasets and asserts
 * {@code FROM <remote>:<wildcard>} is rejected). Here the wildcards feature flag is off on
 * the coordinating (local) cluster, so the remote-dataset detection rail must not fire for wildcards: the gate reads the
 * flag when building the outgoing field-caps request, so the remote reports no datasets and a remote wildcard resolves
 * to remote <em>indices</em> instead of throwing {@code RemoteDatasetNotSupportedException}. This is the test that would
 * have caught the originally-ungated remote rail.
 *
 * <p>The flag is only needed off on the coordinator — the gate is coordinator-side — so the remote keeps the default
 * (federation enabled, so the dataset can be registered there).
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RemoteWildcardDatasetGateRestIT extends ESRestTestCase {

    private static final String WILDCARDS_FLAG_PROPERTY = "es.esql_dataset_wildcards_feature_flag_enabled";
    private static final Path DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster(DATA_PATH, emptyMap(), false);
    static ElasticsearchCluster localCluster = Clusters.localCluster(
        DATA_PATH,
        remoteCluster,
        false,
        emptyMap(),
        false,
        Map.of(WILDCARDS_FLAG_PROPERTY, "false")
    );

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    public void testFlagOffRemoteWildcardResolvesToIndexNotDataset() throws Exception {
        assumeTrue("datasources are only available in snapshot builds", Build.current().isSnapshot());
        // Datasets must exist on both clusters; a BWC leg where either side predates the feature correctly skips.
        List<String> datasetCapability = List.of(EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.capabilityName());
        try (RestClient remoteClient = remoteClusterClient()) {
            assumeTrue(
                "FROM <dataset> requires the capability on both clusters",
                clusterHasCapability("POST", "/_query", List.of(), datasetCapability).orElse(false)
                    && clusterHasCapability(remoteClient, "POST", "/_query", List.of(), datasetCapability).orElse(false)
            );
        }

        final String dataSource = "gate_ds";
        final String dataset = "gate_dataset";
        final String remoteIndex = "gate_logs";

        Path csv = DATA_PATH.resolve("wildcard_gate.csv");
        Files.writeString(csv, "id\n1\n2\n");
        String resource = csv.toUri().toString();

        try (RestClient remoteClient = remoteClusterClient()) {
            // A dataset and a plain index on the remote whose names both match the wildcard "gate*".
            DatasetRegistry.putDataSource(remoteClient, dataSource, "local", Map.of());
            DatasetRegistry.putDataset(remoteClient, dataset, dataSource, resource, Map.of());
            Request indexDoc = new Request("PUT", "/" + remoteIndex + "/_doc/1?refresh=true");
            indexDoc.setJsonEntity("{\"v\":1}");
            remoteClient.performRequest(indexDoc);
            // Guard against a false green: the dataset must really be in the remote's cluster state, else the wildcard
            // never had a dataset to (wrongly) match and the assertions below would pass vacuously.
            assertThat(datasetNames(remoteClient), hasItem(dataset));
        }

        // The wildcard matches the (now-invisible) dataset and the plain index. With the flag off the remote reports no
        // datasets, so the wildcard resolves to gate_logs and the query SUCCEEDS instead of throwing. This is the core
        // "FROM <wildcard> does not bring in datasets, and does not fail" guarantee, across the cluster boundary.
        Response ok = runQuery("FROM " + REMOTE_CLUSTER_NAME + ":gate* | STATS c = COUNT(*)");
        assertThat(ok.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(EntityUtils.toString(ok.getEntity()), not(containsString("remote datasets are not supported")));

        // An exact remote dataset name is not rejected with the courtesy "datasets not supported" message either — with
        // detection off it falls through to a plain unknown remote index (the accepted trade-off; unreadable either way).
        ResponseException exactError = expectThrows(ResponseException.class, () -> runQuery("FROM " + REMOTE_CLUSTER_NAME + ":" + dataset));
        String exactBody = EntityUtils.toString(exactError.getResponse().getEntity());
        assertThat(exactBody, containsString("Unknown index [" + REMOTE_CLUSTER_NAME + ":" + dataset + "]"));
        assertThat(exactBody, not(containsString("remote datasets are not supported")));
    }

    private Response runQuery(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\":\"" + query + "\"}");
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private static List<String> datasetNames(RestClient client) throws IOException {
        Map<String, Object> body = entityAsMap(client.performRequest(new Request("GET", "/_query/dataset")));
        return ((List<Map<String, Object>>) body.get("datasets")).stream().map(h -> (String) h.get("name")).toList();
    }

    private RestClient remoteClusterClient() throws IOException {
        HttpHost[] remoteHosts = parseClusterHosts(remoteCluster.getHttpAddresses()).toArray(HttpHost[]::new);
        return buildClient(restClientSettings(), remoteHosts);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // The dataset/data_source customs are ProjectCustom metadata the shared wipe does not remove; this class-scoped
        // cluster is torn down at the end regardless, so skip the between-test wipe that cannot reach them.
        return true;
    }
}
