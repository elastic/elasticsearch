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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;
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
 * Exercises the outgoing half of federation's remote-dataset gate: a coordinator without federation must not ask its
 * remotes to resolve datasets ({@code EsqlResolveFieldsAction} sets {@code resolveDatasets} from its own availability).
 * Without that, a remote that does have federation answers a qualified name that happens to be one of its datasets with
 * {@code RemoteDatasetNotSupportedException}, which names the dataset and so advertises a feature the local deployment
 * has turned off.
 *
 * <p>The local cluster therefore boots with {@code esql.federation.enabled} off while the remote keeps it on and holds a
 * real dataset in its cluster state. {@code FROM <remote>:<dataset>} must fail as a plain missing index, exactly like a
 * nonexistent name. Removing the gate flips the error to the dataset-naming one, so this goes red exactly when the
 * wiring is lost. The complementary incoming half (remote federation off) is covered by
 * {@link FederationRemoteDatasetGateRestIT}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationOutgoingDatasetGateRestIT extends ESRestTestCase {

    private static final Path DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster(DATA_PATH, emptyMap(), false);
    static ElasticsearchCluster localCluster = Clusters.localCluster(DATA_PATH, remoteCluster, false, emptyMap(), false, "false");

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    public void testDisabledLocalResolvesRemoteDatasetAsMissingIndex() throws Exception {
        assumeTrue("datasources are only available in snapshot builds", Build.current().isSnapshot());
        // The premise is a local cluster with federation off, which only holds where the setting exists: a node without
        // it has federation on and would ask the remote to resolve datasets. The remote has to expose the CRUD routes
        // for the dataset this test registers there.
        assumeTrue(
            "the local cluster has to read the federation setting",
            clusterHasCapability("POST", "/_query", List.of(), List.of(EsqlCapabilities.Cap.FEDERATION_ENABLED_SETTING.capabilityName()))
                .orElse(false)
        );
        try (RestClient capabilityClient = remoteClusterClient()) {
            assumeTrue(
                "the remote cluster has to expose the data source routes",
                clusterHasCapability(
                    capabilityClient,
                    "PUT",
                    "/_query/data_source/{name}",
                    List.of(),
                    List.of(EsqlDataSourcesCapabilities.DATA_SOURCES)
                ).orElse(false)
            );
        }

        final String dataSource = "outgoing_gate_ds";
        final String dataset = "outgoing_gate_dataset";
        final String qualified = REMOTE_CLUSTER_NAME + ":" + dataset;

        // A valid local CSV the dataset points at. The dataset is never actually read (the query fails during
        // resolution), but a real file under the allowlisted path keeps registration from tripping on the resource.
        Path csv = DATA_PATH.resolve("outgoing_gate.csv");
        Files.writeString(csv, "id\n1\n2\n");

        try (RestClient remoteClient = remoteClusterClient()) {
            DatasetRegistry.putDataSource(remoteClient, dataSource, "local", Map.of());
            DatasetRegistry.putDataset(remoteClient, dataset, dataSource, csv.toUri().toString(), Map.of());
            // Guard against a false green: without the dataset in the remote's cluster state the query would fall
            // through to "Unknown index" even with the gate removed.
            assertThat(datasetNames(remoteClient), hasItem(dataset));
        }

        Request query = new Request("POST", "/_query");
        query.setJsonEntity("{\"query\":\"FROM " + qualified + "\"}");
        ResponseException error = expectThrows(ResponseException.class, () -> client().performRequest(query));
        String body = EntityUtils.toString(error.getResponse().getEntity());
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("Unknown index [" + qualified + "]"));
        assertThat(body, not(containsString("remote datasets are not supported")));
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
}
