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
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;
import org.junit.Before;
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
 * The two-real-clusters proof that a dataset registered on a remote cluster is invisible to a query on another one.
 * The remote holds a dataset and an index whose names a single wildcard matches, and both clusters boot normally with
 * federation available at each end, which is the shape the invisibility has to hold in.
 *
 * <p>The exact qualified name must fail as a plain missing index, exactly like a name that was never registered, and
 * the wildcard must come back with the index's row and no partial flag. Both go red if the coordinator starts asking
 * remotes to resolve datasets again, or if the remote starts answering: the query then fails naming the dataset.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RemoteDatasetInvisibleRestIT extends ESRestTestCase {

    private static final Path DATA_PATH = CsvTestUtils.createCsvDataDirectory();

    private static final String DATA_SOURCE = "invisible_ds";
    private static final String DATASET = "invisible_dataset";
    private static final String INDEX = "invisible_logs";
    private static final String QUALIFIED_DATASET = REMOTE_CLUSTER_NAME + ":" + DATASET;
    /** Matches both {@link #DATASET} and {@link #INDEX} on the remote. */
    private static final String QUALIFIED_WILDCARD = REMOTE_CLUSTER_NAME + ":invisible*";

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster(DATA_PATH, emptyMap(), false);
    static ElasticsearchCluster localCluster = Clusters.localCluster(DATA_PATH, remoteCluster, false, emptyMap(), false);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Before
    public void registerDatasetAndIndexOnRemote() throws Exception {
        assumeTrue("datasources are only available in snapshot builds", Build.current().isSnapshot());
        try (RestClient remoteClient = remoteClusterClient()) {
            assumeTrue(
                "the remote cluster has to expose the data source routes",
                clusterHasCapability(
                    remoteClient,
                    "PUT",
                    "/_query/data_source/{name}",
                    List.of(),
                    List.of(EsqlDataSourcesCapabilities.DATA_SOURCES)
                ).orElse(false)
            );

            // A valid local CSV the dataset points at. The dataset is never read, but a real file under the
            // allowlisted path keeps registration from tripping on the resource.
            Path csv = DATA_PATH.resolve("invisible.csv");
            Files.writeString(csv, "id\n1\n2\n");
            DatasetRegistry.putDataSource(remoteClient, DATA_SOURCE, "local", Map.of());
            DatasetRegistry.putDataset(remoteClient, DATASET, DATA_SOURCE, csv.toUri().toString(), Map.of());
            // Guard against a false green: with no dataset in the remote's cluster state both assertions below would
            // hold for the trivial reason that there is nothing to hide.
            assertThat(datasetNames(remoteClient), hasItem(DATASET));

            Request doc = new Request("PUT", "/" + INDEX + "/_doc/1");
            doc.addParameter("refresh", "true");
            doc.setJsonEntity("{\"message\":\"hello\"}");
            remoteClient.performRequest(doc);
        }
    }

    public void testRemoteDatasetResolvesAsMissingIndex() throws Exception {
        ResponseException error = expectThrows(ResponseException.class, () -> query("FROM " + QUALIFIED_DATASET));
        String body = EntityUtils.toString(error.getResponse().getEntity());
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(body, containsString("Unknown index [" + QUALIFIED_DATASET + "]"));
        assertThat(body, not(containsString("remote datasets are not supported")));
    }

    public void testWildcardOverRemoteDatasetReturnsOnlyIndexRows() throws Exception {
        // KEEP pins the row to the index's own field, and LIMIT keeps the default-limit warning out of the response.
        Map<String, Object> response = entityAsMap(query("FROM " + QUALIFIED_WILDCARD + " | KEEP message | LIMIT 100"));
        assertThat(response.get("is_partial"), equalTo(false));
        assertThat(response.get("values"), equalTo(List.of(List.of("hello"))));
    }

    private static org.elasticsearch.client.Response query(String esql) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity("{\"query\":\"" + esql + "\"}");
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
}
