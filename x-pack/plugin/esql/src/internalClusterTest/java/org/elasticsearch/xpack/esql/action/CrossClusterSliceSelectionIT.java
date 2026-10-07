/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;

/**
 * The slices a source reads when the source spans clusters. Each cluster reads them from the plan it receives and applies
 * them to its own indices: it routes the query to the shards that hold the slices and tells its search contexts about them.
 */
public class CrossClusterSliceSelectionIT extends AbstractCrossClusterTestCase {

    private static final String SLICED = "tenants";
    private static final String PLAIN = "reference";
    private static final int SHARDS = 5;
    private static final Map<String, Integer> DOCS_PER_SLICE = Map.of("acme", 4, "globex", 3, "initech", 2);
    private static final int ALL_DOCS = 9;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false);
    }

    @Before
    public void setupIndices() {
        assumeTrue("requires slice selection", EsqlCapabilities.Cap.SLICE_SELECTION_FROM_FILTER.isEnabled());
        for (String cluster : List.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1)) {
            createSlicedIndex(cluster);
            populateIndex(cluster, PLAIN, 1, 5);
        }
    }

    public void testSelectedSlicesRouteTheQueryOnEveryCluster() {
        String from = "FROM tenants, cluster-a:tenants METADATA _slice ";

        try (EsqlQueryResponse response = runQuery(from + "| STATS c = COUNT(*)", true)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) 2 * ALL_DOCS))));
            assertThat(response.getExecutionInfo().getCluster(LOCAL_CLUSTER).getTotalShards(), equalTo(SHARDS));
            assertThat(response.getExecutionInfo().getCluster(REMOTE_CLUSTER_1).getTotalShards(), equalTo(SHARDS));
        }
        try (EsqlQueryResponse response = runQuery(from + "| WHERE _slice == \"acme\" | STATS c = COUNT(*)", true)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2L * DOCS_PER_SLICE.get("acme")))));
            assertThat(response.getExecutionInfo().getCluster(LOCAL_CLUSTER).getTotalShards(), equalTo(1));
            assertThat(response.getExecutionInfo().getCluster(REMOTE_CLUSTER_1).getTotalShards(), equalTo(1));
        }
        try (
            EsqlQueryResponse response = runQuery("FROM cluster-a:tenants METADATA _slice | WHERE _slice == \"globex\" | KEEP _slice", true)
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of("globex"), List.of("globex"), List.of("globex"))));
            assertThat(response.getExecutionInfo().getCluster(REMOTE_CLUSTER_1).getTotalShards(), equalTo(1));
        }
    }

    /**
     * {@code _slice} is null on an index without slices, on any cluster: the filter matches none of its documents.
     */
    public void testIndexWithoutSlicesOnAnotherCluster() {
        for (String from : List.of("FROM tenants, cluster-a:reference", "FROM reference, cluster-a:tenants", "FROM cluster-a:reference")) {
            long expected = from.contains("tenants") ? DOCS_PER_SLICE.get("acme") : 0;
            try (EsqlQueryResponse response = runQuery(from + " METADATA _slice | WHERE _slice == \"acme\" | STATS c = COUNT(*)", null)) {
                assertThat(from, getValuesList(response), equalTo(List.of(List.of(expected))));
            }
        }
    }

    public void testSearchFunctionsOnEveryCluster() {
        String from = "FROM tenants, cluster-a:tenants METADATA _slice ";
        try (EsqlQueryResponse response = runQuery(from + "| WHERE MATCH(description, \"document\") | STATS c = COUNT(*)", null)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) 2 * ALL_DOCS))));
        }
        String knn = "| WHERE KNN(vector, [1, 0, 0]) AND _slice IN (\"acme\", \"initech\") | KEEP _slice | LIMIT 100";
        try (EsqlQueryResponse response = runQuery(from + knn, null)) {
            List<List<Object>> rows = getValuesList(response);
            assertThat(rows.isEmpty(), equalTo(false));
            for (List<Object> row : rows) {
                assertThat(row.get(0), in(List.of("acme", "initech")));
            }
        }
    }

    /**
     * The vector field of a slice-enabled index refuses a knn search that selects no slice, on whichever cluster the index is.
     */
    public void testKnnRequiresSelectedSlicesOnEveryCluster() {
        for (String from : List.of("FROM tenants", "FROM cluster-a:tenants", "FROM tenants, cluster-a:tenants")) {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(from + " | WHERE KNN(vector, [1, 0, 0]) | LIMIT 10");
            request.allowPartialResults(false);
            Exception e = expectThrows(Exception.class, () -> runQuery(request).close());
            assertThat(
                from,
                ExceptionsHelper.stackTrace(e),
                containsString("to perform knn search on field [vector], the slices to search must be selected")
            );
        }
    }

    private void createSlicedIndex(String cluster) {
        Client client = client(cluster);
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(SLICED)
                .setSettings(Settings.builder().put("index.number_of_shards", SHARDS).put(IndexSettings.SLICE_ENABLED.getKey(), true))
                .setMapping("""
                    { "properties": {
                      "description": { "type": "text" },
                      "position": { "type": "integer" },
                      "vector": { "type": "dense_vector", "dims": 3, "similarity": "l2_norm" }
                    } }""")
        );
        BulkRequestBuilder bulk = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        DOCS_PER_SLICE.forEach((slice, docs) -> {
            for (int i = 0; i < docs; i++) {
                bulk.add(
                    new IndexRequest(SLICED).id(Integer.toString(i))
                        .source("description", "a document of " + slice, "position", i, "vector", new float[] { 1, i, 0 })
                        .routing(slice)
                        .setRoutingFromSlice(true)
                );
            }
        });
        assertNoFailures(bulk.get());
    }
}
