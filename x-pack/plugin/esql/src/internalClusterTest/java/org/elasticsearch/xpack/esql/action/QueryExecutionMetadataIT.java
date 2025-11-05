/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class QueryExecutionMetadataIT extends AbstractCrossClusterTestCase {
    protected static final String IDX_ALIAS = "alias1";
    protected static final String FILTERED_IDX_ALIAS = "alias-filtered-1";

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, randomBoolean());
    }

    protected void assertClusterInfoSuccess(EsqlExecutionInfo.Cluster clusterInfo, int numShards, long overallTookMillis) {
        assertThat(clusterInfo.getIndexExpression(), equalTo("logs-*"));
        assertThat(clusterInfo.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
        super.assertClusterInfoSuccess(clusterInfo, numShards);
    }

    protected EsqlQueryResponse runQuery(String query) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        request.includeExecutionMetadata(true);
        return runQuery(request);
    }

    public void testLocal() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");

        try (EsqlQueryResponse resp = runQuery("from logs-* | stats sum (v)")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(45L)));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(false));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeExecutionMetadata(), equalTo(true));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

            // ensure that the _clusters metadata is always present (since includeExecutionMetadata is true)
            assertClusterMetadataInResponse(resp);
        }
    }

    public void testRemote() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        try (EsqlQueryResponse resp = runQuery("from c*:logs-* | stats sum (v)")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(285L)));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeExecutionMetadata(), equalTo(true));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, remoteNumShards, overallTookMillis);

            // ensure that the _clusters metadata is always present (since includeExecutionMetadata is true)
            assertClusterMetadataInResponse(resp);
        }
    }

    public void testLocalAndRemote() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        try (EsqlQueryResponse resp = runQuery("from logs-*,c*:logs-* | stats sum (v)")) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(330L)));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeExecutionMetadata(), equalTo(true));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, remoteNumShards, overallTookMillis);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

            // ensure that the _clusters metadata is always present (since includeExecutionMetadata is true)
            assertClusterMetadataInResponse(resp);
        }
    }

    private static void assertClusterMetadataInResponse(EsqlQueryResponse resp) {
        try {
            final Map<String, Object> esqlResponseAsMap = XContentTestUtils.convertToMap(resp);
            final Object clusters = esqlResponseAsMap.get("_clusters");

            assertNotNull(clusters);
            // test a few entries to ensure it looks correct (other tests do a full analysis of the metadata in the response)
            @SuppressWarnings("unchecked")
            Map<String, Object> inner = (Map<String, Object>) clusters;
            assertTrue(inner.containsKey("total"));
            assertTrue(inner.containsKey("details"));

        } catch (IOException e) {
            fail("Could not convert ESQL response to Map: " + e);
        }
    }

    Map<String, Object> setupTwoClusters() throws IOException {
        return setupClusters(2);
    }

}
