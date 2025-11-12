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

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, randomBoolean());
    }

    protected void assertClusterInfoSuccess(EsqlExecutionInfo.Cluster clusterInfo, int numShards, long overallTookMillis) {
        assertThat(clusterInfo.getIndexExpression(), equalTo("logs-*"));
        assertThat(clusterInfo.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
        super.assertClusterInfoSuccess(clusterInfo, numShards);
    }

    protected EsqlQueryResponse runQueryWithMetadata(String query, Boolean includeExecutionMetadata) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (includeExecutionMetadata != null) {
            request.includeExecutionMetadata(includeExecutionMetadata);
        }
        return runQuery(request);
    }

    public void testLocal() throws Exception {
        testQuery(true, false, 45L);
    }

    public void testRemote() throws Exception {
        testQuery(false, true, 285L);
    }

    public void testLocalAndRemote() throws Exception {
        testQuery(true, true, 330L);
    }

    protected void testQuery(boolean local, boolean remote, long nRecords) throws Exception {
        if (local == false && remote == false) {
            throw new IllegalArgumentException("At least one of local or remote must be true");
        }
        StringBuilder query = new StringBuilder("from ");
        if (local) {
            query.append("logs-*");
            if (remote) {
                query.append(",");
            }
        }
        if (remote) {
            query.append("c*:logs-*");
        }
        query.append(" | stats sum (v)");
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        boolean includeMetadata = randomBoolean();
        try (EsqlQueryResponse resp = runQueryWithMetadata(query.toString(), includeMetadata)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(nRecords)));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(remote));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(
                executionInfo.includeExecutionMetadata(),
                equalTo(
                    includeMetadata ? EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS : EsqlExecutionInfo.IncludeExecutionMetadata.NEVER
                )
            );

            if (remote && local) {
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));
            } else if (remote) {
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1)));
            } else {
                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER)));
            }

            if (remote) {
                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSuccess(remoteCluster, remoteNumShards, overallTookMillis);
            }

            if (local) {
                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);
            }

            assertClusterMetadataInResponse(resp, includeMetadata);
        }
    }

    private static void assertClusterMetadataInResponse(EsqlQueryResponse resp, boolean present) {
        try {
            final Map<String, Object> esqlResponseAsMap = XContentTestUtils.convertToMap(resp);
            final Object clusters = esqlResponseAsMap.get("_clusters");

            if (present) {
                assertNotNull(clusters);
                // test a few entries to ensure it looks correct (other tests do a full analysis of the metadata in the response)
                @SuppressWarnings("unchecked")
                Map<String, Object> inner = (Map<String, Object>) clusters;
                assertTrue(inner.containsKey("total"));
                assertTrue(inner.containsKey("details"));
            } else {
                assertNull(clusters);
            }

        } catch (IOException e) {
            fail("Could not convert ESQL response to Map: " + e);
        }
    }

    Map<String, Object> setupTwoClusters() throws IOException {
        return setupClusters(2);
    }

}
