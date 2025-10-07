/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClusterQueryUnavailableRemotesIT extends AbstractCrossClusterTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    protected void assertClusterInfoSkipped(EsqlExecutionInfo.Cluster clusterInfo, long overallTookMillis) {
        assertThat(clusterInfo.getIndexExpression(), equalTo("logs-*"));
        assertThat(clusterInfo.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
        super.assertClusterInfoSkipped(clusterInfo);
    }

    protected void assertClusterInfoSuccess(EsqlExecutionInfo.Cluster clusterInfo, int numShards, long overallTookMillis) {
        assertThat(clusterInfo.getIndexExpression(), equalTo("logs-*"));
        assertThat(clusterInfo.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
        super.assertClusterInfoSuccess(clusterInfo, numShards);
    }

    public void testCCSAgainstDisconnectedRemoteWithSkipUnavailableTrue() throws Exception {
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            try (EsqlQueryResponse resp = runQuery("FROM logs-*,*:logs-* | STATS sum (v)", requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(1));
                assertThat(values.get(0), equalTo(List.of(330L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSkipped(remote1Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSuccess(remote2Cluster, remote2NumShards, overallTookMillis);

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, numClusters);
            }

            // scenario where there are no indices to match because
            // 1) the local cluster indexExpression and REMOTE_CLUSTER_2 indexExpression match no indices
            // 2) the REMOTE_CLUSTER_1 is unavailable
            // 3) both remotes are marked as skip_un=true
            String query = "FROM nomatch*," + REMOTE_CLUSTER_1 + ":logs-*," + REMOTE_CLUSTER_2 + ":nomatch* | STATS sum (v)";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSkipped(remote1Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSkipped(remote1Cluster);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("nomatch*"));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("nomatch*"));
                // local cluster should never be marked as SKIPPED
                assertClusterInfoSuccess(localCluster, 0);
                assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, numClusters);
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            try (EsqlQueryResponse resp = runQuery("FROM logs-*,*:logs-* | STATS sum (v)", requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(1));
                assertThat(values.get(0), equalTo(List.of(45L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSkipped(remote1Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSkipped(remote2Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, numClusters);
            }
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    public void testCCSAgainstDisconnectedRemoteWithSkipUnavailableTrueInlineStats() throws Exception {
        assumeTrue("requires inlinestats", EsqlCapabilities.Cap.INLINE_STATS_SUPPORTS_REMOTE.isEnabled());
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            try (EsqlQueryResponse resp = runQuery("FROM logs-*,*:logs-* | INLINE STATS sum (v) | SORT v", requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                // 20 is local + cluster2 but not cluster1
                assertThat(values, hasSize(20));
                values.forEach(row -> assertThat(row.get(4), equalTo(330L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSkipped(remote1Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSuccess(remote2Cluster, remote2NumShards, overallTookMillis);

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, numClusters);
            }

            // scenario where there are no indices to match because
            // 1) the local cluster indexExpression and REMOTE_CLUSTER_2 indexExpression match no indices
            // 2) the REMOTE_CLUSTER_1 is unavailable
            // 3) both remotes are marked as skip_un=true
            String query = "FROM nomatch*," + REMOTE_CLUSTER_1 + ":logs-*," + REMOTE_CLUSTER_2 + ":nomatch* | INLINE STATS sum (v)";
            try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSkipped(remote1Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSkipped(remote1Cluster);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("nomatch*"));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("nomatch*"));
                // local cluster should never be marked as SKIPPED
                assertClusterInfoSuccess(localCluster, 0);
                assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, numClusters);
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            try (EsqlQueryResponse resp = runQuery("FROM logs-*,*:logs-* | INLINE STATS sum (v) | SORT v", requestIncludeMeta)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(10));
                values.forEach(row -> assertThat(row.get(4), equalTo(45L)));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2, LOCAL_CLUSTER)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSkipped(remote1Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSkipped(remote2Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, numClusters);
            }
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    public void testRemoteOnlyCCSAgainstDisconnectedRemoteWithSkipUnavailableTrue() throws Exception {
        int numClusters = 3;
        setupClusters(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            // close remote cluster 1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            String stats = randomStats();
            // query only the REMOTE_CLUSTER_1
            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM " + REMOTE_CLUSTER_1 + ":logs-* | " + stats + " sum (v) | SORT v",
                    requestIncludeMeta
                )
            ) {
                List<ColumnInfoImpl> columns = resp.columns();
                assertThat(columns.size(), equalTo(1));
                // column from an empty result should be {"name":"<no-fields>","type":"null"}
                assertThat(columns.get(0).name(), equalTo("<no-fields>"));
                assertThat(columns.get(0).type(), equalTo(DataType.NULL));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1)));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSkipped(remoteCluster, overallTookMillis);

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, 1);
            }

            // close remote cluster 2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            // query only the both remote clusters
            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM " + REMOTE_CLUSTER_1 + ":logs-*," + REMOTE_CLUSTER_2 + ":logs-* | " + stats + " sum (v)",
                    requestIncludeMeta
                )
            ) {
                List<ColumnInfoImpl> columns = resp.columns();
                assertThat(columns.size(), equalTo(1));
                // column from an empty result should be {"name":"<no-fields>","type":"null"}
                assertThat(columns.get(0).name(), equalTo("<no-fields>"));
                assertThat(columns.get(0).type(), equalTo(DataType.NULL));

                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(0));

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertThat(executionInfo.isPartial(), equalTo(true));

                assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));

                EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSkipped(remote1Cluster, overallTookMillis);

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertClusterInfoSkipped(remote2Cluster, overallTookMillis);

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, 2);
            }

        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    public void testCCSAgainstDisconnectedRemoteWithSkipUnavailableFalse() throws Exception {
        int numClusters = 2;
        setupClusters(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);

        try {
            // close the remote cluster so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();

            String stats = randomStats();
            final Exception exception = expectThrows(
                Exception.class,
                () -> runQuery("FROM logs-*,*:logs-* | " + stats + " sum (v) | SORT v", requestIncludeMeta)
            );
            assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }

    public void testRemoteOnlyCCSAgainstDisconnectedRemoteWithSkipUnavailableFalse() throws Exception {
        int numClusters = 3;
        setupClusters(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, randomBoolean());

        try {
            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            String stats = randomStats();
            {
                // close the remote cluster so that it is unavailable
                cluster(REMOTE_CLUSTER_1).close();
                Exception exception = expectThrows(
                    Exception.class,
                    () -> runQuery("FROM *:logs-* | " + stats + " sum (v) | SORT v", requestIncludeMeta)
                );
                assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
            }
            {
                // close remote cluster 2 so that it is unavailable
                cluster(REMOTE_CLUSTER_2).close();
                Exception exception = expectThrows(
                    Exception.class,
                    () -> runQuery("FROM *:logs-* | " + stats + " sum (v)", requestIncludeMeta)
                );
                assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
            }
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }
}
