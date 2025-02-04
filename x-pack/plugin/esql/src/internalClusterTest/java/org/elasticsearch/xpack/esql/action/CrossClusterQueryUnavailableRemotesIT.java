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
                assertThat(remote1Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote1Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote1Cluster.getTotalShards(), equalTo(0));
                assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote1Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(remote2NumShards));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(remote2NumShards));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
                assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
                assertThat(localCluster.getSkippedShards(), equalTo(0));
                assertThat(localCluster.getFailedShards(), equalTo(0));

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
                assertThat(remote1Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote1Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote1Cluster.getTotalShards(), equalTo(0));
                assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote1Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("nomatch*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(0));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("nomatch*"));
                // local cluster should never be marked as SKIPPED
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(localCluster.getTotalShards(), equalTo(0));
                assertThat(localCluster.getSuccessfulShards(), equalTo(0));
                assertThat(localCluster.getSkippedShards(), equalTo(0));
                assertThat(localCluster.getFailedShards(), equalTo(0));

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
                assertThat(remote1Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote1Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote1Cluster.getTotalShards(), equalTo(0));
                assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote1Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(0));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
                assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
                assertThat(localCluster.getSkippedShards(), equalTo(0));
                assertThat(localCluster.getFailedShards(), equalTo(0));

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

            // query only the REMOTE_CLUSTER_1
            try (EsqlQueryResponse resp = runQuery("FROM " + REMOTE_CLUSTER_1 + ":logs-* | STATS sum (v)", requestIncludeMeta)) {
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
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remoteCluster.getTotalShards(), equalTo(0));
                assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
                assertThat(remoteCluster.getSkippedShards(), equalTo(0));
                assertThat(remoteCluster.getFailedShards(), equalTo(0));

                // ensure that the _clusters metadata is present only if requested
                assertClusterMetadataInResponse(resp, responseExpectMeta, 1);
            }

            // close remote cluster 2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            // query only the both remote clusters
            try (
                EsqlQueryResponse resp = runQuery(
                    "FROM " + REMOTE_CLUSTER_1 + ":logs-*," + REMOTE_CLUSTER_2 + ":logs-* | STATS sum (v)",
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
                assertThat(remote1Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote1Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote1Cluster.getTotalShards(), equalTo(0));
                assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote1Cluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE_CLUSTER_2);
                assertThat(remote2Cluster.getIndexExpression(), equalTo("logs-*"));
                assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remote2Cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
                assertThat(remote2Cluster.getTotalShards(), equalTo(0));
                assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
                assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
                assertThat(remote2Cluster.getFailedShards(), equalTo(0));

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

            final Exception exception = expectThrows(
                Exception.class,
                () -> runQuery("FROM logs-*,*:logs-* | STATS sum (v)", requestIncludeMeta)
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
            {
                // close the remote cluster so that it is unavailable
                cluster(REMOTE_CLUSTER_1).close();
                Exception exception = expectThrows(Exception.class, () -> runQuery("FROM *:logs-* | STATS sum (v)", requestIncludeMeta));
                assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
            }
            {
                // close remote cluster 2 so that it is unavailable
                cluster(REMOTE_CLUSTER_2).close();
                Exception exception = expectThrows(Exception.class, () -> runQuery("FROM *:logs-* | STATS sum (v)", requestIncludeMeta));
                assertThat(ExceptionsHelper.isRemoteUnavailableException(exception), is(true));
            }
        } finally {
            clearSkipUnavailable(numClusters);
        }
    }
}
