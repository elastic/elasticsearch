/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.type.EsFieldTests;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EsqlSessionTests extends ESTestCase {

    public void testUpdateExecutionInfoWithUnavailableClusters() {
        // skip_unavailable=true clusters are unavailable, both marked as SKIPPED
        {
            final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            final String remote1Alias = "remote1";
            final String remote2Alias = "remote2";
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", true));

            EsqlSession.updateExecutionInfoWithUnavailableClusters(executionInfo, Set.of(remote1Alias, remote2Alias));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndHasNullCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndHasNullCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndHasNullCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);
        }

        // skip_unavailable=false cluster is unavailable, marked as SKIPPED // TODO: in follow on PR this will change to throwing an
        // Exception
        {
            final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            final String remote1Alias = "remote1";
            final String remote2Alias = "remote2";
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsqlSession.updateExecutionInfoWithUnavailableClusters(executionInfo, Set.of(remote2Alias));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndHasNullCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndHasNullCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndHasNullCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);
        }

        // all clusters available, no Clusters in ExecutionInfo should be modified
        {
            final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            final String remote1Alias = "remote1";
            final String remote2Alias = "remote2";
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsqlSession.updateExecutionInfoWithUnavailableClusters(executionInfo, Set.of());

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndHasNullCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndHasNullCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndHasNullCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }
    }

    public void testUpdateExecutionInfoWithClustersWithNoMatchingIndices() {
        // all clusters present in EsIndex, so no updates to EsqlExecutionInfo should happen
        {
            final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            final String remote1Alias = "remote1";
            final String remote2Alias = "remote2";
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsIndex esIndex = new EsIndex(
                "logs*,remote1:*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of(
                    "logs-a",
                    IndexMode.STANDARD,
                    "remote1:logs-a",
                    IndexMode.STANDARD,
                    "remote2:mylogs1",
                    IndexMode.STANDARD,
                    "remote2:mylogs2",
                    IndexMode.STANDARD,
                    "remote2:logs-b",
                    IndexMode.STANDARD
                )
            );
            IndexResolution indexResolution = IndexResolution.valid(esIndex, Set.of());

            EsqlSession.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndHasNullCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndHasNullCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndHasNullCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }

        // remote1 is missing from EsIndex info, so it should be updated and marked as SKIPPED with 0 total shards, 0 took time, etc.
        {
            final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            final String remote1Alias = "remote1";
            final String remote2Alias = "remote2";
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of(
                    "logs-a",
                    IndexMode.STANDARD,
                    "remote2:mylogs1",
                    IndexMode.STANDARD,
                    "remote2:mylogs2",
                    IndexMode.STANDARD,
                    "remote2:logs-b",
                    IndexMode.STANDARD
                )
            );
            IndexResolution indexResolution = IndexResolution.valid(esIndex, Set.of());

            EsqlSession.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndHasNullCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remote1Cluster.getTook().millis(), equalTo(0L));
            assertThat(remote1Cluster.getTotalShards(), equalTo(0));
            assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
            assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
            assertThat(remote1Cluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndHasNullCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }

        // all remotes are missing from EsIndex info, so they should be updated and marked as SKIPPED with 0 total shards, 0 took time, etc.
        {
            final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            final String remote1Alias = "remote1";
            final String remote2Alias = "remote2";
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of("logs-a", IndexMode.STANDARD)
            );
            // mark remote1 as unavailable
            IndexResolution indexResolution = IndexResolution.valid(esIndex, Set.of(remote1Alias));

            EsqlSession.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndHasNullCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            // remote1 is left as RUNNING, since another method (updateExecutionInfoWithUnavailableClusters) not under test changes status
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(remote1Cluster.getTook());
            assertNull(remote1Cluster.getTotalShards());

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remote2Cluster.getTook().millis(), equalTo(0L));
            assertThat(remote2Cluster.getTotalShards(), equalTo(0));
            assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
            assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
            assertThat(remote2Cluster.getFailedShards(), equalTo(0));
        }
    }

    public void testUpdateExecutionInfoAtEndOfPlanning() {
        String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        String remote1Alias = "remote1";
        String remote2Alias = "remote2";
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
        executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
        executionInfo.swapCluster(
            remote1Alias,
            (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
        );
        executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

        assertNull(executionInfo.planningTookTime());
        assertNull(executionInfo.overallTook());
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {}

        EsqlSession.updateExecutionInfoAtEndOfPlanning(executionInfo);

        assertThat(executionInfo.planningTookTime().millis(), greaterThanOrEqualTo(0L));
        assertNull(executionInfo.overallTook());

        // only remote1 should be altered, since it is the only one marked as SKIPPED when passed into updateExecutionInfoAtEndOfPlanning
        EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertNull(localCluster.getTotalShards());
        assertNull(localCluster.getTook());

        EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
        assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        assertThat(remote1Cluster.getTotalShards(), equalTo(0));
        assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
        assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
        assertThat(remote1Cluster.getFailedShards(), equalTo(0));
        assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(remote1Cluster.getTook().millis(), equalTo(executionInfo.planningTookTime().millis()));

        EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
        assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertNull(remote2Cluster.getTotalShards());
        assertNull(remote2Cluster.getTook());
    }

    private void assertClusterStatusAndHasNullCounts(EsqlExecutionInfo.Cluster cluster, EsqlExecutionInfo.Cluster.Status status) {
        assertThat(cluster.getStatus(), equalTo(status));
        assertNull(cluster.getTook());
        assertNull(cluster.getTotalShards());
        assertNull(cluster.getSuccessfulShards());
        assertNull(cluster.getSkippedShards());
        assertNull(cluster.getFailedShards());
    }

    private static Map<String, EsField> randomMapping() {
        int size = between(0, 10);
        Map<String, EsField> result = new HashMap<>(size);
        while (result.size() < size) {
            result.put(randomAlphaOfLength(5), EsFieldTests.randomAnyEsField(1));
        }
        return result;
    }
}
