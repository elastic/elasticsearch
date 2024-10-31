/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.type.EsFieldTests;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EsqlSessionTests extends ESTestCase {

    public void testCreateIndexExpressionFromAvailableClusters() {
        final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        final String remote1Alias = "remote1";
        final String remote2Alias = "remote2";

        // no clusters marked as skipped
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", true));

            String indexExpr = EsqlSession.createIndexExpressionFromAvailableClusters(executionInfo);
            List<String> list = Arrays.stream(Strings.splitStringByCommaToArray(indexExpr)).toList();
            assertThat(list.size(), equalTo(5));
            assertThat(
                new HashSet<>(list),
                equalTo(Strings.commaDelimitedListToSet("logs*,remote1:*,remote2:mylogs1,remote2:mylogs2,remote2:logs*"))
            );
        }

        // one cluster marked as skipped, so not present in revised index expression
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*,foo", true));
            executionInfo.swapCluster(
                remote2Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    remote2Alias,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            String indexExpr = EsqlSession.createIndexExpressionFromAvailableClusters(executionInfo);
            List<String> list = Arrays.stream(Strings.splitStringByCommaToArray(indexExpr)).toList();
            assertThat(list.size(), equalTo(3));
            assertThat(new HashSet<>(list), equalTo(Strings.commaDelimitedListToSet("logs*,remote1:*,remote1:foo")));
        }

        // two clusters marked as skipped, so only local cluster present in revised index expression
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(
                remote1Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*,foo", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
            );
            executionInfo.swapCluster(
                remote2Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    remote2Alias,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            assertThat(EsqlSession.createIndexExpressionFromAvailableClusters(executionInfo), equalTo("logs*"));
        }

        // only remotes present and all marked as skipped, so in revised index expression should be empty string
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(
                remote1Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*,foo", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
            );
            executionInfo.swapCluster(
                remote2Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    remote2Alias,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            assertThat(EsqlSession.createIndexExpressionFromAvailableClusters(executionInfo), equalTo(""));
        }
    }

    public void testUpdateExecutionInfoWithUnavailableClusters() {
        final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        final String remote1Alias = "remote1";
        final String remote2Alias = "remote2";

        // skip_unavailable=true clusters are unavailable, both marked as SKIPPED
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", true));

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            var unvailableClusters = Map.of(remote1Alias, failure, remote2Alias, failure);
            EsqlSession.updateExecutionInfoWithUnavailableClusters(executionInfo, unvailableClusters);

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);
        }

        // skip_unavailable=false cluster is unavailable, throws Exception
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            RemoteTransportException e = expectThrows(
                RemoteTransportException.class,
                () -> EsqlSession.updateExecutionInfoWithUnavailableClusters(executionInfo, Map.of(remote2Alias, failure))
            );
            assertThat(e.status().getStatus(), equalTo(500));
            assertThat(
                e.getDetailedMessage(),
                containsString("Remote cluster [remote2] (with setting skip_unavailable=false) is not available")
            );
            assertThat(e.getCause().getMessage(), containsString("unable to connect"));
        }

        // all clusters available, no Clusters in ExecutionInfo should be modified
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsqlSession.updateExecutionInfoWithUnavailableClusters(executionInfo, Map.of());

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }
    }

    public void testUpdateExecutionInfoWithClustersWithNoMatchingIndices() {
        final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        final String remote1Alias = "remote1";
        final String remote2Alias = "remote2";
        // all clusters present in EsIndex, so no updates to EsqlExecutionInfo should happen
        {
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
            IndexResolution indexResolution = IndexResolution.valid(esIndex, Map.of());

            EsqlSession.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }

        // remote1 is missing from EsIndex info, so it should be updated and marked as SKIPPED with 0 total shards, 0 took time, etc.
        {
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
            IndexResolution indexResolution = IndexResolution.valid(esIndex, Map.of());

            EsqlSession.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

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
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }

        // all remotes are missing from EsIndex info, so they should be updated and marked as SKIPPED with 0 total shards, 0 took time, etc.
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of("logs-a", IndexMode.STANDARD)
            );
            // remote1 is unavailable
            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            IndexResolution indexResolution = IndexResolution.valid(esIndex, Map.of(remote1Alias, failure));

            EsqlSession.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            // since remote1 is in the unavailable Map (passed to IndexResolution.valid), it's status will not be changed
            // by updateExecutionInfoWithClustersWithNoMatchingIndices (it is handled in updateExecutionInfoWithUnavailableClusters)
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remote2Cluster.getTook().millis(), equalTo(0L));
            assertThat(remote2Cluster.getTotalShards(), equalTo(0));
            assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
            assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
            assertThat(remote2Cluster.getFailedShards(), equalTo(0));
        }

        // all remotes are missing from EsIndex info. Since one is configured with skip_unavailable=false,
        // an exception should be thrown
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*"));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of("logs-a", IndexMode.STANDARD)
            );

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            IndexResolution indexResolution = IndexResolution.valid(esIndex, Map.of(remote1Alias, failure));
            EsqlSession.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);
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

    private void assertClusterStatusAndShardCounts(EsqlExecutionInfo.Cluster cluster, EsqlExecutionInfo.Cluster.Status status) {
        assertThat(cluster.getStatus(), equalTo(status));
        assertNull(cluster.getTook());
        if (status == EsqlExecutionInfo.Cluster.Status.RUNNING) {
            assertNull(cluster.getTotalShards());
            assertNull(cluster.getSuccessfulShards());
            assertNull(cluster.getSkippedShards());
            assertNull(cluster.getFailedShards());
        } else if (status == EsqlExecutionInfo.Cluster.Status.SKIPPED) {
            assertThat(cluster.getTotalShards(), equalTo(0));
            assertThat(cluster.getSuccessfulShards(), equalTo(0));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(0));
        } else {
            fail("Unexpected status: " + status);
        }
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
