/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class IndexResolverTests extends ESTestCase {

    /**
     * Test the IndexResolver#updateExecutionInfoWithFieldCapsResults to ensure that:
     * For clusters that have field-caps results (an entry in the clusterAndResolvedIndices map passed in)
     *  - it overrides the existing index expression with the resolved index expression (all wildcards removed)
     *  - it does not update any other fields in {@link EsqlExecutionInfo.Cluster}
     * For clusters that do not have any field-caps results (e.g., no matching index on a remote cluster):
     *  - it leaves the index expression unchanged (any existing wildcards still present)
     *  - it updates status to SUCCESSFUL, and sets all shard counts and took time to 0
     * For clusters that have field-caps errors that indicate no remote connection to the cluster:
     *  - for skip_unavailable=true, it sets status to SKIPPED, leaves shard counts as null
     *  - (future) for skip_unavailable=false, it throws an Exception // TODO: write test in follow on ticket
     */
    public void testUpdateExecutionInfoWithFieldCapsResultsNoFieldCapsFailures() {
        final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        final String remote1Alias = "remote1";
        final String remote2Alias = "remote2";
        List<FieldCapabilitiesFailure> failures = new ArrayList<>();

        // three clusters, all have field-caps results
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo();
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*"));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*"));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*"));

            Map<String, StringBuilder> clusterAndResolvedIndices = new HashMap<>();
            clusterAndResolvedIndices.put(localClusterAlias, new StringBuilder("logs-a,logs-b"));
            clusterAndResolvedIndices.put(remote1Alias, new StringBuilder("remote1:logs-a"));
            clusterAndResolvedIndices.put(remote2Alias, new StringBuilder("remote2:mylogs1,remote2:mylogs2,remote2:logs-a,remote2:logs-b"));

            IndexResolver.updateExecutionInfoWithFieldCapsResults(executionInfo, clusterAndResolvedIndices, failures);

            assertThat(executionInfo.getClusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.getOverallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-a,logs-b"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(localCluster.getTook());
            assertNull(localCluster.getTotalShards());
            assertNull(localCluster.getSuccessfulShards());
            assertNull(localCluster.getSkippedShards());
            assertNull(localCluster.getFailedShards());

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("remote1:logs-a"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(remote1Cluster.getTook());
            assertNull(remote1Cluster.getTotalShards());
            assertNull(remote1Cluster.getSuccessfulShards());
            assertNull(remote1Cluster.getSkippedShards());
            assertNull(remote1Cluster.getFailedShards());

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("remote2:mylogs1,remote2:mylogs2,remote2:logs-a,remote2:logs-b"));
            assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(remote2Cluster.getTook());
            assertNull(remote2Cluster.getTotalShards());
            assertNull(remote2Cluster.getSuccessfulShards());
            assertNull(remote2Cluster.getSkippedShards());
            assertNull(remote2Cluster.getFailedShards());
        }

        // two clusters, both remote
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo();
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*"));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*"));

            Map<String, StringBuilder> clusterAndResolvedIndices = new HashMap<>();
            clusterAndResolvedIndices.put(remote1Alias, new StringBuilder("remote1:logs-a,remote1:logs-b"));
            clusterAndResolvedIndices.put(remote2Alias, new StringBuilder("remote2:mylogs1,remote2:mylogs2"));

            IndexResolver.updateExecutionInfoWithFieldCapsResults(executionInfo, clusterAndResolvedIndices, failures);

            assertThat(executionInfo.getClusterAliases(), equalTo(Set.of(remote1Alias, remote2Alias)));
            assertNull(executionInfo.getOverallTook());

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("remote1:logs-a,remote1:logs-b"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(remote1Cluster.getTook());
            assertNull(remote1Cluster.getTotalShards());
            assertNull(remote1Cluster.getSuccessfulShards());
            assertNull(remote1Cluster.getSkippedShards());
            assertNull(remote1Cluster.getFailedShards());

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("remote2:mylogs1,remote2:mylogs2"));
            assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(remote2Cluster.getTook());
            assertNull(remote2Cluster.getTotalShards());
            assertNull(remote2Cluster.getSuccessfulShards());
            assertNull(remote2Cluster.getSkippedShards());
            assertNull(remote2Cluster.getFailedShards());
        }

        // two clusters, only local had matching indices from field-caps, so remote1 should be marked as done (SUCCESSFUL)
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo();
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*"));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*"));

            Map<String, StringBuilder> clusterAndResolvedIndices = new HashMap<>();
            clusterAndResolvedIndices.put(localClusterAlias, new StringBuilder("logs-a,logs-b"));

            IndexResolver.updateExecutionInfoWithFieldCapsResults(executionInfo, clusterAndResolvedIndices, failures);

            assertThat(executionInfo.getClusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias)));
            assertNull(executionInfo.getOverallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-a,logs-b"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(localCluster.getTook());
            assertNull(localCluster.getTotalShards());
            assertNull(localCluster.getSuccessfulShards());
            assertNull(localCluster.getSkippedShards());
            assertNull(localCluster.getFailedShards());

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remote1Cluster.getTook().millis(), equalTo(0L));
            assertThat(remote1Cluster.getTotalShards(), equalTo(0));
            assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
            assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
            assertThat(remote1Cluster.getFailedShards(), equalTo(0));
        }
    }

    /**
     * Test updateExecutionInfoWithFieldCapsResults to ensure that:
     * For clusters that have field-caps errors:
     *  - for skip_unavailable=true, it sets status to SKIPPED, leaves shard counts as null
     *  - (future) for skip_unavailable=false, throws an Exception // TODO: write test in follow on ticket
     */
    public void testUpdateExecutionInfoWithFieldCapsResultsWithFieldCapsFailures() {
        final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        final String remote1Alias = "remote1";
        final String remote2Alias = "remote2";

        // three clusters, all have field-caps results, both remote clusters with failures, both are skip_unavailable=true
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo();
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*"));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", true));

            Map<String, StringBuilder> clusterAndResolvedIndices = new HashMap<>();
            clusterAndResolvedIndices.put(localClusterAlias, new StringBuilder("logs-a,logs-b"));
            clusterAndResolvedIndices.put(remote1Alias, new StringBuilder("remote1:*"));
            clusterAndResolvedIndices.put(remote2Alias, new StringBuilder("remote2:mylogs1,remote2:mylogs2,remote2:logs*"));

            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSuchRemoteClusterException("remote2")));
            failures.add(
                new FieldCapabilitiesFailure(
                    new String[] { "remote1:foo", "remote1:bar" },
                    new IllegalStateException("Unable to open any connections")
                )
            );

            IndexResolver.updateExecutionInfoWithFieldCapsResults(executionInfo, clusterAndResolvedIndices, failures);

            assertThat(executionInfo.getClusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.getOverallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-a,logs-b"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertNull(localCluster.getTook());
            assertNull(localCluster.getTotalShards());
            assertNull(localCluster.getSuccessfulShards());
            assertNull(localCluster.getSkippedShards());
            assertNull(localCluster.getFailedShards());

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("remote1:*"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertNull(remote1Cluster.getTook());
            assertNull(remote1Cluster.getTotalShards());
            assertNull(remote1Cluster.getSuccessfulShards());
            assertNull(remote1Cluster.getSkippedShards());
            assertNull(remote1Cluster.getFailedShards());

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("remote2:mylogs1,remote2:mylogs2,remote2:logs*"));
            assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertNull(remote2Cluster.getTook());
            assertNull(remote2Cluster.getTotalShards());
            assertNull(remote2Cluster.getSuccessfulShards());
            assertNull(remote2Cluster.getSkippedShards());
            assertNull(remote2Cluster.getFailedShards());
        }

        // one remote cluster, with failures, having skip_unavailable=true
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo();
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));

            Map<String, StringBuilder> clusterAndResolvedIndices = new HashMap<>();
            clusterAndResolvedIndices.put(remote1Alias, new StringBuilder("remote1:*"));

            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(
                new FieldCapabilitiesFailure(new String[] { "remote1:foo", "remote1:bar" }, new NoSeedNodeLeftException("no seed node"))
            );

            IndexResolver.updateExecutionInfoWithFieldCapsResults(executionInfo, clusterAndResolvedIndices, failures);

            assertThat(executionInfo.getClusterAliases(), equalTo(Set.of(remote1Alias)));
            assertNull(executionInfo.getOverallTook());

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("remote1:*"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertNull(remote1Cluster.getTook());
            assertNull(remote1Cluster.getTotalShards());
            assertNull(remote1Cluster.getSuccessfulShards());
            assertNull(remote1Cluster.getSkippedShards());
            assertNull(remote1Cluster.getFailedShards());
        }

        // TODO this test will be removed (or changed to expect an error) when we add skip_unavailable=false handling to ES|QL
        // one remote cluster, with failures, having skip_unavailable=false
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo();
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", false));

            Map<String, StringBuilder> clusterAndResolvedIndices = new HashMap<>();
            clusterAndResolvedIndices.put(remote1Alias, new StringBuilder("remote1:*"));

            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(
                new FieldCapabilitiesFailure(new String[] { "remote1:foo", "remote1:bar" }, new NoSeedNodeLeftException("no seed node"))
            );

            IndexResolver.updateExecutionInfoWithFieldCapsResults(executionInfo, clusterAndResolvedIndices, failures);

            assertThat(executionInfo.getClusterAliases(), equalTo(Set.of(remote1Alias)));
            assertNull(executionInfo.getOverallTook());

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("remote1:*"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertNull(remote1Cluster.getTook());
            assertNull(remote1Cluster.getTotalShards());
            assertNull(remote1Cluster.getSuccessfulShards());
            assertNull(remote1Cluster.getSkippedShards());
            assertNull(remote1Cluster.getFailedShards());
        }
    }
}
