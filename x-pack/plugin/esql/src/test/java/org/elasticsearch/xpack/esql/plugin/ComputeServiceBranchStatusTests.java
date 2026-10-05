/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;

import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Merge-branch status transitions for a shared {@link EsqlExecutionInfo}. Covers the two CCS orders that used to leave {@code SKIPPED}
 * with nonzero successful shard counts.
 */
public class ComputeServiceBranchStatusTests extends ESTestCase {
    private static final String REMOTE = "remote1";

    public void testRunningSuccessBecomesSuccessful() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.RUNNING, false, true, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
    }

    public void testRunningFailureWithResultsBecomesPartial() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.RUNNING, true, true, EsqlExecutionInfo.Cluster.Status.PARTIAL);
    }

    public void testRunningFailureWithoutResultsBecomesSkipped() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.RUNNING, true, false, EsqlExecutionInfo.Cluster.Status.SKIPPED);
    }

    public void testSuccessfulThenSuccessStaysSuccessful() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, false, true, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
    }

    public void testSuccessfulThenFailureBecomesPartial() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, true, false, EsqlExecutionInfo.Cluster.Status.PARTIAL);
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, true, true, EsqlExecutionInfo.Cluster.Status.PARTIAL);
    }

    public void testSkippedThenLaterReportBecomesPartial() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.SKIPPED, false, true, EsqlExecutionInfo.Cluster.Status.PARTIAL);
    }

    public void testSkippedThenFailureWithResultsBecomesPartial() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.SKIPPED, true, true, EsqlExecutionInfo.Cluster.Status.PARTIAL);
    }

    public void testSkippedThenAnotherEmptyFailureStaysSkipped() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.SKIPPED, true, false, EsqlExecutionInfo.Cluster.Status.SKIPPED);
    }

    public void testPartialIsNeverDemoted() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.PARTIAL, false, true, EsqlExecutionInfo.Cluster.Status.PARTIAL);
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.PARTIAL, true, false, EsqlExecutionInfo.Cluster.Status.PARTIAL);
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.PARTIAL, true, true, EsqlExecutionInfo.Cluster.Status.PARTIAL);
    }

    public void testFailedIsNeverDemoted() {
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.FAILED, false, true, EsqlExecutionInfo.Cluster.Status.FAILED);
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.FAILED, true, false, EsqlExecutionInfo.Cluster.Status.FAILED);
        assertStatusAfterBranch(EsqlExecutionInfo.Cluster.Status.FAILED, true, true, EsqlExecutionInfo.Cluster.Status.FAILED);
    }

    /**
     * IN-subquery can add shard counts without changing status. An empty later failure must not mark that cluster SKIPPED.
     */
    public void testRunningWithPriorShardsBecomesPartialOnEmptyFailure() {
        EsqlExecutionInfo.Cluster existing = cluster(EsqlExecutionInfo.Cluster.Status.RUNNING, 2, 2, 0, List.of());
        var builder = new EsqlExecutionInfo.Cluster.Builder(existing);
        ComputeService.applyClusterStatusAfterBranch(builder, existing, true, false);
        assertThat(builder.build().getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
    }

    /**
     * Success then failure: a later CCS branch fails before returning anything. Status must become PARTIAL and keep the earlier successful
     * shard counts.
     */
    public void testSuccessThenEmptyFailureKeepsShardsAndBecomesPartial() {
        EsqlExecutionInfo executionInfo = executionInfoWith(cluster(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 3, 3, 0, List.of()));
        ComputeService.markClusterAfterRuntimeBranchFailure(executionInfo, REMOTE, false, new IllegalStateException("remote failed"));
        EsqlExecutionInfo.Cluster updated = executionInfo.getCluster(REMOTE);
        assertThat(updated.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertThat(updated.getTotalShards(), equalTo(3));
        assertThat(updated.getSuccessfulShards(), equalTo(3));
        assertThat(updated.getFailedShards(), equalTo(0));
        assertThat(updated.getFailures(), not(empty()));
        assertThat(updated.getFailures().get(0).reason(), containsString("remote failed"));
    }

    /**
     * Skip then success is applied via {@link ComputeService#applyClusterStatusAfterBranch}: an earlier runtime
     * SKIPPED plus a later report must become PARTIAL, not stay SKIPPED and not become SUCCESSFUL.
     */
    public void testSkippedThenSuccessDoesNotBecomeSuccessful() {
        EsqlExecutionInfo.Cluster existing = cluster(EsqlExecutionInfo.Cluster.Status.SKIPPED, 0, 0, 0, List.of());
        var builder = new EsqlExecutionInfo.Cluster.Builder(existing);
        ComputeService.applyShardCounts(builder, existing, 2, 2, 0, 0, true);
        ComputeService.applyClusterStatusAfterBranch(builder, existing, false, true);
        EsqlExecutionInfo.Cluster updated = builder.build();
        assertThat(updated.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertThat(updated.getTotalShards(), equalTo(2));
        assertThat(updated.getSuccessfulShards(), equalTo(2));
    }

    public void testFirstBranchEmptyFailureMarksSkippedAndRecordsFailure() {
        EsqlExecutionInfo executionInfo = executionInfoWith(cluster(EsqlExecutionInfo.Cluster.Status.RUNNING, null, null, null, List.of()));
        ComputeService.markClusterAfterRuntimeBranchFailure(executionInfo, REMOTE, false, new IllegalStateException("unavailable"));
        EsqlExecutionInfo.Cluster updated = executionInfo.getCluster(REMOTE);
        assertThat(updated.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        assertThat(updated.getTotalShards(), equalTo(0));
        assertThat(updated.getSuccessfulShards(), equalTo(0));
        assertThat(updated.getFailures(), not(empty()));
        assertThat(updated.getFailures().get(0).reason(), containsString("unavailable"));
    }

    public void testThisBranchReceivedResultsMarksPartial() {
        EsqlExecutionInfo executionInfo = executionInfoWith(cluster(EsqlExecutionInfo.Cluster.Status.RUNNING, null, null, null, List.of()));
        ComputeService.markClusterAfterRuntimeBranchFailure(executionInfo, REMOTE, true, new IllegalStateException("partial fail"));
        assertThat(executionInfo.getCluster(REMOTE).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
    }

    private static void assertStatusAfterBranch(
        EsqlExecutionInfo.Cluster.Status existingStatus,
        boolean failed,
        boolean receivedResults,
        EsqlExecutionInfo.Cluster.Status expected
    ) {
        EsqlExecutionInfo.Cluster existing = cluster(existingStatus, 0, 0, 0, List.of());
        var builder = new EsqlExecutionInfo.Cluster.Builder(existing);
        ComputeService.applyClusterStatusAfterBranch(builder, existing, failed, receivedResults);
        assertThat(builder.build().getStatus(), equalTo(expected));
    }

    private static EsqlExecutionInfo executionInfoWith(EsqlExecutionInfo.Cluster cluster) {
        EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(true);
        executionInfo.swapCluster(REMOTE, (k, v) -> cluster);
        return executionInfo;
    }

    private static EsqlExecutionInfo.Cluster cluster(
        EsqlExecutionInfo.Cluster.Status status,
        Integer totalShards,
        Integer successfulShards,
        Integer failedShards,
        List<ShardSearchFailure> failures
    ) {
        return new EsqlExecutionInfo.Cluster(
            REMOTE,
            REMOTE,
            "logs-*",
            true,
            status,
            totalShards,
            successfulShards,
            0,
            failedShards,
            failures,
            TimeValue.timeValueMillis(5)
        );
    }
}
