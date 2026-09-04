/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.plugin.SourceOutcomeAccumulator.SourceClusterKey;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ComputeServiceSourceOutcomeTests extends ESTestCase {

    public void testMissingInitialClusterStatusDoesNotSkipRemote() {
        assertFalse(ComputeService.shouldSkipRemoteCluster(null));
        assertFalse(ComputeService.shouldSkipRemoteCluster(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertTrue(ComputeService.shouldSkipRemoteCluster(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertTrue(ComputeService.shouldSkipRemoteCluster(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        assertTrue(ComputeService.shouldSkipRemoteCluster(EsqlExecutionInfo.Cluster.Status.PARTIAL));
    }

    public void testRepeatedProducerAttemptsDoNotMultiplyShardCounts() {
        EsqlExecutionInfo executionInfo = executionInfo();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        SourceClusterKey first = new SourceClusterKey("", List.of("first"));
        SourceClusterKey second = new SourceClusterKey("", List.of("second"));

        outcomes.recordIndexResponse(first, response(5, 5, 0, 0, List.of()));
        outcomes.recordIndexResponse(
            first,
            response(5, 4, 0, 1, List.of(new ShardSearchFailure(new IllegalStateException("failed shard"))))
        );
        outcomes.recordIndexResponse(second, response(3, 3, 0, 0, List.of()));
        outcomes.applyTo(executionInfo);

        EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster("");
        assertThat(cluster.getTotalShards(), equalTo(8));
        assertThat(cluster.getSuccessfulShards(), equalTo(7));
        assertThat(cluster.getFailedShards(), equalTo(1));
        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertThat(cluster.getFailures().size(), equalTo(1));
    }

    public void testRepeatedPartialProducerAttemptsAreOrderIndependent() {
        ComputeResponse successful = response(5, 5, 0, 0, List.of());
        ComputeResponse partial = response(5, 3, 0, 2, List.of(new ShardSearchFailure(new IllegalStateException("failed shards"))));

        EsqlExecutionInfo firstExecution = executionInfo();
        SourceOutcomeAccumulator firstOutcomes = new SourceOutcomeAccumulator();
        SourceClusterKey source = new SourceClusterKey("", List.of("test"));
        firstOutcomes.recordIndexResponse(source, successful);
        firstOutcomes.recordIndexResponse(source, partial);
        firstOutcomes.applyTo(firstExecution);

        EsqlExecutionInfo secondExecution = executionInfo();
        SourceOutcomeAccumulator secondOutcomes = new SourceOutcomeAccumulator();
        secondOutcomes.recordIndexResponse(source, partial);
        secondOutcomes.recordIndexResponse(source, successful);
        secondOutcomes.applyTo(secondExecution);

        assertThat(firstExecution.getCluster("").getTotalShards(), equalTo(secondExecution.getCluster("").getTotalShards()));
        assertThat(firstExecution.getCluster("").getSuccessfulShards(), equalTo(secondExecution.getCluster("").getSuccessfulShards()));
        assertThat(firstExecution.getCluster("").getFailedShards(), equalTo(secondExecution.getCluster("").getFailedShards()));
        assertThat(firstExecution.getCluster("").getSuccessfulShards(), equalTo(3));
        assertThat(firstExecution.getCluster("").getFailedShards(), equalTo(2));
    }

    public void testSuccessfulRepeatedAttemptPreventsAllSourcesFailure() {
        EsqlExecutionInfo executionInfo = executionInfo();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        SourceClusterKey source = new SourceClusterKey("", List.of("test"));
        IllegalStateException failure = new IllegalStateException("failed shards");

        outcomes.recordIndexResponse(source, response(2, 0, 0, 2, List.of(new ShardSearchFailure(failure))));
        outcomes.recordIndexResponse(source, response(2, 2, 0, 0, List.of()));

        outcomes.failIfAllSourcesFailed(executionInfo, List.of());
    }

    public void testSkippedRemoteSourceRemainsSkipped() {
        EsqlExecutionInfo executionInfo = executionInfo();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        SourceClusterKey source = new SourceClusterKey("", List.of("test"));

        outcomes.recordRemoteOutcome(
            source,
            new ClusterComputeHandler.RemoteClusterOutcome.ToleratedFailure(
                EsqlExecutionInfo.Cluster.Status.SKIPPED,
                new IllegalStateException("unavailable"),
                null
            )
        );
        outcomes.applyTo(executionInfo);

        EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster("");
        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        assertThat(cluster.getTotalShards(), equalTo(0));
        assertThat(cluster.getSuccessfulShards(), equalTo(0));
    }

    /**
     * Producers of one fan-in run concurrently, so two of them against the same cluster overlap in wall clock.
     * Their total would report the cluster as taking longer than the query that waited on it.
     */
    public void testRemoteTimingTakesTheLongestProducerNotTheirTotal() {
        EsqlExecutionInfo executionInfo = executionInfo("remote");
        executionInfo.queryProfile().planning().start();
        executionInfo.queryProfile().planning().stop();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        SourceClusterKey source = new SourceClusterKey("remote", List.of("test"));

        outcomes.recordRemoteOutcome(
            source,
            new ClusterComputeHandler.RemoteClusterOutcome.Success(response(TimeValue.timeValueMillis(5), 1, 1, 0, 0, List.of()))
        );
        outcomes.recordRemoteOutcome(
            source,
            new ClusterComputeHandler.RemoteClusterOutcome.Success(response(TimeValue.timeValueMillis(7), 1, 1, 0, 0, List.of()))
        );
        outcomes.applyTo(executionInfo);

        long expectedNanos = executionInfo.queryProfile().planning().timeTook().nanos() + TimeValue.timeValueMillis(7).nanos();
        assertThat(executionInfo.getCluster("remote").getTook().nanos(), equalTo(expectedNanos));
    }

    /**
     * Separate executes against one cluster (an INLINE STATS or IN-subquery subplan, then the main plan) are
     * sequential, so their times add. Mirrors {@code ClusterComputeHandler#updateExecutionInfo}.
     */
    public void testRemoteTimingAddsToTookLeftByAnEarlierExecute() {
        EsqlExecutionInfo executionInfo = executionInfo("remote");
        executionInfo.queryProfile().planning().start();
        executionInfo.queryProfile().planning().stop();
        executionInfo.swapCluster(
            "remote",
            (key, cluster) -> new EsqlExecutionInfo.Cluster.Builder(cluster).setTook(TimeValue.timeValueMillis(4)).build()
        );
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();

        outcomes.recordRemoteOutcome(
            new SourceClusterKey("remote", List.of("test")),
            new ClusterComputeHandler.RemoteClusterOutcome.Success(response(TimeValue.timeValueMillis(7), 1, 1, 0, 0, List.of()))
        );
        outcomes.applyTo(executionInfo);

        assertThat(executionInfo.getCluster("remote").getTook().nanos(), equalTo(TimeValue.timeValueMillis(11).nanos()));
    }

    public void testFailureBeforeShardResponseFailsAnEmptyQuery() {
        EsqlExecutionInfo executionInfo = executionInfo();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        IllegalStateException failure = new IllegalStateException("failed before response");

        outcomes.recordIndexFailure(new SourceClusterKey("", List.of("test")), failure);

        Exception thrown = expectThrows(Exception.class, () -> outcomes.failIfAllSourcesFailed(executionInfo, List.of()));
        assertSame(failure, thrown);
    }

    public void testRemoteFailureBeforeShardResponseFailsAnEmptyQuery() {
        EsqlExecutionInfo executionInfo = executionInfo("remote");
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        IllegalStateException failure = new IllegalStateException("failed before response");

        outcomes.recordRemoteOutcome(
            new SourceClusterKey("remote", List.of("test")),
            new ClusterComputeHandler.RemoteClusterOutcome.ToleratedFailure(EsqlExecutionInfo.Cluster.Status.SKIPPED, failure, null)
        );

        Exception thrown = expectThrows(Exception.class, () -> outcomes.failIfAllSourcesFailed(executionInfo, List.of()));
        assertSame(failure, thrown);
    }

    public void testPipelineBreakerOutputDoesNotMaskAllSourceFailures() {
        EsqlExecutionInfo executionInfo = executionInfo();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        IllegalStateException failure = new IllegalStateException("failed shard");
        outcomes.recordIndexResponse(
            new SourceClusterKey("", List.of("test")),
            response(1, 0, 0, 1, List.of(new ShardSearchFailure(failure)))
        );

        Exception thrown = expectThrows(Exception.class, () -> outcomes.failIfAllSourcesFailed(executionInfo, List.of(new Page(1))));
        assertSame(failure, thrown);
    }

    public void testSuccessfulSourceAllowsOtherSourceFailures() {
        EsqlExecutionInfo executionInfo = executionInfo();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        outcomes.recordIndexFailure(new SourceClusterKey("", List.of("failed")), new IllegalStateException("failed before response"));
        outcomes.recordIndexResponse(new SourceClusterKey("", List.of("successful")), response(1, 1, 0, 0, List.of()));

        outcomes.failIfAllSourcesFailed(executionInfo, List.of());
    }

    public void testCompletedEmptyIndexAllowsOtherSourceFailures() {
        EsqlExecutionInfo executionInfo = executionInfo();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        outcomes.recordIndexResponse(
            new SourceClusterKey("", List.of("empty")),
            randomBoolean() ? response(0, 0, 0, 0, List.of()) : response(3, 0, 3, 0, List.of())
        );
        outcomes.recordExternalFailure(new IllegalStateException("failed external source"));

        outcomes.failIfAllSourcesFailed(executionInfo, List.of());
    }

    public void testSuccessfulExternalSourceAllowsOtherExternalSourceFailures() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        outcomes.recordExternalFailure(new IllegalStateException("failed external source"));
        outcomes.recordExternalSuccess();

        outcomes.failIfAllSourcesFailed(executionInfo, List.of());
    }

    public void testExternalFailureFailsWithoutSuccessfulSource() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        IllegalStateException failure = new IllegalStateException("failed external source");
        outcomes.recordExternalFailure(failure);

        Exception thrown = expectThrows(Exception.class, () -> outcomes.failIfAllSourcesFailed(executionInfo, List.of()));
        assertSame(failure, thrown);
    }

    public void testSubplanApplyToLeavesClusterRunning() {
        EsqlExecutionInfo executionInfo = executionInfo("remote");
        executionInfo.startSubPlans(randomBoolean());
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();
        SourceClusterKey source = new SourceClusterKey("remote", List.of("test"));

        outcomes.recordIndexResponse(source, response(4, 4, 0, 0, List.of()));
        outcomes.applyTo(executionInfo);

        EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster("remote");
        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertThat(cluster.getTotalShards(), equalTo(4));
        assertThat(cluster.getSuccessfulShards(), equalTo(4));
        assertFalse(ComputeService.shouldSkipRemoteCluster(cluster.getStatus()));
    }

    public void testSubplanApplyToLeavesUnusedClusterRunning() {
        EsqlExecutionInfo executionInfo = executionInfo("remote");
        executionInfo.startSubPlans(randomBoolean());
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();

        outcomes.applyTo(executionInfo);

        EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster("remote");
        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertNull(cluster.getTotalShards());
        assertFalse(ComputeService.shouldSkipRemoteCluster(cluster.getStatus()));
    }

    public void testLeftoverRunningClusterIsSuccessfulWithoutInventingShards() {
        EsqlExecutionInfo executionInfo = executionInfo("remote");
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();

        outcomes.applyTo(executionInfo);

        EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster("remote");
        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertNull(cluster.getTotalShards());
        assertNull(cluster.getSuccessfulShards());
    }

    public void testLeftoverRunningClusterIsPartialWhenStopped() {
        EsqlExecutionInfo executionInfo = executionInfo("remote");
        executionInfo.markAsStopped();
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();

        outcomes.applyTo(executionInfo);

        EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster("remote");
        assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertNull(cluster.getTotalShards());
        assertNull(cluster.getSuccessfulShards());
    }

    public void testExternalSuccessIsRecordedWithoutClusterMetadata() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        SourceOutcomeAccumulator outcomes = new SourceOutcomeAccumulator();

        outcomes.recordExternalSuccess();
        outcomes.applyTo(executionInfo);

        assertTrue(outcomes.externalSourceSucceeded());
        assertTrue(executionInfo.clusterAliases().isEmpty());
    }

    private static EsqlExecutionInfo executionInfo() {
        return executionInfo("");
    }

    private static EsqlExecutionInfo executionInfo(String clusterAlias) {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(alias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.ALWAYS);
        executionInfo.swapCluster(
            clusterAlias,
            (key, value) -> new EsqlExecutionInfo.Cluster(
                clusterAlias,
                clusterAlias,
                "test",
                false,
                EsqlExecutionInfo.Cluster.Status.RUNNING,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        return executionInfo;
    }

    private static ComputeResponse response(
        int totalShards,
        int successfulShards,
        int skippedShards,
        int failedShards,
        List<ShardSearchFailure> failures
    ) {
        return response(null, totalShards, successfulShards, skippedShards, failedShards, failures);
    }

    private static ComputeResponse response(
        TimeValue took,
        int totalShards,
        int successfulShards,
        int skippedShards,
        int failedShards,
        List<ShardSearchFailure> failures
    ) {
        return new ComputeResponse(DriverCompletionInfo.EMPTY, took, totalShards, successfulShards, skippedShards, failedShards, failures);
    }
}
