/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.task;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;

import java.util.Locale;
import java.util.regex.Pattern;

public class ResultsWriteAliasCheckTests extends ESTestCase {

    private final String jobId = "job-" + randomAlphaOfLength(8);
    private final String writeAlias = AnomalyDetectorsIndex.resultsWriteAlias(jobId);
    private final String readAlias = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);

    private IndexMetadata.Builder index(String name, String... aliases) {
        IndexMetadata.Builder index = IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0));
        for (String alias : aliases) {
            index.putAlias(AliasMetadata.builder(alias));
        }
        return index;
    }

    private static ClusterState project(IndexMetadata.Builder... indices) {
        return stateOf(ProjectId.DEFAULT, indices);
    }

    private static ClusterState stateOf(ProjectId projectId, IndexMetadata.Builder... indices) {
        ProjectMetadata.Builder project = ProjectMetadata.builder(projectId);
        for (IndexMetadata.Builder index : indices) {
            project.put(index);
        }
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project)).build();
    }

    private void assertViolation(ClusterState state) {
        MockLog.assertThatLogger(
            () -> ResultsWriteAliasCheck.logIfUnexpectedTarget(jobId, state),
            ResultsWriteAliasCheck.class,
            new MockLog.PatternSeenEventExpectation(
                "violation",
                ResultsWriteAliasCheck.class.getCanonicalName(),
                Level.ERROR,
                Pattern.quote("[ml-invariant:results_alias_unexpected_target]") + ".*" + Pattern.quote(jobId) + ".*"
            )
        );
    }

    private void assertSilent(ClusterState state) {
        String loggerName = ResultsWriteAliasCheck.class.getCanonicalName();
        MockLog.assertThatLogger(() -> {
            // The check runs on every job open, including reopen after node churn.
            for (int i = 0; i < 3; i++) {
                ResultsWriteAliasCheck.logIfUnexpectedTarget(jobId, state);
            }
        },
            ResultsWriteAliasCheck.class,
            new MockLog.PatternNotSeenEventExpectation("no error", loggerName, Level.ERROR, ".*"),
            new MockLog.PatternNotSeenEventExpectation("no warning", loggerName, Level.WARN, ".*")
        );
    }

    public void testWriteAliasOnIndexOutsideResultsNamingShouldLogViolation() {
        assertViolation(project(index("my-index-000001", writeAlias, readAlias)));
    }

    public void testWriteAliasOnSeveralResultsIndicesShouldLogViolation() {
        assertViolation(
            project(
                index(".ml-anomalies-shared-000001", writeAlias, readAlias),
                index(".ml-anomalies-shared-000002", writeAlias, readAlias)
            )
        );
    }

    public void testWriteAliasOnResultsIndexAndForeignIndexShouldLogViolation() {
        assertViolation(project(index(".ml-anomalies-shared-000001", writeAlias), index("my-index-000001", writeAlias)));
    }

    public void testWriteAliasNotCreatedYetShouldBeSilent() {
        assertSilent(project(index(".ml-anomalies-shared-000001")));
    }

    public void testNoIndicesShouldBeSilent() {
        assertSilent(project());
    }

    public void testNullClusterStateShouldBeSilentAndNotThrow() {
        assertSilent(null);
    }

    public void testStateWithoutDefaultProjectShouldBeSilentAndNotThrow() {
        assertSilent(stateOf(randomUniqueProjectId(), index("my-index-000001", writeAlias, readAlias)));
    }

    public void testRolloverGenerationsOnReadAliasShouldBeSilent() {
        assertSilent(
            project(
                index(".ml-anomalies-shared-000001", readAlias),
                index(".ml-anomalies-shared-000002", readAlias),
                index(".ml-anomalies-shared-000003", readAlias, writeAlias)
            )
        );
    }

    public void testCustomResultsIndexShouldBeSilent() {
        String name = ".ml-anomalies-custom-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        assertSilent(project(index(name + "-000001", writeAlias, readAlias)));
    }

    public void testLegacyResultsIndexWithoutSixDigitSuffixShouldBeSilent() {
        assertSilent(project(index(randomFrom(".ml-anomalies-shared", ".ml-anomalies-custom-legacy"), writeAlias, readAlias)));
    }

    public void testClosedResultsIndexShouldBeSilent() {
        assertSilent(project(index(".ml-anomalies-shared-000001", writeAlias, readAlias).state(IndexMetadata.State.CLOSE)));
    }

    public void testReindexedIndexAwaitingMappingHealShouldBeSilent() {
        String reindexed = randomFrom(".reindexed-v7-ml-anomalies-shared", ".reindexed-v8-ml-anomalies-shared-000001");
        assertSilent(project(index(reindexed, writeAlias, readAlias)));
    }

    public void testWriteAliasOnReindexedAndResultsIndexWhileHealingShouldBeSilent() {
        assertSilent(
            project(
                index(".reindexed-v7-ml-anomalies-shared", writeAlias, readAlias),
                index(".reindexed-v8-ml-anomalies-shared-000001", writeAlias, readAlias),
                index(".ml-anomalies-shared-000001", writeAlias, readAlias)
            )
        );
    }
}
