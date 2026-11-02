/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MlAnomaliesIndexUpdate;
import org.elasticsearch.xpack.ml.utils.MlInvariantLogger;

import java.util.List;

/** Logs an invariant event when a job's results write alias points at an index outside the results index naming. */
final class ResultsWriteAliasCheck {

    private static final Logger logger = LogManager.getLogger(ResultsWriteAliasCheck.class);
    private static final MlInvariantLogger invariantLogger = new MlInvariantLogger();

    private ResultsWriteAliasCheck() {}

    /** Never throws: a null state or one without the default project is skipped so the check cannot fail a job open. */
    static void logIfUnexpectedTarget(String jobId, @Nullable ClusterState state) {
        if (state == null || state.metadata().hasProject(Metadata.DEFAULT_PROJECT_ID) == false) {
            return;
        }
        ProjectMetadata project = state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
        List<String> targets = project.aliasedIndices(AnomalyDetectorsIndex.resultsWriteAlias(jobId))
            .stream()
            .map(Index::getName)
            .sorted()
            .toList();
        // A missing alias is a job still being created. The reindexed-index heal step later moves the alias off
        // reindexed indices in one atomic update, so a job opened before it runs is not a violation.
        if (targets.isEmpty() || targets.stream().anyMatch(MlAnomaliesIndexUpdate::isReindexedAnomaliesIndex)) {
            return;
        }
        // Legacy results indices lack a six-digit suffix until they are rolled over, so only the prefix is checked.
        if (targets.size() == 1 && targets.get(0).startsWith(AnomalyDetectorsIndex.jobResultsIndexPrefix())) {
            return;
        }
        invariantLogger.violated(
            logger,
            MlInvariantLogger.Kind.RESULTS_ALIAS_UNEXPECTED_TARGET,
            jobId,
            Strings.format("results write alias for job [%s] resolves to %s, expected one results index", jobId, targets)
        );
    }
}
