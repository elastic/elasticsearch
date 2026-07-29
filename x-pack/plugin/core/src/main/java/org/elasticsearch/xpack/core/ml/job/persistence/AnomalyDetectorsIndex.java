/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Methods for handling index naming related functions
 */
public final class AnomalyDetectorsIndex {

    private static final String RESULTS_MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";
    private static final String RESOURCE_PATH = "/ml/anomalydetection/";
    private static final String WRITE_ALIAS_PREFIX = ".write-";
    public static final int RESULTS_INDEX_MAPPINGS_VERSION = 1;

    private AnomalyDetectorsIndex() {}

    public static String jobResultsIndexPrefix() {
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX;
    }

    public static String jobResultsIndexPattern() {
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + "*";
    }

    /**
     * The name of the alias pointing to the indices where the job's results are stored
     * @param jobId Job Id
     * @return The read alias
     */
    public static String jobResultsAliasedName(String jobId) {
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + jobId;
    }

    /**
     * Extract the job Id from the alias name.
     * If not an results index alias null is returned
     * @param jobResultsAliasedName The alias
     * @return The job Id
     */
    public static Optional<String> jobIdFromAlias(String jobResultsAliasedName) {
        if (jobResultsAliasedName.length() < AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX.length()) {
            return Optional.empty();
        }

        var jobId = jobResultsAliasedName.substring(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX.length());
        if (jobId.startsWith(WRITE_ALIAS_PREFIX)) {
            jobId = jobId.substring(WRITE_ALIAS_PREFIX.length());
        }
        return Optional.of(jobId);
    }

    /**
     * The name of the alias pointing to the write index for a job
     * @param jobId Job Id
     * @return The write alias
     */
    public static String resultsWriteAlias(String jobId) {
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_WRITE_PREFIX + jobId;
    }

    /**
     * The name of the alias pointing to the appropriate index for writing job state
     * @return The write alias name
     */
    public static String jobStateIndexWriteAlias() {
        return AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-write";
    }

    /**
     * Index patterns for ML state indices, including those created by major-version system-index
     * reindex migration (for example {@code .reindexed-v8-ml-state-000001}).
     * <p>
     * Use this array with {@link IndexNameExpressionResolver#concreteIndexNames}. A single
     * comma-separated string is not equivalent: the resolver treats it as one expression.
     */
    public static String[] jobStateIndexPatterns() {
        return new String[] {
            AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "*",
            AnomalyDetectorsIndexFields.REINDEXED_V7_STATE_INDEX_PREFIX + "*",
            AnomalyDetectorsIndexFields.REINDEXED_V8_STATE_INDEX_PREFIX + "*" };
    }

    /**
     * Comma-separated index patterns for ML state indices for REST query strings that split on commas.
     * <p>
     * Do not pass this value as a single argument to Java client methods that accept one index expression
     * (for example {@code prepareSearch(String)}); use {@link #jobStateIndexPatterns()} instead.
     *
     * @return index patterns for ML state indices
     */
    public static String jobStateIndexPattern() {
        return String.join(",", jobStateIndexPatterns());
    }

    /**
     * Creates the .ml-state-000001 index (if necessary)
     * Creates the .ml-state-write alias for the .ml-state-000001 index (if necessary)
     */
    public static void createStateIndexAndAliasIfNecessary(
        Client client,
        ClusterState state,
        IndexNameExpressionResolver resolver,
        TimeValue masterNodeTimeout,
        final ActionListener<Boolean> finalListener
    ) {
        MlIndexAndAlias.createIndexAndAliasIfNecessary(
            client,
            state,
            resolver,
            AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX,
            AnomalyDetectorsIndex.jobStateIndexWriteAlias(),
            masterNodeTimeout,
            // TODO: shard count default preserves the existing behaviour when the
            // parameter was added but it may be that ActiveShardCount.ALL is a
            // better option
            ActiveShardCount.DEFAULT,
            finalListener
        );
    }

    public static void createStateIndexAndAliasIfNecessaryAndWaitForYellow(
        Client client,
        ClusterState state,
        IndexNameExpressionResolver resolver,
        TimeValue masterNodeTimeout,
        final ActionListener<Boolean> finalListener
    ) {
        final ActionListener<Boolean> stateIndexAndAliasCreated = finalListener.delegateFailureAndWrap((delegate, success) -> {
            final ClusterHealthRequest request = new ClusterHealthRequest(
                masterNodeTimeout,
                AnomalyDetectorsIndex.jobStateIndexWriteAlias()
            ).waitForYellowStatus();
            executeAsyncWithOrigin(
                client,
                ML_ORIGIN,
                TransportClusterHealthAction.TYPE,
                request,
                delegate.delegateFailureAndWrap((l, r) -> l.onResponse(r.isTimedOut() == false))
            );
        });

        MlIndexAndAlias.createIndexAndAliasIfNecessary(
            client,
            state,
            resolver,
            AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX,
            AnomalyDetectorsIndex.jobStateIndexWriteAlias(),
            masterNodeTimeout,
            // TODO: shard count default preserves the existing behaviour when the
            // parameter was added but it may be that ActiveShardCount.ALL is a
            // better option
            ActiveShardCount.DEFAULT,
            stateIndexAndAliasCreated
        );
    }

    public static String wrappedResultsMapping() {
        return String.format(Locale.ROOT, """
            {
            "_doc" : %s
            }""", resultsMapping());
    }

    public static String resultsMapping() {
        return TemplateUtils.loadTemplate(
            RESOURCE_PATH + "results_index_mappings.json",
            MlIndexAndAlias.BWC_MAPPINGS_VERSION, // Only needed for BWC with pre-8.10.0 nodes
            RESULTS_MAPPINGS_VERSION_VARIABLE,
            Map.of("xpack.ml.managed.index.version", Integer.toString(RESULTS_INDEX_MAPPINGS_VERSION))
        );
    }
}
