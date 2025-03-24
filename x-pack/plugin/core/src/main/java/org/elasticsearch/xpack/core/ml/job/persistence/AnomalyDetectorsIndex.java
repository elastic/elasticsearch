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

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Methods for handling index naming related functions
 */
public final class AnomalyDetectorsIndex {

    private static final String RESULTS_MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";
    private static final String RESOURCE_PATH = "/ml/anomalydetection/";
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
    public static String jobIdFromAlias(String jobResultsAliasedName) {
        if (jobResultsAliasedName.length() < AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX.length()) {
            return null;
        }
        return jobResultsAliasedName.substring(AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX.length());
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
     * The name pattern to capture all .ml-state prefixed indices
     * @return The .ml-state index pattern
     */
    public static String jobStateIndexPattern() {
        return AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "*";
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
