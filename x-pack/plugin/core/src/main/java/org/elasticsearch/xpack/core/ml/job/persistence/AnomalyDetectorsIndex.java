/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.utils.MlIndexAndAlias;
import org.elasticsearch.xpack.core.template.TemplateUtils;

/**
 * Methods for handling index naming related functions
 */
public final class AnomalyDetectorsIndex {

    private static final String RESULTS_MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";
    private static final String RESOURCE_PATH = "/org/elasticsearch/xpack/core/ml/anomalydetection/";

    private AnomalyDetectorsIndex() {}

    public static String jobResultsIndexPrefix() {
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX;
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
     * The name of the alias pointing to the write index for a job
     * @param jobId Job Id
     * @return The write alias
     */
    public static String resultsWriteAlias(String jobId) {
        // ".write" rather than simply "write" to avoid the danger of clashing
        // with the read alias of a job whose name begins with "write-"
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + ".write-" + jobId;
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
    public static void createStateIndexAndAliasIfNecessary(Client client, ClusterState state,
                                                           IndexNameExpressionResolver resolver,
                                                           TimeValue masterNodeTimeout,
                                                           final ActionListener<Boolean> finalListener) {
        MlIndexAndAlias.createIndexAndAliasIfNecessary(
            client,
            state,
            resolver,
            AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX,
            AnomalyDetectorsIndex.jobStateIndexWriteAlias(),
            masterNodeTimeout,
            finalListener);
    }

    public static String wrappedResultsMapping() {
        return "{\n\"_doc\" : " + resultsMapping() + "\n}";
    }

    public static String resultsMapping() {
        return TemplateUtils.loadTemplate(RESOURCE_PATH + "results_index_mappings.json",
            Version.CURRENT.toString(), RESULTS_MAPPINGS_VERSION_VARIABLE);
    }
}
