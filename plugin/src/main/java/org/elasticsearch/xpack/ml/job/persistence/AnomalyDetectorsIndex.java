/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;

/**
 * Methods for handling index naming related functions
 */
public final class AnomalyDetectorsIndex {
    private static final String RESULTS_INDEX_PREFIX = ".ml-anomalies-";
    private static final String STATE_INDEX_NAME = ".ml-state";

    private AnomalyDetectorsIndex() {
    }

    /**
     * The name of the default index where the job's results are stored
     * @param jobId Job Id
     * @return The index name
     */
    public static String jobResultsIndexName(String jobId) {
        return RESULTS_INDEX_PREFIX + jobId;
    }

    /**
     * The default index pattern for rollover index results
     * @param jobId Job Id
     * @return The index name
     */
    public static String getCurrentResultsIndex(ClusterState state, String jobId) {
        MlMetadata meta = state.getMetaData().custom(MlMetadata.TYPE);
        return RESULTS_INDEX_PREFIX + meta.getJobs().get(jobId).getIndexName();
    }

    /**
     * The name of the default index where a job's state is stored
     * @return The index name
     */
    public static String jobStateIndexName() {
        return STATE_INDEX_NAME;
    }
}
