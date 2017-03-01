/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.ml.MlMetadata;

/**
 * Methods for handling index naming related functions
 */
public final class AnomalyDetectorsIndex {
    /**
     * Where to store the ml info in Elasticsearch - must match what's
     * expected by kibana/engineAPI/app/directives/mlLogUsage.js
     */
    public static final String ML_META_INDEX = ".ml-meta";

    public static final String RESULTS_INDEX_PREFIX = ".ml-anomalies-";
    private static final String STATE_INDEX_NAME = ".ml-state";
    public static final String RESULTS_INDEX_DEFAULT = "shared";

    private AnomalyDetectorsIndex() {
    }

    public static String jobResultsIndexPrefix() {
        return RESULTS_INDEX_PREFIX;
    }

    /**
     * The name of the default index where the job's results are stored
     * @param jobId Job Id
     * @return The index name
     */
    public static String jobResultsAliasedName(String jobId) {
        return RESULTS_INDEX_PREFIX + jobId;
    }

    /**
     * Retrieves the currently defined physical index from the job state
     * @param jobId Job Id
     * @return The index name
     */
    public static String getPhysicalIndexFromState(ClusterState state, String jobId) {
        MlMetadata meta = state.getMetaData().custom(MlMetadata.TYPE);
        return meta.getJobs().get(jobId).getResultsIndexName();
    }

    /**
     * The name of the default index where a job's state is stored
     * @return The index name
     */
    public static String jobStateIndexName() {
        return STATE_INDEX_NAME;
    }
}
