/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

/**
 * Methods for handling index naming related functions
 */
public final class AnomalyDetectorsIndex {

    public static final int CONFIG_INDEX_MAX_RESULTS_WINDOW = 10_000;

    private AnomalyDetectorsIndex() {
    }

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
     * The name of the default index where a job's state is stored
     * @return The index name
     */
    public static String jobStateIndexName() {
        return AnomalyDetectorsIndexFields.STATE_INDEX_NAME;
    }

    /**
     * The name of the index where job and datafeed configuration
     * is stored
     * @return The index name
     */
    public static String configIndexName() {
        return AnomalyDetectorsIndexFields.CONFIG_INDEX;
    }

}
