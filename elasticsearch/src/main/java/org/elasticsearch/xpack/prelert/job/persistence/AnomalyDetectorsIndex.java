/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

/**
 * Methods for handling index naming related functions
 */
public final class AnomalyDetectorsIndex {
    private static final String INDEX_PREFIX = "prelertresults-";

    private AnomalyDetectorsIndex() {
    }

    public static String getJobIndexName(String jobId) {
        return INDEX_PREFIX + jobId;
    }

}
