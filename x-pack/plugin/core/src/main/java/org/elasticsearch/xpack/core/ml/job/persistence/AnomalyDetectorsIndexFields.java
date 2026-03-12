/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

public final class AnomalyDetectorsIndexFields {

    public static final String STATE_INDEX_PREFIX = ".ml-state";

    public static final String RESULTS_INDEX_PREFIX = ".ml-anomalies-";
    // ".write" rather than simply "write" to avoid the danger of clashing
    // with the read alias of a job whose name begins with "write-"
    public static final String RESULTS_INDEX_WRITE_PREFIX = RESULTS_INDEX_PREFIX + ".write-";
    public static final String RESULTS_INDEX_DEFAULT = "shared";

    private AnomalyDetectorsIndexFields() {}
}
