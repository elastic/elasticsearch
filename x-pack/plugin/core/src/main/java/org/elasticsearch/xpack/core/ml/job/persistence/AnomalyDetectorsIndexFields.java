/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

public final class AnomalyDetectorsIndexFields {

    public static final String CONFIG_INDEX = ".ml-config";
    public static final String RESULTS_INDEX_PREFIX = ".ml-anomalies-";
    public static final String STATE_INDEX_PREFIX = ".ml-state";
    public static final String RESULTS_INDEX_DEFAULT = "shared";

    private AnomalyDetectorsIndexFields() {}
}
