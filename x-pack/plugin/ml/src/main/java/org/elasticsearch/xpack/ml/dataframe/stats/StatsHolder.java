/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.stats;

/**
 * Holds data frame analytics stats in memory so that they may be retrieved
 * from the get stats api for started jobs efficiently.
 */
public class StatsHolder {

    private final ProgressTracker progressTracker = new ProgressTracker();

    public ProgressTracker getProgressTracker() {
        return progressTracker;
    }
}
