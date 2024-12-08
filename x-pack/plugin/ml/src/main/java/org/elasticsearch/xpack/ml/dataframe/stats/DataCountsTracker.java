/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;

import java.util.Objects;

public class DataCountsTracker {

    private final String jobId;
    private volatile long trainingDocsCount;
    private volatile long testDocsCount;
    private volatile long skippedDocsCount;

    public DataCountsTracker(DataCounts dataCounts) {
        this.jobId = Objects.requireNonNull(dataCounts.getJobId());
        this.trainingDocsCount = dataCounts.getTrainingDocsCount();
        this.testDocsCount = dataCounts.getTestDocsCount();
        this.skippedDocsCount = dataCounts.getSkippedDocsCount();
    }

    public void incrementTrainingDocsCount() {
        trainingDocsCount++;
    }

    public void incrementTestDocsCount() {
        testDocsCount++;
    }

    public void incrementSkippedDocsCount() {
        skippedDocsCount++;
    }

    public DataCounts report() {
        return new DataCounts(jobId, trainingDocsCount, testDocsCount, skippedDocsCount);
    }

    public void reset() {
        trainingDocsCount = 0;
        testDocsCount = 0;
        skippedDocsCount = 0;
    }

    public void setTestDocsCount(long testDocsCount) {
        this.testDocsCount = testDocsCount;
    }
}
