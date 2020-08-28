/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;

public class DataCountsTracker {

    private volatile long trainingDocsCount;
    private volatile long testDocsCount;
    private volatile long skippedDocsCount;

    public void incrementTrainingDocsCount() {
        trainingDocsCount++;
    }

    public void incrementTestDocsCount() {
        testDocsCount++;
    }

    public void incrementSkippedDocsCount() {
        skippedDocsCount++;
    }

    public DataCounts report(String jobId) {
        return new DataCounts(
            jobId,
            trainingDocsCount,
            testDocsCount,
            skippedDocsCount
        );
    }
}
