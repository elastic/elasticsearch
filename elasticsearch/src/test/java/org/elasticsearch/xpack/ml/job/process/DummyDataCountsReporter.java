/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.job.persistence.JobDataCountsPersister;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;

import static org.mockito.Mockito.mock;

/**
 * Dummy DataCountsReporter for testing
 */
class DummyDataCountsReporter extends DataCountsReporter {

    int logStatusCallCount = 0;

    DummyDataCountsReporter() {
        super(mock(ThreadPool.class), Settings.EMPTY, "DummyJobId", new DataCounts("DummyJobId"),
                mock(JobDataCountsPersister.class));
    }

    /**
     * It's difficult to use mocking to get the number of calls to {@link #logStatus(long)}
     * and Mockito.spy() doesn't work due to the lambdas used in {@link DataCountsReporter}.
     * Override the method here an count the calls
     */
    @Override
    protected void logStatus(long totalRecords) {
        super.logStatus(totalRecords);
        ++logStatusCallCount;
    }


    /**
     * @return Then number of times {@link #logStatus(long)} was called.
     */
    public int getLogStatusCallCount() {
        return logStatusCallCount;
    }

    @Override
    public void close() {
        // Do nothing
    }
}
