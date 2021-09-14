/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;

import static org.hamcrest.Matchers.equalTo;

public class DataCountsTrackerTests extends ESTestCase {

    private static final String JOB_ID = "test";

    public void testReset() {
        DataCountsTracker dataCountsTracker = new DataCountsTracker(new DataCounts(JOB_ID, 10, 20, 30));
        dataCountsTracker.reset();
        DataCounts resetDataCounts = dataCountsTracker.report();
        assertThat(resetDataCounts, equalTo(new DataCounts(JOB_ID)));
    }

    public void testSetTestDocsCount() {
        DataCountsTracker dataCountsTracker = new DataCountsTracker(new DataCounts(JOB_ID, 10, 20, 30));
        dataCountsTracker.setTestDocsCount(15);
        DataCounts resetDataCounts = dataCountsTracker.report();
        assertThat(resetDataCounts, equalTo(new DataCounts(JOB_ID, 10, 15, 30)));
    }
}
