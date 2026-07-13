/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;

import static org.hamcrest.Matchers.equalTo;

public class AutodetectParamsTests extends ESTestCase {

    private static final String JOB_ID = "my-job";

    public void testBuilder_WithTimingStats() {
        TimingStats timingStats = new TimingStats(JOB_ID, 7, 1.0, 1000.0, 666.0, 1000.0, new ExponentialAverageCalculationContext());
        AutodetectParams params = new AutodetectParams.Builder(JOB_ID).setTimingStats(timingStats).build();
        assertThat(params.timingStats(), equalTo(timingStats));

        timingStats.updateStats(2000.0);
        assertThat(
            timingStats,
            equalTo(new TimingStats(JOB_ID, 8, 1.0, 2000.0, 832.75, 1010.0, new ExponentialAverageCalculationContext(2000.0, null, null)))
        );
        assertThat(
            params.timingStats(),
            equalTo(new TimingStats(JOB_ID, 7, 1.0, 1000.0, 666.0, 1000.0, new ExponentialAverageCalculationContext()))
        );
    }

    public void testBuilder_WithoutTimingStats() {
        AutodetectParams params = new AutodetectParams.Builder(JOB_ID).build();
        assertThat(params.timingStats(), equalTo(new TimingStats(JOB_ID)));
    }
}
