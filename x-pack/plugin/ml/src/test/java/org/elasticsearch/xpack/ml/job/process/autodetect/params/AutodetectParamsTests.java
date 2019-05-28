/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.params;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;

import static org.hamcrest.Matchers.equalTo;

public class AutodetectParamsTests extends ESTestCase {

    private static final String JOB_ID = "my-job";

    public void testBuilder_WithTimingStats() {
        TimingStats timingStats = new TimingStats(JOB_ID, 7, 1.0, 1000.0, 666.0);
        AutodetectParams params = new AutodetectParams.Builder(JOB_ID).setTimingStats(timingStats).build();
        assertThat(params.timingStats(), equalTo(timingStats));
    }

    public void testBuilder_WithoutTimingStats() {
        AutodetectParams params = new AutodetectParams.Builder(JOB_ID).build();
        assertThat(params.timingStats(), equalTo(new TimingStats(JOB_ID)));
    }
}
