/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class MlConfigSizeUsageTests extends ESTestCase {

    public void testCollectJobConfigSizesIncludesDescriptionHistogram() {
        Job.Builder builder = JobTests.buildJobBuilder("job-1");
        builder.setDescription("Anomaly detection for CPU usage");
        Map<String, Object> configSizes = MlConfigSizeUsage.collectJobConfigSizes(List.of(builder.build()));

        assertThat(configSizes.containsKey("description"), is(true));
        @SuppressWarnings("unchecked")
        Map<String, Object> description = (Map<String, Object>) configSizes.get("description");
        assertThat(description.get(SizeHistogramAccumulator.COUNT), equalTo(1L));
        assertThat((Double) description.get(StatsAccumulator.Fields.MAX), greaterThan(0.0));
    }

    public void testPutConfigSizesSkipsEmptyMap() {
        Map<String, Object> usageEntry = new java.util.HashMap<>();
        MlConfigSizeUsage.putConfigSizes(usageEntry, Map.of());
        assertThat(usageEntry.isEmpty(), is(true));
    }
}
