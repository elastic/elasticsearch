/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class UpdateParamsTests extends ESTestCase {

    public void testFromJobUpdate() {
        String jobId = "foo";
        DetectionRule rule = new DetectionRule.Builder(
            Collections.singletonList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 1.0))
        ).build();
        List<DetectionRule> rules = Collections.singletonList(rule);
        List<JobUpdate.DetectorUpdate> detectorUpdates = Collections.singletonList(new JobUpdate.DetectorUpdate(2, null, rules));
        JobUpdate.Builder updateBuilder = new JobUpdate.Builder(jobId).setModelPlotConfig(new ModelPlotConfig())
            .setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig())
            .setDetectorUpdates(detectorUpdates);

        UpdateParams params = UpdateParams.fromJobUpdate(updateBuilder.build());

        assertFalse(params.isUpdateScheduledEvents());
        assertEquals(params.getDetectorUpdates(), updateBuilder.build().getDetectorUpdates());
        assertEquals(params.getModelPlotConfig(), updateBuilder.build().getModelPlotConfig());
        assertEquals(params.getPerPartitionCategorizationConfig(), updateBuilder.build().getPerPartitionCategorizationConfig());

        params = UpdateParams.fromJobUpdate(updateBuilder.setGroups(Collections.singletonList("bar")).build());

        assertTrue(params.isUpdateScheduledEvents());
        assertTrue(params.isJobUpdate());
    }

    public void testExtractReferencedFilters() {
        JobUpdate.DetectorUpdate detectorUpdate1 = new JobUpdate.DetectorUpdate(
            0,
            "",
            Arrays.asList(
                new DetectionRule.Builder(RuleScope.builder().include("a", "filter_1")).build(),
                new DetectionRule.Builder(RuleScope.builder().include("b", "filter_2")).build()
            )
        );
        JobUpdate.DetectorUpdate detectorUpdate2 = new JobUpdate.DetectorUpdate(
            0,
            "",
            Collections.singletonList(new DetectionRule.Builder(RuleScope.builder().include("c", "filter_3")).build())
        );

        UpdateParams updateParams = new UpdateParams.Builder("test_job").detectorUpdates(Arrays.asList(detectorUpdate1, detectorUpdate2))
            .filter(MlFilter.builder("filter_4").build())
            .build();

        assertThat(updateParams.extractReferencedFilters(), containsInAnyOrder("filter_1", "filter_2", "filter_3", "filter_4"));
    }
}
