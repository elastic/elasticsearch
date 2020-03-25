/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.job.config.RuleCondition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class UpdateParamsTests extends ESTestCase {

    public void testFromJobUpdate() {
        String jobId = "foo";
        DetectionRule rule = new DetectionRule.Builder(Arrays.asList(
            new RuleCondition(RuleCondition.AppliesTo.ACTUAL,
                Operator.GT, 1.0))).build();
        List<DetectionRule> rules = Arrays.asList(rule);
        List<JobUpdate.DetectorUpdate> detectorUpdates = Collections.singletonList(
            new JobUpdate.DetectorUpdate(2, null, rules));
        JobUpdate.Builder updateBuilder = new JobUpdate.Builder(jobId)
            .setModelPlotConfig(new ModelPlotConfig())
            .setDetectorUpdates(detectorUpdates);

        UpdateParams params = UpdateParams.fromJobUpdate(updateBuilder.build());

        assertFalse(params.isUpdateScheduledEvents());
        assertEquals(params.getDetectorUpdates(), updateBuilder.build().getDetectorUpdates());
        assertEquals(params.getModelPlotConfig(), updateBuilder.build().getModelPlotConfig());

        params = UpdateParams.fromJobUpdate(updateBuilder.setGroups(Arrays.asList("bar")).build());

        assertTrue(params.isUpdateScheduledEvents());
    }

}
