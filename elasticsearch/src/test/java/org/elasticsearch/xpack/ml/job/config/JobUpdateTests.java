/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class JobUpdateTests extends AbstractSerializingTestCase<JobUpdate> {

    @Override
    protected JobUpdate createTestInstance() {
        String description = null;
        if (randomBoolean()) {
            description = randomAsciiOfLength(20);
        }
        List<JobUpdate.DetectorUpdate> detectorUpdates = null;
        if (randomBoolean()) {
            int size = randomInt(10);
            detectorUpdates = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                String detectorDescription = null;
                if (randomBoolean()) {
                    detectorDescription = randomAsciiOfLength(12);
                }
                List<DetectionRule> detectionRules = null;
                if (randomBoolean()) {
                    detectionRules = new ArrayList<>();
                    Condition condition = new Condition(Operator.GT, "5");
                    detectionRules.add(new DetectionRule("foo", null, Connective.OR, Collections.singletonList(
                            new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null))));
                }
                detectorUpdates.add(new JobUpdate.DetectorUpdate(i, detectorDescription, detectionRules));
            }
        }
        ModelDebugConfig modelDebugConfig = null;
        if (randomBoolean()) {
            modelDebugConfig = new ModelDebugConfig(randomDouble(), randomAsciiOfLength(10));
        }
        AnalysisLimits analysisLimits = null;
        if (randomBoolean()) {
            analysisLimits = new AnalysisLimits(randomNonNegativeLong(), randomNonNegativeLong());
        }
        Long renormalizationWindowDays = null;
        if (randomBoolean()) {
            renormalizationWindowDays = randomNonNegativeLong();
        }
        Long backgroundPersistInterval = null;
        if (randomBoolean()) {
            backgroundPersistInterval = randomNonNegativeLong();
        }
        Long modelSnapshotRetentionDays = null;
        if (randomBoolean()) {
            modelSnapshotRetentionDays = randomNonNegativeLong();
        }
        Long resultsRetentionDays = null;
        if (randomBoolean()) {
            resultsRetentionDays = randomNonNegativeLong();
        }
        List<String> categorizationFilters = null;
        if (randomBoolean()) {
            categorizationFilters = Arrays.asList(generateRandomStringArray(10, 10, false));
        }
        Map<String, Object> customSettings = null;
        if (randomBoolean()) {
            customSettings = Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10));
        }

        return new JobUpdate(description, detectorUpdates, modelDebugConfig, analysisLimits, backgroundPersistInterval,
                renormalizationWindowDays, resultsRetentionDays, modelSnapshotRetentionDays, categorizationFilters, customSettings);
    }

    @Override
    protected Writeable.Reader<JobUpdate> instanceReader() {
        return JobUpdate::new;
    }

    @Override
    protected JobUpdate parseInstance(XContentParser parser) {
        return JobUpdate.PARSER.apply(parser, null);
    }

    public void testMergeWithJob() {
        List<JobUpdate.DetectorUpdate> detectorUpdates = new ArrayList<>();
        List<DetectionRule> detectionRules1 = Collections.singletonList(new DetectionRule("client", null, Connective.OR,
                Collections.singletonList(
                        new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, new Condition(Operator.GT, "5"), null))));
        detectorUpdates.add(new JobUpdate.DetectorUpdate(0, "description-1", detectionRules1));
        List<DetectionRule> detectionRules2 = Collections.singletonList(new DetectionRule("host", null, Connective.OR,
                Collections.singletonList(
                        new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, new Condition(Operator.GT, "5"), null))));
        detectorUpdates.add(new JobUpdate.DetectorUpdate(1, "description-2", detectionRules2));

        ModelDebugConfig modelDebugConfig = new ModelDebugConfig(randomDouble(), randomAsciiOfLength(10));
        AnalysisLimits analysisLimits = new AnalysisLimits(randomNonNegativeLong(), randomNonNegativeLong());
        List<String> categorizationFilters = Arrays.asList(generateRandomStringArray(10, 10, false));
        Map<String, Object> customSettings = Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10));

        JobUpdate update = new JobUpdate("updated_description", detectorUpdates, modelDebugConfig,
                analysisLimits, randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                categorizationFilters, customSettings);

        Job.Builder jobBuilder = new Job.Builder("foo");
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("client");
        Detector.Builder d2 = new Detector.Builder("min", "field");
        d2.setOverFieldName("host");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build()));
        ac.setCategorizationFieldName("cat_field");
        jobBuilder.setAnalysisConfig(ac);

        Job updatedJob = update.mergeWithJob(jobBuilder.build());

        assertEquals(update.getDescription(), updatedJob.getDescription());
        assertEquals(update.getModelDebugConfig(), updatedJob.getModelDebugConfig());
        assertEquals(update.getAnalysisLimits(), updatedJob.getAnalysisLimits());
        assertEquals(update.getRenormalizationWindowDays(), updatedJob.getRenormalizationWindowDays());
        assertEquals(update.getBackgroundPersistInterval(), updatedJob.getBackgroundPersistInterval());
        assertEquals(update.getModelSnapshotRetentionDays(), updatedJob.getModelSnapshotRetentionDays());
        assertEquals(update.getResultsRetentionDays(), updatedJob.getResultsRetentionDays());
        assertEquals(update.getCategorizationFilters(), updatedJob.getAnalysisConfig().getCategorizationFilters());
        assertEquals(update.getCustomSettings(), updatedJob.getCustomSettings());
        for (JobUpdate.DetectorUpdate detectorUpdate : update.getDetectorUpdates()) {
            assertNotNull(updatedJob.getAnalysisConfig().getDetectors().get(detectorUpdate.getIndex()).getDetectorDescription());
            assertEquals(detectorUpdate.getDescription(),
                    updatedJob.getAnalysisConfig().getDetectors().get(detectorUpdate.getIndex()).getDetectorDescription());
            assertNotNull(updatedJob.getAnalysisConfig().getDetectors().get(detectorUpdate.getIndex()).getDetectorDescription());
            assertEquals(detectorUpdate.getRules(),
                    updatedJob.getAnalysisConfig().getDetectors().get(detectorUpdate.getIndex()).getDetectorRules());
        }
    }

    public void testIsAutodetectProcessUpdate() {
        JobUpdate update = new JobUpdate(null, null, null, null, null, null, null, null, null, null);
        assertFalse(update.isAutodetectProcessUpdate());
        update = new JobUpdate(null, null, new ModelDebugConfig(1.0, "ff"), null, null, null, null, null, null, null);
        assertTrue(update.isAutodetectProcessUpdate());
        update = new JobUpdate(null, Arrays.asList(mock(JobUpdate.DetectorUpdate.class)), null, null, null, null, null, null, null, null);
        assertTrue(update.isAutodetectProcessUpdate());
    }
}