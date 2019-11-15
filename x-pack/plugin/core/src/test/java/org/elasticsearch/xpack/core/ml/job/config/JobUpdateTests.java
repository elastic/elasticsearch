/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class JobUpdateTests extends AbstractSerializingTestCase<JobUpdate> {

    private boolean useInternalParser = randomBoolean();

    @Override
    protected JobUpdate createTestInstance() {
        return createRandom(randomAlphaOfLength(4), null);
    }

    /**
     * Creates a completely random update when the job is null
     * or a random update that is is valid for the given job
     */
    public JobUpdate createRandom(String jobId, @Nullable Job job) {
        JobUpdate.Builder update = new JobUpdate.Builder(jobId);
        if (randomBoolean()) {
            int groupsNum = randomIntBetween(0, 10);
            List<String> groups = new ArrayList<>(groupsNum);
            for (int i = 0; i < groupsNum; i++) {
                groups.add(JobTests.randomValidJobId());
            }
            update.setGroups(groups);
        }
        if (randomBoolean()) {
            update.setDescription(randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            List<JobUpdate.DetectorUpdate> detectorUpdates = job == null ? createRandomDetectorUpdates()
                    : createRandomDetectorUpdatesForJob(job);
            update.setDetectorUpdates(detectorUpdates);
        }
        if (randomBoolean()) {
            update.setModelPlotConfig(new ModelPlotConfig(randomBoolean(), randomAlphaOfLength(10)));
        }
        if (randomBoolean()) {
            update.setAnalysisLimits(AnalysisLimits.validateAndSetDefaults(AnalysisLimitsTests.createRandomized(), null,
                    AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB));
        }
        if (randomBoolean()) {
            update.setRenormalizationWindowDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            update.setBackgroundPersistInterval(TimeValue.timeValueHours(randomIntBetween(1, 24)));
        }
        if (randomBoolean()) {
            update.setModelSnapshotRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            update.setResultsRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean() && jobSupportsCategorizationFilters(job)) {
            update.setCategorizationFilters(Arrays.asList(generateRandomStringArray(10, 10, false)));
        }
        if (randomBoolean()) {
            update.setCustomSettings(Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        if (useInternalParser && randomBoolean()) {
            update.setModelSnapshotId(randomAlphaOfLength(10));
        }
        if (useInternalParser && randomBoolean()) {
            update.setModelSnapshotMinVersion(Version.CURRENT);
        }
        if (useInternalParser && randomBoolean()) {
            update.setJobVersion(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
        }
        if (useInternalParser) {
            update.setClearFinishTime(randomBoolean());
        }
        if (randomBoolean()) {
            update.setAllowLazyOpen(randomBoolean());
        }

        return update.build();
    }

    private static boolean jobSupportsCategorizationFilters(@Nullable Job job) {
        if (job == null) {
            return true;
        }
        if (job.getAnalysisConfig().getCategorizationFieldName() == null) {
            return false;
        }
        if (job.getAnalysisConfig().getCategorizationAnalyzerConfig() != null) {
            return false;
        }
        return true;
    }

    private static List<JobUpdate.DetectorUpdate> createRandomDetectorUpdates() {
        int size = randomInt(10);
        List<JobUpdate.DetectorUpdate> detectorUpdates = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String detectorDescription = null;
            if (randomBoolean()) {
                detectorDescription = randomAlphaOfLength(12);
            }
            List<DetectionRule> detectionRules = null;
            if (randomBoolean()) {
                detectionRules = new ArrayList<>();
                detectionRules.add(new DetectionRule.Builder(
                        Collections.singletonList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 5))).build());
            }
            detectorUpdates.add(new JobUpdate.DetectorUpdate(i, detectorDescription, detectionRules));
        }
        return detectorUpdates;
    }

    private static List<JobUpdate.DetectorUpdate> createRandomDetectorUpdatesForJob(Job job) {
        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        int size = randomInt(analysisConfig.getDetectors().size());
        List<JobUpdate.DetectorUpdate> detectorUpdates = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String detectorDescription = null;
            if (randomBoolean()) {
                detectorDescription = randomAlphaOfLength(12);
            }
            int rulesSize = randomBoolean() ? randomIntBetween(1, 5) : 0;
            List<DetectionRule> detectionRules = rulesSize == 0 ? null : new ArrayList<>(rulesSize);
            for (int ruleIndex = 0; ruleIndex < rulesSize; ++ruleIndex) {
                int detectorIndex = randomInt(analysisConfig.getDetectors().size() - 1);
                Detector detector = analysisConfig.getDetectors().get(detectorIndex);
                List<String> analysisFields = detector.extractAnalysisFields();
                if (randomBoolean() || analysisFields.isEmpty()) {
                    detectionRules.add(new DetectionRule.Builder(Collections.singletonList(new RuleCondition(
                            randomFrom(RuleCondition.AppliesTo.values()), randomFrom(Operator.values()), randomDouble()))).build());
                } else {
                    RuleScope.Builder ruleScope = RuleScope.builder();
                    int scopeSize = randomIntBetween(1, analysisFields.size());
                    Set<String> analysisFieldsPickPot = new HashSet<>(analysisFields);
                    for (int scopeIndex = 0; scopeIndex < scopeSize; ++scopeIndex) {
                        String scopedField = randomFrom(analysisFieldsPickPot);
                        analysisFieldsPickPot.remove(scopedField);
                        if (randomBoolean()) {
                            ruleScope.include(scopedField, MlFilterTests.randomValidFilterId());
                        } else {
                            ruleScope.exclude(scopedField, MlFilterTests.randomValidFilterId());
                        }
                    }
                    detectionRules.add(new DetectionRule.Builder(ruleScope).build());
                }
            }
            detectorUpdates.add(new JobUpdate.DetectorUpdate(i, detectorDescription, detectionRules));
        }
        return detectorUpdates;
    }

    @Override
    protected Writeable.Reader<JobUpdate> instanceReader() {
        return JobUpdate::new;
    }

    @Override
    protected JobUpdate doParseInstance(XContentParser parser) {
        if (useInternalParser) {
            return JobUpdate.INTERNAL_PARSER.apply(parser, null).build();
        } else {
            return JobUpdate.EXTERNAL_PARSER.apply(parser, null).build();
        }
    }

    public void testMergeWithJob() {
        List<JobUpdate.DetectorUpdate> detectorUpdates = new ArrayList<>();
        List<DetectionRule> detectionRules1 = Collections.singletonList(new DetectionRule.Builder(
                Collections.singletonList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 5)))
                .build());
        detectorUpdates.add(new JobUpdate.DetectorUpdate(0, "description-1", detectionRules1));
        List<DetectionRule> detectionRules2 = Collections.singletonList(new DetectionRule.Builder(Collections.singletonList(
                new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 5))).build());
        detectorUpdates.add(new JobUpdate.DetectorUpdate(1, "description-2", detectionRules2));

        ModelPlotConfig modelPlotConfig = new ModelPlotConfig(randomBoolean(), randomAlphaOfLength(10));
        AnalysisLimits analysisLimits = new AnalysisLimits(randomNonNegativeLong(), randomNonNegativeLong());
        List<String> categorizationFilters = Arrays.asList(generateRandomStringArray(10, 10, false));
        Map<String, Object> customSettings = Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10));

        JobUpdate.Builder updateBuilder = new JobUpdate.Builder("foo");
        updateBuilder.setGroups(Arrays.asList("group-1", "group-2"));
        updateBuilder.setDescription("updated_description");
        updateBuilder.setDetectorUpdates(detectorUpdates);
        updateBuilder.setModelPlotConfig(modelPlotConfig);
        updateBuilder.setAnalysisLimits(analysisLimits);
        updateBuilder.setBackgroundPersistInterval(TimeValue.timeValueHours(randomIntBetween(1, 24)));
        updateBuilder.setResultsRetentionDays(randomNonNegativeLong());
        updateBuilder.setModelSnapshotRetentionDays(randomNonNegativeLong());
        updateBuilder.setRenormalizationWindowDays(randomNonNegativeLong());
        updateBuilder.setCategorizationFilters(categorizationFilters);
        updateBuilder.setCustomSettings(customSettings);
        updateBuilder.setModelSnapshotId(randomAlphaOfLength(10));
        updateBuilder.setJobVersion(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
        JobUpdate update = updateBuilder.build();

        Job.Builder jobBuilder = new Job.Builder("foo");
        jobBuilder.setGroups(Arrays.asList("group-1"));
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("mlcategory");
        Detector.Builder d2 = new Detector.Builder("min", "field");
        d2.setOverFieldName("host");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build()));
        ac.setCategorizationFieldName("cat_field");
        jobBuilder.setAnalysisConfig(ac);
        jobBuilder.setDataDescription(new DataDescription.Builder());
        jobBuilder.setCreateTime(new Date());
        Job job = jobBuilder.build();

        Job updatedJob = update.mergeWithJob(job, new ByteSizeValue(0L));

        assertEquals(update.getGroups(), updatedJob.getGroups());
        assertEquals(update.getDescription(), updatedJob.getDescription());
        assertEquals(update.getModelPlotConfig(), updatedJob.getModelPlotConfig());
        assertEquals(update.getAnalysisLimits(), updatedJob.getAnalysisLimits());
        assertEquals(update.getRenormalizationWindowDays(), updatedJob.getRenormalizationWindowDays());
        assertEquals(update.getBackgroundPersistInterval(), updatedJob.getBackgroundPersistInterval());
        assertEquals(update.getModelSnapshotRetentionDays(), updatedJob.getModelSnapshotRetentionDays());
        assertEquals(update.getResultsRetentionDays(), updatedJob.getResultsRetentionDays());
        assertEquals(update.getCategorizationFilters(), updatedJob.getAnalysisConfig().getCategorizationFilters());
        assertEquals(update.getCustomSettings(), updatedJob.getCustomSettings());
        assertEquals(update.getModelSnapshotId(), updatedJob.getModelSnapshotId());
        assertEquals(update.getJobVersion(), updatedJob.getJobVersion());
        for (JobUpdate.DetectorUpdate detectorUpdate : update.getDetectorUpdates()) {
            Detector updatedDetector = updatedJob.getAnalysisConfig().getDetectors().get(detectorUpdate.getDetectorIndex());
            assertNotNull(updatedDetector);
            assertEquals(detectorUpdate.getDescription(), updatedDetector.getDetectorDescription());
            assertEquals(detectorUpdate.getRules(), updatedDetector.getRules());
        }

        assertThat(job, not(equalTo(updatedJob)));
    }

    public void testMergeWithJob_GivenRandomUpdates_AssertImmutability() {
        for (int i = 0; i < 100; ++i) {
            Job job = JobTests.createRandomizedJob();
            JobUpdate update = createRandom(job.getId(), job);
            while (update.isNoop(job)) {
                update = createRandom(job.getId(), job);
            }

            Job updatedJob = update.mergeWithJob(job, new ByteSizeValue(0L));

            assertThat(job, not(equalTo(updatedJob)));
        }
    }

    public void testIsAutodetectProcessUpdate() {
        JobUpdate update = new JobUpdate.Builder("foo").build();
        assertFalse(update.isAutodetectProcessUpdate());
        update = new JobUpdate.Builder("foo").setModelPlotConfig(new ModelPlotConfig(true, "ff")).build();
        assertTrue(update.isAutodetectProcessUpdate());
        update = new JobUpdate.Builder("foo").setDetectorUpdates(Collections.singletonList(mock(JobUpdate.DetectorUpdate.class))).build();
        assertTrue(update.isAutodetectProcessUpdate());
        update = new JobUpdate.Builder("foo").setGroups(Arrays.asList("bar")).build();
        assertTrue(update.isAutodetectProcessUpdate());
    }

    public void testUpdateAnalysisLimitWithValueGreaterThanMax() {
        Job.Builder jobBuilder = new Job.Builder("foo");
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("mlcategory");
        Detector.Builder d2 = new Detector.Builder("min", "field");
        d2.setOverFieldName("host");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build()));
        ac.setCategorizationFieldName("cat_field");
        jobBuilder.setAnalysisConfig(ac);
        jobBuilder.setDataDescription(new DataDescription.Builder());
        jobBuilder.setCreateTime(new Date());
        jobBuilder.setAnalysisLimits(new AnalysisLimits(256L, null));

        JobUpdate update = new JobUpdate.Builder("foo").setAnalysisLimits(new AnalysisLimits(1024L, null)).build();

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> update.mergeWithJob(jobBuilder.build(), new ByteSizeValue(512L, ByteSizeUnit.MB)));
        assertEquals("model_memory_limit [1gb] must be less than the value of the xpack.ml.max_model_memory_limit setting [512mb]",
                e.getMessage());
    }

    public void testUpdate_withAnalysisLimitsPreviouslyUndefined() {
        Job.Builder jobBuilder = new Job.Builder("foo");
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d1.build()));
        jobBuilder.setAnalysisConfig(ac);
        jobBuilder.setDataDescription(new DataDescription.Builder());
        jobBuilder.setCreateTime(new Date());
        jobBuilder.validateAnalysisLimitsAndSetDefaults(null);

        JobUpdate update = new JobUpdate.Builder("foo").setAnalysisLimits(new AnalysisLimits(2048L, 5L)).build();
        Job updated = update.mergeWithJob(jobBuilder.build(), new ByteSizeValue(0L));
        assertThat(updated.getAnalysisLimits().getModelMemoryLimit(), equalTo(2048L));
        assertThat(updated.getAnalysisLimits().getCategorizationExamplesLimit(), equalTo(5L));

        JobUpdate updateAboveMaxLimit = new JobUpdate.Builder("foo").setAnalysisLimits(new AnalysisLimits(8000L, null)).build();

        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> updateAboveMaxLimit.mergeWithJob(jobBuilder.build(), new ByteSizeValue(5000L, ByteSizeUnit.MB)));
        assertEquals("model_memory_limit [7.8gb] must be less than the value of the xpack.ml.max_model_memory_limit setting [4.8gb]",
                e.getMessage());

        updateAboveMaxLimit.mergeWithJob(jobBuilder.build(), new ByteSizeValue(10000L, ByteSizeUnit.MB));
    }
}
