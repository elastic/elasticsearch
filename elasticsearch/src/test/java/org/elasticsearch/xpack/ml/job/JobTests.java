/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.condition.Condition;
import org.elasticsearch.xpack.ml.job.condition.Operator;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.transform.TransformType;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobTests extends AbstractSerializingTestCase<Job> {

    @Override
    protected Job createTestInstance() {
        return createRandomizedJob();
    }

    @Override
    protected Writeable.Reader<Job> instanceReader() {
        return Job::new;
    }

    @Override
    protected Job parseInstance(XContentParser parser) {
        return Job.PARSER.apply(parser, null).build();
    }

    public void testConstructor_GivenEmptyJobConfiguration() {
        Job job = buildJobBuilder("foo").build(true, "foo");

        assertEquals("foo", job.getId());
        assertNotNull(job.getCreateTime());
        assertEquals(600L, job.getTimeout());
        assertNotNull(job.getAnalysisConfig());
        assertNull(job.getAnalysisLimits());
        assertNull(job.getCustomSettings());
        assertNotNull(job.getDataDescription());
        assertNull(job.getDescription());
        assertNull(job.getFinishedTime());
        assertNull(job.getIgnoreDowntime());
        assertNull(job.getLastDataTime());
        assertNull(job.getModelDebugConfig());
        assertNull(job.getRenormalizationWindowDays());
        assertNull(job.getBackgroundPersistInterval());
        assertNull(job.getModelSnapshotRetentionDays());
        assertNull(job.getResultsRetentionDays());
        assertEquals(Collections.emptyList(), job.getTransforms());
        assertNotNull(job.allFields());
        assertFalse(job.allFields().isEmpty());
    }

    public void testConstructor_GivenJobConfigurationWithIgnoreDowntime() {
        Job.Builder builder = new Job.Builder("foo");
        builder.setIgnoreDowntime(IgnoreDowntime.ONCE);
        builder.setAnalysisConfig(createAnalysisConfig());
        Job job = builder.build();

        assertEquals("foo", job.getId());
        assertEquals(IgnoreDowntime.ONCE, job.getIgnoreDowntime());
    }

    public void testNoId() {
        expectThrows(IllegalArgumentException.class, () -> buildJobBuilder("").build(true, ""));
    }

    public void testInconsistentId() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> buildJobBuilder("foo").build(true, "bar"));
        assertEquals(Messages.getMessage(Messages.INCONSISTENT_ID, Job.ID.getPreferredName(), "foo", "bar"), e.getMessage());
    }

    public void testEquals_GivenDifferentClass() {
        Job job = buildJobBuilder("foo").build();
        assertFalse(job.equals("a string"));
    }

    public void testEquals_GivenDifferentIds() {
        Date createTime = new Date();
        Job.Builder builder = buildJobBuilder("foo");
        builder.setCreateTime(createTime);
        Job job1 = builder.build();
        builder.setId("bar");
        Job job2 = builder.build();
        assertFalse(job1.equals(job2));
    }

    public void testEquals_GivenDifferentRenormalizationWindowDays() {
        Job.Builder jobDetails1 = new Job.Builder("foo");
        jobDetails1.setAnalysisConfig(createAnalysisConfig());
        jobDetails1.setRenormalizationWindowDays(3L);
        Job.Builder jobDetails2 = new Job.Builder("foo");
        jobDetails2.setRenormalizationWindowDays(4L);
        jobDetails2.setAnalysisConfig(createAnalysisConfig());
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentBackgroundPersistInterval() {
        Job.Builder jobDetails1 = new Job.Builder("foo");
        jobDetails1.setAnalysisConfig(createAnalysisConfig());
        jobDetails1.setBackgroundPersistInterval(10000L);
        Job.Builder jobDetails2 = new Job.Builder("foo");
        jobDetails2.setBackgroundPersistInterval(8000L);
        jobDetails2.setAnalysisConfig(createAnalysisConfig());
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentModelSnapshotRetentionDays() {
        Job.Builder jobDetails1 = new Job.Builder("foo");
        jobDetails1.setAnalysisConfig(createAnalysisConfig());
        jobDetails1.setModelSnapshotRetentionDays(10L);
        Job.Builder jobDetails2 = new Job.Builder("foo");
        jobDetails2.setModelSnapshotRetentionDays(8L);
        jobDetails2.setAnalysisConfig(createAnalysisConfig());
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentResultsRetentionDays() {
        Job.Builder jobDetails1 = new Job.Builder("foo");
        jobDetails1.setAnalysisConfig(createAnalysisConfig());
        jobDetails1.setResultsRetentionDays(30L);
        Job.Builder jobDetails2 = new Job.Builder("foo");
        jobDetails2.setResultsRetentionDays(4L);
        jobDetails2.setAnalysisConfig(createAnalysisConfig());
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentCustomSettings() {
        Job.Builder jobDetails1 = buildJobBuilder("foo");
        Map<String, Object> customSettings1 = new HashMap<>();
        customSettings1.put("key1", "value1");
        jobDetails1.setCustomSettings(customSettings1);
        Job.Builder jobDetails2 = buildJobBuilder("foo");
        Map<String, Object> customSettings2 = new HashMap<>();
        customSettings2.put("key2", "value2");
        jobDetails2.setCustomSettings(customSettings2);
        assertFalse(jobDetails1.build().equals(jobDetails2.build()));
    }

    public void testEquals_GivenDifferentIgnoreDowntime() {
        Job.Builder job1 = new Job.Builder();
        job1.setIgnoreDowntime(IgnoreDowntime.NEVER);
        Job.Builder job2 = new Job.Builder();
        job2.setIgnoreDowntime(IgnoreDowntime.ONCE);

        assertFalse(job1.equals(job2));
        assertFalse(job2.equals(job1));
    }

    public void testSetAnalysisLimits() {
        Job.Builder builder = new Job.Builder();
        builder.setAnalysisLimits(new AnalysisLimits(42L, null));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> builder.setAnalysisLimits(new AnalysisLimits(41L, null)));
        assertEquals("Invalid update value for analysis_limits: model_memory_limit cannot be decreased; existing is 42, update had 41",
                e.getMessage());
    }

    // JobConfigurationTests:

    /**
     * Test the {@link AnalysisConfig#analysisFields()} method which produces a
     * list of analysis fields from the detectors
     */
    public void testAnalysisConfigRequiredFields() {
        Detector.Builder d1 = new Detector.Builder("max", "field");
        d1.setByFieldName("by");

        Detector.Builder d2 = new Detector.Builder("metric", "field2");
        d2.setOverFieldName("over");

        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build()));
        ac.setSummaryCountFieldName("agg");

        List<String> analysisFields = ac.build().analysisFields();
        assertTrue(analysisFields.size() == 5);

        assertTrue(analysisFields.contains("agg"));
        assertTrue(analysisFields.contains("field"));
        assertTrue(analysisFields.contains("by"));
        assertTrue(analysisFields.contains("field2"));
        assertTrue(analysisFields.contains("over"));

        assertFalse(analysisFields.contains("max"));
        assertFalse(analysisFields.contains(""));
        assertFalse(analysisFields.contains(null));

        Detector.Builder d3 = new Detector.Builder("count", null);
        d3.setByFieldName("by2");
        d3.setPartitionFieldName("partition");

        ac = new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build(), d3.build()));

        analysisFields = ac.build().analysisFields();
        assertTrue(analysisFields.size() == 6);

        assertTrue(analysisFields.contains("partition"));
        assertTrue(analysisFields.contains("field"));
        assertTrue(analysisFields.contains("by"));
        assertTrue(analysisFields.contains("by2"));
        assertTrue(analysisFields.contains("field2"));
        assertTrue(analysisFields.contains("over"));

        assertFalse(analysisFields.contains("count"));
        assertFalse(analysisFields.contains("max"));
        assertFalse(analysisFields.contains(""));
        assertFalse(analysisFields.contains(null));
    }

    // JobConfigurationVerifierTests:

    public void testCheckValidId_IdTooLong()  {
        Job.Builder builder = buildJobBuilder("foo");
        builder.setId("averyveryveryaveryveryveryaveryveryveryaveryveryveryaveryveryveryaveryveryverylongid");
        expectThrows(IllegalArgumentException.class, () -> builder.build());
    }

    public void testCheckValidId_GivenAllValidChars() {
        Job.Builder builder = buildJobBuilder("foo");
        builder.setId("abcdefghijklmnopqrstuvwxyz-._0123456789");
        builder.build();
    }

    public void testCheckValidId_ProhibitedChars() {
        String invalidChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()+?\"'~±/\\[]{},<>=";
        Job.Builder builder = buildJobBuilder("foo");
        for (char c : invalidChars.toCharArray()) {
            builder.setId(Character.toString(c));
            String errorMessage = Messages.getMessage(Messages.INVALID_ID, Job.ID.getPreferredName(), Character.toString(c));
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
            assertEquals(errorMessage, e.getMessage());
        }
    }

    public void testCheckValidId_startsWithUnderscore() {
        Job.Builder builder = buildJobBuilder("_foo");
        String errorMessage = Messages.getMessage(Messages.INVALID_ID, Job.ID.getPreferredName(), "_foo");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertEquals(errorMessage, e.getMessage());
    }

    public void testCheckValidId_endsWithUnderscore() {
        Job.Builder builder = buildJobBuilder("foo_");
        String errorMessage = Messages.getMessage(Messages.INVALID_ID, Job.ID.getPreferredName(), "foo_");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertEquals(errorMessage, e.getMessage());
    }

    public void testCheckValidId_ControlChars() {
        Job.Builder builder = buildJobBuilder("foo");
        builder.setId("two\nlines");
        expectThrows(IllegalArgumentException.class, builder::build);
    }

    public void jobConfigurationTest() {
        Job.Builder builder = new Job.Builder();
        expectThrows(IllegalArgumentException.class, builder::build);
        builder.setId("bad id with spaces");
        expectThrows(IllegalArgumentException.class, builder::build);
        builder.setId("bad_id_with_UPPERCASE_chars");
        expectThrows(IllegalArgumentException.class, builder::build);
        builder.setId("a very  very very very very very very very very very very very very very very very very very very very long id");
        expectThrows(IllegalArgumentException.class, builder::build);
        builder.setId(null);
        expectThrows(IllegalArgumentException.class, builder::build);

        Detector.Builder d = new Detector.Builder("max", "a");
        d.setByFieldName("b");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        builder.setAnalysisConfig(ac);
        builder.build();
        builder.setAnalysisLimits(new AnalysisLimits(-1L, null));
        expectThrows(IllegalArgumentException.class, builder::build);
        AnalysisLimits limits = new AnalysisLimits(1000L, 4L);
        builder.setAnalysisLimits(limits);
        builder.build();
        DataDescription.Builder dc = new DataDescription.Builder();
        dc.setTimeFormat("YYY_KKKKajsatp*");
        builder.setDataDescription(dc);
        expectThrows(IllegalArgumentException.class, builder::build);
        dc = new DataDescription.Builder();
        builder.setDataDescription(dc);
        builder.setTimeout(-1L);
        expectThrows(IllegalArgumentException.class, builder::build);
        builder.setTimeout(300L);
        builder.build();
    }

    public void testCheckTransformOutputIsUsed_throws() {
        Job.Builder builder = buildJobBuilder("foo");
        TransformConfig tc = new TransformConfig(TransformType.Names.DOMAIN_SPLIT_NAME);
        tc.setInputs(Arrays.asList("dns"));
        builder.setTransforms(Arrays.asList(tc));
        expectThrows(IllegalArgumentException.class, builder::build);
        Detector.Builder newDetector = new Detector.Builder();
        newDetector.setFunction(Detector.MIN);
        newDetector.setFieldName(TransformType.DOMAIN_SPLIT.defaultOutputNames().get(0));
        AnalysisConfig.Builder config = new AnalysisConfig.Builder(Collections.singletonList(newDetector.build()));
        builder.setAnalysisConfig(config);
        builder.build();
    }

    public void testCheckTransformDuplicatOutput_outputIsSummaryCountField() {
        Job.Builder builder = buildJobBuilder("foo");
        AnalysisConfig.Builder config = createAnalysisConfig();
        config.setSummaryCountFieldName("summaryCountField");
        builder.setAnalysisConfig(config);
        TransformConfig tc = new TransformConfig(TransformType.Names.DOMAIN_SPLIT_NAME);
        tc.setInputs(Arrays.asList("dns"));
        tc.setOutputs(Arrays.asList("summaryCountField"));
        builder.setTransforms(Arrays.asList(tc));
        expectThrows(IllegalArgumentException.class, builder::build);
    }

    public void testCheckTransformOutputIsUsed_outputIsSummaryCountField() {
        Job.Builder builder = buildJobBuilder("foo");
        TransformConfig tc = new TransformConfig(TransformType.Names.EXTRACT_NAME);
        tc.setInputs(Arrays.asList("dns"));
        tc.setOutputs(Arrays.asList("summaryCountField"));
        tc.setArguments(Arrays.asList("(.*)"));
        builder.setTransforms(Arrays.asList(tc));
        expectThrows(IllegalArgumentException.class, builder::build);
    }

    public void testCheckTransformOutputIsUsed_transformHasNoOutput() {
        Job.Builder builder = buildJobBuilder("foo");
        // The exclude filter has no output
        TransformConfig tc = new TransformConfig(TransformType.Names.EXCLUDE_NAME);
        tc.setCondition(new Condition(Operator.MATCH, "whitelisted_host"));
        tc.setInputs(Arrays.asList("dns"));
        builder.setTransforms(Arrays.asList(tc));
        builder.build();
    }

    public void testVerify_GivenDataFormatIsSingleLineAndNullTransforms() {
        String errorMessage = Messages.getMessage(
                Messages.JOB_CONFIG_DATAFORMAT_REQUIRES_TRANSFORM,
                DataDescription.DataFormat.SINGLE_LINE);
        Job.Builder builder = buildJobBuilder("foo");
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.SINGLE_LINE);
        builder.setDataDescription(dataDescription);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenDataFormatIsSingleLineAndEmptyTransforms() {
        String errorMessage = Messages.getMessage(
                Messages.JOB_CONFIG_DATAFORMAT_REQUIRES_TRANSFORM,
                DataDescription.DataFormat.SINGLE_LINE);
        Job.Builder builder = buildJobBuilder("foo");
        builder.setTransforms(new ArrayList<>());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.SINGLE_LINE);
        builder.setDataDescription(dataDescription);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);

        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenDataFormatIsSingleLineAndNonEmptyTransforms() {
        ArrayList<TransformConfig> transforms = new ArrayList<>();
        TransformConfig transform = new TransformConfig("trim");
        transform.setInputs(Arrays.asList("raw"));
        transform.setOutputs(Arrays.asList("time"));
        transforms.add(transform);
        Job.Builder builder = buildJobBuilder("foo");
        builder.setTransforms(transforms);
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.SINGLE_LINE);
        builder.setDataDescription(dataDescription);
        builder.build();
    }

    public void testVerify_GivenNegativeRenormalizationWindowDays() {
        String errorMessage = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW,
                "renormalizationWindowDays", 0, -1);
        Job.Builder builder = buildJobBuilder("foo");
        builder.setRenormalizationWindowDays(-1L);
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, builder::build);
        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenNegativeModelSnapshotRetentionDays() {
        String errorMessage = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW, "modelSnapshotRetentionDays", 0, -1);
        Job.Builder builder = buildJobBuilder("foo");
        builder.setModelSnapshotRetentionDays(-1L);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);

        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenLowBackgroundPersistInterval() {
        String errorMessage = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW, "backgroundPersistInterval", 3600, 3599);
        Job.Builder builder = buildJobBuilder("foo");
        builder.setBackgroundPersistInterval(3599L);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertEquals(errorMessage, e.getMessage());
    }

    public void testVerify_GivenNegativeResultsRetentionDays() {
        String errorMessage = Messages.getMessage(Messages.JOB_CONFIG_FIELD_VALUE_TOO_LOW,
                "resultsRetentionDays", 0, -1);
        Job.Builder builder = buildJobBuilder("foo");
        builder.setResultsRetentionDays(-1L);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, builder::build);
        assertEquals(errorMessage, e.getMessage());
    }

    public void testBuilder_setsDefaultIndexName() {
        Job.Builder builder = buildJobBuilder("foo");
        Job job = builder.build();
        assertEquals("foo", job.getIndexName());
    }

    public void testBuilder_setsIndexName() {
        Job.Builder builder = buildJobBuilder("foo");
        builder.setIndexName("carol");
        Job job = builder.build();
        assertEquals("carol", job.getIndexName());
    }

    public void testBuilder_withInvalidIndexNameThrows () {
        Job.Builder builder = buildJobBuilder("foo");
        builder.setIndexName("_bad^name");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.build());
        assertEquals(Messages.getMessage(Messages.INVALID_ID, Job.INDEX_NAME.getPreferredName()), e.getMessage());
    }

    public static Job.Builder buildJobBuilder(String id) {
        Job.Builder builder = new Job.Builder(id);
        builder.setCreateTime(new Date());
        AnalysisConfig.Builder ac = createAnalysisConfig();
        DataDescription.Builder dc = new DataDescription.Builder();
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(dc);
        return builder;
    }

    public static String randomValidJobId() {
        CodepointSetGenerator generator =  new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    public static AnalysisConfig.Builder createAnalysisConfig() {
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("client");
        Detector.Builder d2 = new Detector.Builder("min", "field");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(d1.build(), d2.build()));
        return ac;
    }

    public static Job createRandomizedJob() {
        String jobId = randomValidJobId();
        Job.Builder builder = new Job.Builder(jobId);
        if (randomBoolean()) {
            builder.setDescription(randomAsciiOfLength(10));
        }
        builder.setCreateTime(new Date(randomNonNegativeLong()));
        if (randomBoolean()) {
            builder.setFinishedTime(new Date(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            builder.setLastDataTime(new Date(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            builder.setTimeout(randomNonNegativeLong());
        }
        AnalysisConfig.Builder analysisConfig = createAnalysisConfig();
        analysisConfig.setBucketSpan(100L);
        builder.setAnalysisConfig(analysisConfig);
        builder.setAnalysisLimits(new AnalysisLimits(randomNonNegativeLong(), randomNonNegativeLong()));
        if (randomBoolean()) {
            DataDescription.Builder dataDescription = new DataDescription.Builder();
            dataDescription.setFormat(randomFrom(DataDescription.DataFormat.values()));
            builder.setDataDescription(dataDescription);
        }
        String[] outputs;
        TransformType[] transformTypes ;
        AnalysisConfig ac = analysisConfig.build();
        if (randomBoolean()) {
            transformTypes = new TransformType[] {TransformType.TRIM, TransformType.LOWERCASE};
            outputs = new String[] {ac.getDetectors().get(0).getFieldName(), ac.getDetectors().get(0).getOverFieldName()};
        } else {
            transformTypes = new TransformType[] {TransformType.TRIM};
            outputs = new String[] {ac.getDetectors().get(0).getFieldName()};
        }
        List<TransformConfig> transformConfigList = new ArrayList<>(transformTypes.length);
        for (int i = 0; i < transformTypes.length; i++) {
            TransformConfig tc = new TransformConfig(transformTypes[i].prettyName());
            tc.setInputs(Collections.singletonList("input" + i));
            tc.setOutputs(Collections.singletonList(outputs[i]));
            transformConfigList.add(tc);
        }
        builder.setTransforms(transformConfigList);
        if (randomBoolean()) {
            builder.setModelDebugConfig(new ModelDebugConfig(randomDouble(), randomAsciiOfLength(10)));
        }
        builder.setIgnoreDowntime(randomFrom(IgnoreDowntime.values()));
        if (randomBoolean()) {
            builder.setRenormalizationWindowDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setBackgroundPersistInterval(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setModelSnapshotRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setResultsRetentionDays(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            builder.setCustomSettings(Collections.singletonMap(randomAsciiOfLength(10), randomAsciiOfLength(10)));
        }
        if (randomBoolean()) {
            builder.setModelSnapshotId(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            builder.setIndexName(randomValidJobId());
        }
        return builder.build();
    }
}
