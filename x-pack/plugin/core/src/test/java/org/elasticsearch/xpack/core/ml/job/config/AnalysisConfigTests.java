/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.process.writer.RecordWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class AnalysisConfigTests extends AbstractSerializingTestCase<AnalysisConfig> {

    @Override
    protected AnalysisConfig createTestInstance() {
        return createRandomized().build();
    }

    public static AnalysisConfig.Builder createRandomized() {
        boolean isCategorization = randomBoolean();
        List<Detector> detectors = new ArrayList<>();
        int numDetectors = randomIntBetween(1, 10);
        for (int i = 0; i < numDetectors; i++) {
            Detector.Builder builder = new Detector.Builder("count", null);
            if (isCategorization) {
                builder.setByFieldName("mlcategory");
            }
            builder.setPartitionFieldName("part");
            detectors.add(builder.build());
        }
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(detectors);

        if (randomBoolean()) {
            TimeValue bucketSpan = TimeValue.timeValueSeconds(randomIntBetween(1, 1_000));
            builder.setBucketSpan(bucketSpan);

            // There is a dependency between model_prune_window and bucket_span: model_prune window must be
            // at least twice the size of bucket_span.
            builder.setModelPruneWindow(TimeValue.timeValueSeconds(randomIntBetween(2, 1_000) * bucketSpan.seconds()));

        }
        if (isCategorization) {
            builder.setCategorizationFieldName(randomAlphaOfLength(10));
            if (randomBoolean()) {
                builder.setCategorizationFilters(Arrays.asList(generateRandomStringArray(10, 10, false)));
            } else {
                CategorizationAnalyzerConfig.Builder analyzerBuilder = new CategorizationAnalyzerConfig.Builder();
                if (rarely()) {
                    analyzerBuilder.setAnalyzer(randomAlphaOfLength(10));
                } else {
                    if (randomBoolean()) {
                        for (String pattern : generateRandomStringArray(3, 40, false)) {
                            Map<String, Object> charFilter = new HashMap<>();
                            charFilter.put("type", "pattern_replace");
                            charFilter.put("pattern", pattern);
                            analyzerBuilder.addCharFilter(charFilter);
                        }
                    }

                    Map<String, Object> tokenizer = new HashMap<>();
                    tokenizer.put("type", "pattern");
                    tokenizer.put("pattern", randomAlphaOfLength(10));
                    analyzerBuilder.setTokenizer(tokenizer);

                    if (randomBoolean()) {
                        for (String pattern : generateRandomStringArray(4, 40, false)) {
                            Map<String, Object> tokenFilter = new HashMap<>();
                            tokenFilter.put("type", "pattern_replace");
                            tokenFilter.put("pattern", pattern);
                            analyzerBuilder.addTokenFilter(tokenFilter);
                        }
                    }
                }
                builder.setCategorizationAnalyzerConfig(analyzerBuilder.build());
            }
            if (randomBoolean()) {
                boolean enabled = randomBoolean();
                builder.setPerPartitionCategorizationConfig(
                    new PerPartitionCategorizationConfig(enabled, enabled && randomBoolean()));
            }
        }
        if (randomBoolean()) {
            builder.setLatency(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            builder.setMultivariateByFields(randomBoolean());
        }

        builder.setInfluencers(Arrays.asList(generateRandomStringArray(10, 10, false)));

        return builder;
    }

    @Override
    protected Writeable.Reader<AnalysisConfig> instanceReader() {
        return AnalysisConfig::new;
    }

    @Override
    protected AnalysisConfig doParseInstance(XContentParser parser) {
        return AnalysisConfig.STRICT_PARSER.apply(parser, null).build();
    }

    public void testFieldConfiguration_singleDetector_notPreSummarised() {
        // Single detector, not pre-summarised
        Detector.Builder det = new Detector.Builder("max", "responsetime");
        det.setByFieldName("airline");
        det.setPartitionFieldName("sourcetype");
        AnalysisConfig ac = createConfigWithDetectors(Collections.singletonList(det.build()));

        Set<String> termFields = new TreeSet<>(Arrays.asList("airline", "sourcetype"));
        Set<String> analysisFields = new TreeSet<>(Arrays.asList("responsetime", "airline", "sourcetype"));

        assertEquals(termFields.size(), ac.termFields().size());
        assertEquals(analysisFields.size(), ac.analysisFields().size());

        for (String s : ac.termFields()) {
            assertTrue(termFields.contains(s));
        }

        for (String s : termFields) {
            assertTrue(ac.termFields().contains(s));
        }

        for (String s : ac.analysisFields()) {
            assertTrue(analysisFields.contains(s));
        }

        for (String s : analysisFields) {
            assertTrue(ac.analysisFields().contains(s));
        }

        assertEquals(1, ac.fields().size());
        assertTrue(ac.fields().contains("responsetime"));

        assertEquals(1, ac.byFields().size());
        assertTrue(ac.byFields().contains("airline"));

        assertEquals(1, ac.partitionFields().size());
        assertTrue(ac.partitionFields().contains("sourcetype"));

        assertNull(ac.getSummaryCountFieldName());

        // Single detector, pre-summarised
        analysisFields.add("summaryCount");
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(ac);
        builder.setSummaryCountFieldName("summaryCount");
        ac = builder.build();

        for (String s : ac.analysisFields()) {
            assertTrue(analysisFields.contains(s));
        }

        for (String s : analysisFields) {
            assertTrue(ac.analysisFields().contains(s));
        }

        assertEquals("summaryCount", ac.getSummaryCountFieldName());
        assertEquals(1, ac.getDetectors().size());
        assertEquals(0, ac.getDetectors().get(0).getDetectorIndex());
    }

    public void testFieldConfiguration_multipleDetectors_NotPreSummarised() {
        // Multiple detectors, not pre-summarised
        List<Detector> detectors = new ArrayList<>();

        Detector.Builder det = new Detector.Builder("metric", "metric1");
        det.setByFieldName("by_one");
        det.setPartitionFieldName("partition_one");
        detectors.add(det.build());

        det = new Detector.Builder("metric", "metric2");
        det.setByFieldName("by_two");
        det.setOverFieldName("over_field");
        detectors.add(det.build());

        det = new Detector.Builder("metric", "metric2");
        det.setByFieldName("by_two");
        det.setPartitionFieldName("partition_two");
        detectors.add(det.build());

        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(detectors);
        builder.setInfluencers(Collections.singletonList("Influencer_Field"));
        AnalysisConfig ac = builder.build();

        Set<String> termFields = new TreeSet<>(Arrays.asList(
                "by_one", "by_two", "over_field",
                "partition_one", "partition_two", "Influencer_Field"));
        Set<String> analysisFields = new TreeSet<>(Arrays.asList(
                "metric1", "metric2", "by_one", "by_two", "over_field",
                "partition_one", "partition_two", "Influencer_Field"));

        assertEquals(termFields.size(), ac.termFields().size());
        assertEquals(analysisFields.size(), ac.analysisFields().size());

        for (String s : ac.termFields()) {
            assertTrue(s, termFields.contains(s));
        }

        for (String s : termFields) {
            assertTrue(s, ac.termFields().contains(s));
        }

        for (String s : ac.analysisFields()) {
            assertTrue(analysisFields.contains(s));
        }

        for (String s : analysisFields) {
            assertTrue(ac.analysisFields().contains(s));
        }

        assertEquals(2, ac.fields().size());
        assertTrue(ac.fields().contains("metric1"));
        assertTrue(ac.fields().contains("metric2"));

        assertEquals(2, ac.byFields().size());
        assertTrue(ac.byFields().contains("by_one"));
        assertTrue(ac.byFields().contains("by_two"));

        assertEquals(1, ac.overFields().size());
        assertTrue(ac.overFields().contains("over_field"));

        assertEquals(2, ac.partitionFields().size());
        assertTrue(ac.partitionFields().contains("partition_one"));
        assertTrue(ac.partitionFields().contains("partition_two"));

        assertNull(ac.getSummaryCountFieldName());

        assertEquals(3, ac.getDetectors().size());
        int expectedDetectorIndex = 0;
        for (Detector detector : ac.getDetectors()) {
            assertEquals(expectedDetectorIndex++, detector.getDetectorIndex());
        }
    }

    public void testBuild_GivenMlCategoryUsedAsByFieldButNoCategorizationFieldName() {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setByFieldName("mlcategory");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        ac.setCategorizationFieldName(null);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, ac::build);
        assertThat(e.getMessage(), equalTo("categorization_field_name must be set for mlcategory to be available"));
    }

    public void testBuild_GivenMlCategoryUsedAsOverFieldButNoCategorizationFieldName() {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setOverFieldName("mlcategory");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        ac.setCategorizationFieldName(null);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, ac::build);
        assertThat(e.getMessage(), equalTo("categorization_field_name must be set for mlcategory to be available"));
    }

    public void testBuild_GivenMlCategoryUsedAsPartitionFieldButNoCategorizationFieldName() {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setPartitionFieldName("mlcategory");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        ac.setCategorizationFieldName(null);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, ac::build);
        assertThat(e.getMessage(), equalTo("categorization_field_name must be set for mlcategory to be available"));
    }

    public void testBuild_GivenCategorizationFieldNameButNoUseOfMlCategory() {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setOverFieldName("foo");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        ac.setCategorizationFieldName("msg");

        ElasticsearchException e = expectThrows(ElasticsearchException.class, ac::build);
        assertThat(e.getMessage(), equalTo("categorization_field_name is set but mlcategory is " +
                "not used in any detector by/over/partition field"));
    }

    public void testBuild_GivenMlCategoryUsedAsByFieldAndCategorizationFieldName() {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setOverFieldName("mlcategory");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        ac.setCategorizationFieldName("msg");
        ac.build();
    }

    public void testBuild_GivenNestedFieldOverlapsNonNested() {
        Detector.Builder detector1 = new Detector.Builder();
        detector1.setFunction("count");
        detector1.setByFieldName("a");
        Detector.Builder detector2 = new Detector.Builder();
        detector2.setFunction("count");
        detector2.setPartitionFieldName("a.b");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(detector1.build(), detector2.build()));

        ElasticsearchException e = expectThrows(ElasticsearchException.class, ac::build);
        assertThat(e.getMessage(), equalTo("Fields [a] and [a.b] cannot both be used in the same analysis_config"));
    }

    public void testBuild_GivenOverlappingNestedFields() {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setByFieldName("a.b.c");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        ac.setInfluencers(Arrays.asList("a.b", "d"));

        ElasticsearchException e = expectThrows(ElasticsearchException.class, ac::build);
        assertThat(e.getMessage(), equalTo("Fields [a.b] and [a.b.c] cannot both be used in the same analysis_config"));
    }

    public void testBuild_GivenNonOverlappingNestedFields() {
        Detector.Builder detector = new Detector.Builder();
        detector.setFunction("count");
        detector.setByFieldName("a.b.c");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        ac.setInfluencers(Arrays.asList("a.b.c", "a.b.d"));

        ac.build();
    }

    public void testEquals_GivenSameReference() {
        AnalysisConfig config = createFullyPopulatedNonRandomConfig();
        assertTrue(config.equals(config));
    }

    public void testEquals_GivenDifferentClass() {
        assertFalse(createFullyPopulatedNonRandomConfig().equals("a string"));
    }

    public void testEquals_GivenNull() {
        assertFalse(createFullyPopulatedNonRandomConfig().equals(null));
    }

    public void testEquals_GivenEqualConfig() {
        AnalysisConfig config1 = createFullyPopulatedNonRandomConfig();
        AnalysisConfig config2 = createFullyPopulatedNonRandomConfig();

        assertTrue(config1.equals(config2));
        assertTrue(config2.equals(config1));
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    public void testEquals_GivenDifferentBucketSpan() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setBucketSpan(TimeValue.timeValueSeconds(1800));
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setBucketSpan(TimeValue.timeValueHours(1));
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenCategorizationField() {
        AnalysisConfig.Builder builder = createValidCategorizationConfig();
        builder.setCategorizationFieldName("foo");
        AnalysisConfig config1 = builder.build();

        builder = createValidCategorizationConfig();
        builder.setCategorizationFieldName("bar");
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentDetector() {
        AnalysisConfig config1 = createConfigWithDetectors(Collections.singletonList(new Detector.Builder("min", "low_count").build()));

        AnalysisConfig config2 = createConfigWithDetectors(Collections.singletonList(new Detector.Builder("min", "high_count").build()));

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentInfluencers() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setInfluencers(Collections.singletonList("foo"));
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setInfluencers(Collections.singletonList("bar"));
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentLatency() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setLatency(TimeValue.timeValueSeconds(1800));
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setLatency(TimeValue.timeValueSeconds(1801));
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentModelPruneWindow() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setModelPruneWindow(TimeValue.timeValueDays(14));
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setModelPruneWindow(TimeValue.timeValueDays(28));
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenSummaryCountField() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setSummaryCountFieldName("foo");
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setSummaryCountFieldName("bar");
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenMultivariateByField() {
        AnalysisConfig.Builder builder = createConfigBuilder();
        builder.setMultivariateByFields(true);
        AnalysisConfig config1 = builder.build();

        builder = createConfigBuilder();
        builder.setMultivariateByFields(false);
        AnalysisConfig config2 = builder.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testEquals_GivenDifferentCategorizationFilters() {
        AnalysisConfig.Builder configBuilder1 = createValidCategorizationConfig();
        AnalysisConfig.Builder configBuilder2 = createValidCategorizationConfig();
        configBuilder1.setCategorizationFilters(Arrays.asList("foo", "bar"));
        configBuilder2.setCategorizationFilters(Arrays.asList("foo", "foobar"));
        AnalysisConfig config1 = configBuilder1.build();
        AnalysisConfig config2 = configBuilder2.build();

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testExtractReferencedFilters() {
        DetectionRule rule1 = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "filter1")).build();
        DetectionRule rule2 = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "filter2")).build();
        Detector.Builder detector1 = new Detector.Builder("count", null);
        detector1.setByFieldName("foo");
        detector1.setRules(Collections.singletonList(rule1));
        Detector.Builder detector2 = new Detector.Builder("count", null);
        detector2.setRules(Collections.singletonList(rule2));
        detector2.setByFieldName("foo");
        AnalysisConfig config = new AnalysisConfig.Builder(
                Arrays.asList(detector1.build(), detector2.build(), new Detector.Builder("count", null).build())).build();

        assertEquals(new HashSet<>(Arrays.asList("filter1", "filter2")), config.extractReferencedFilters());
    }

    private static AnalysisConfig createFullyPopulatedNonRandomConfig() {
        Detector.Builder detector = new Detector.Builder("min", "count");
        detector.setOverFieldName("mlcategory");
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(
                Collections.singletonList(detector.build()));
        builder.setBucketSpan(TimeValue.timeValueHours(1));
        builder.setCategorizationFieldName("cat");
        builder.setCategorizationAnalyzerConfig(
                CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(Collections.singletonList("foo")));
        builder.setInfluencers(Collections.singletonList("myInfluencer"));
        builder.setLatency(TimeValue.timeValueSeconds(3600));
        builder.setModelPruneWindow(TimeValue.timeValueDays(30));
        builder.setSummaryCountFieldName("sumCount");
        return builder.build();
    }

    private static AnalysisConfig createConfigWithDetectors(List<Detector> detectors) {
        return new AnalysisConfig.Builder(detectors).build();
    }

    private static AnalysisConfig.Builder createConfigBuilder() {
        return new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("min", "count").build()));
    }

    public void testVerify_GivenNegativeBucketSpan() {
        AnalysisConfig.Builder config = createValidConfig();
        config.setBucketSpan(TimeValue.timeValueSeconds(-1));

        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, config::build);

        assertEquals("bucket_span cannot be less or equal than 0. Value = -1", e.getMessage());
    }

    public void testVerify_GivenNegativeLatency() {
        AnalysisConfig.Builder analysisConfig = createValidConfig();
        analysisConfig.setLatency(TimeValue.timeValueSeconds(-1));

        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, analysisConfig::build);

        assertEquals("latency cannot be less than 0. Value = -1", e.getMessage());
    }

    public void testVerify_GivenDefaultConfig_ShouldBeInvalidDueToNoDetectors() {
        AnalysisConfig.Builder analysisConfig = createValidConfig();
        analysisConfig.setDetectors(null);

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, analysisConfig::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_NO_DETECTORS), e.getMessage());
    }

    public void testVerify_GivenValidConfig() {
        AnalysisConfig.Builder analysisConfig = createValidConfig();
        analysisConfig.build();
    }

    public void testVerify_GivenValidConfigWithCategorizationFieldNameAndCategorizationFilters() {
        AnalysisConfig.Builder analysisConfig = createValidCategorizationConfig();
        analysisConfig.setCategorizationFilters(Arrays.asList("foo", "bar"));

        analysisConfig.build();
    }

    public void testVerify_GivenValidConfigWithCategorizationFieldNameAndCategorizationAnalyzerConfig() {
        AnalysisConfig.Builder analysisConfig = createValidCategorizationConfig();
        analysisConfig.setCategorizationAnalyzerConfig(
                CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(Arrays.asList("foo", "bar")));

        analysisConfig.build();
    }

    public void testVerify_GivenBothCategorizationFiltersAndCategorizationAnalyzerConfig() {
        AnalysisConfig.Builder analysisConfig = createValidCategorizationConfig();
        analysisConfig.setCategorizationFilters(Arrays.asList("foo", "bar"));
        analysisConfig.setCategorizationAnalyzerConfig(
                CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(Collections.singletonList("baz")));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, analysisConfig::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_INCOMPATIBLE_WITH_CATEGORIZATION_ANALYZER),
                e.getMessage());
    }

    public void testVerify_GivenFieldIsControlField() {
        AnalysisConfig.Builder analysisConfig = createValidConfig();
        if (randomBoolean()) {
            analysisConfig.setSummaryCountFieldName(RecordWriter.CONTROL_FIELD_NAME);
        } else {
            analysisConfig.setCategorizationFieldName(RecordWriter.CONTROL_FIELD_NAME);
        }

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, analysisConfig::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_INVALID_FIELDNAME, RecordWriter.CONTROL_FIELD_NAME,
                RecordWriter.CONTROL_FIELD_NAME), e.getMessage());
    }

    public void testVerify_GivenMetricAndSummaryCountField() {
        Detector d = new Detector.Builder("metric", "my_metric").build();
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d));
        ac.setSummaryCountFieldName("my_summary_count");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, ac::build);
        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_FUNCTION_INCOMPATIBLE_PRESUMMARIZED, DetectorFunction.METRIC), e.getMessage());
    }

    public void testVerify_GivenCategorizationFiltersButNoCategorizationFieldName() {
        AnalysisConfig.Builder config = createValidConfig();
        config.setCategorizationFilters(Collections.singletonList("foo"));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, config::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_REQUIRE_CATEGORIZATION_FIELD_NAME), e.getMessage());
    }

    public void testVerify_GivenDuplicateCategorizationFilters() {
        AnalysisConfig.Builder config = createValidCategorizationConfig();
        config.setCategorizationFilters(Arrays.asList("foo", "bar", "foo"));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, config::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_DUPLICATES), e.getMessage());
    }

    public void testVerify_GivenEmptyCategorizationFilter() {
        AnalysisConfig.Builder config = createValidCategorizationConfig();
        config.setCategorizationFilters(Arrays.asList("foo", ""));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, config::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_EMPTY), e.getMessage());
    }

    public void testCheckDetectorsHavePartitionFields_doesntThrowWhenValid() {
        AnalysisConfig.Builder config = createValidConfig();
        Detector.Builder builder = new Detector.Builder(config.build().getDetectors().get(0));
        builder.setPartitionFieldName("pField");
        config.build().getDetectors().set(0, builder.build());

        config.build();
    }

    public void testVerify_GivenCategorizationFiltersContainInvalidRegex() {
        AnalysisConfig.Builder config = createValidCategorizationConfig();
        config.setCategorizationFilters(Arrays.asList("foo", "("));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, config::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_INVALID_REGEX, "("), e.getMessage());
    }

    public void testVerify_GivenPerPartitionCategorizationAndNoPartitions() {
        AnalysisConfig.Builder analysisConfig = createValidCategorizationConfig();
        analysisConfig.setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig(true, randomBoolean()));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, analysisConfig::build);

        assertEquals(
            "partition_field_name must be set for detectors that reference mlcategory when per-partition categorization is enabled",
            e.getMessage());
    }

    public void testVerify_GivenPerPartitionCategorizationAndMultiplePartitionFields() {

        List<Detector> detectors = new ArrayList<>();
        for (String partitionFieldValue : Arrays.asList("part1", "part2")) {
            Detector.Builder detector = new Detector.Builder("count", null);
            detector.setByFieldName("mlcategory");
            detector.setPartitionFieldName(partitionFieldValue);
            detectors.add(detector.build());
        }
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(detectors);
        analysisConfig.setCategorizationFieldName("msg");
        analysisConfig.setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig(true, randomBoolean()));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, analysisConfig::build);

        assertEquals(
            "partition_field_name cannot vary between detectors when per-partition categorization is enabled: [part1] and [part2] are used",
            e.getMessage());
    }

    public void testVerify_GivenPerPartitionCategorizationAndNoPartitionFieldOnCategorizationDetector() {

        List<Detector> detectors = new ArrayList<>();
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("mlcategory");
        detectors.add(detector.build());
        detector = new Detector.Builder("mean", "responsetime");
        detector.setPartitionFieldName("airline");
        detectors.add(detector.build());
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(detectors);
        analysisConfig.setCategorizationFieldName("msg");
        analysisConfig.setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig(true, randomBoolean()));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, analysisConfig::build);

        assertEquals(
            "partition_field_name must be set for detectors that reference mlcategory when per-partition categorization is enabled",
            e.getMessage());
    }

    public void testVerify_GivenComplexPerPartitionCategorizationConfig() {

        List<Detector> detectors = new ArrayList<>();
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("mlcategory");
        detector.setPartitionFieldName("event.dataset");
        detectors.add(detector.build());
        detector = new Detector.Builder("mean", "responsetime");
        detector.setByFieldName("airline");
        detectors.add(detector.build());
        detector = new Detector.Builder("rare", null);
        detector.setByFieldName("mlcategory");
        detector.setPartitionFieldName("event.dataset");
        detectors.add(detector.build());
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(detectors);
        analysisConfig.setCategorizationFieldName("msg");
        boolean stopOnWarn = randomBoolean();
        analysisConfig.setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig(true, stopOnWarn));

        assertThat(analysisConfig.build().getPerPartitionCategorizationConfig().isStopOnWarn(), is(stopOnWarn));
    }

    private static AnalysisConfig.Builder createValidConfig() {
        List<Detector> detectors = new ArrayList<>();
        Detector detector = new Detector.Builder("count", null).build();
        detectors.add(detector);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(detectors);
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        analysisConfig.setLatency(TimeValue.ZERO);
        analysisConfig.setModelPruneWindow(TimeValue.timeValueHours(3));
        return analysisConfig;
    }

    private static AnalysisConfig.Builder createValidCategorizationConfig() {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("mlcategory");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        analysisConfig.setLatency(TimeValue.ZERO);
        analysisConfig.setModelPruneWindow(TimeValue.timeValueHours(3));
        analysisConfig.setCategorizationFieldName("msg");
        return analysisConfig;
    }

    @Override
    protected AnalysisConfig mutateInstance(AnalysisConfig instance) {
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(instance);
        switch (between(0, 8)) {
        case 0:
            List<Detector> detectors = new ArrayList<>(instance.getDetectors());
            Detector.Builder detector = new Detector.Builder();
            detector.setFunction("mean");
            detector.setFieldName(randomAlphaOfLengthBetween(10, 20));
            detectors.add(detector.build());
            builder.setDetectors(detectors);
            break;
        case 1:
            TimeValue bucketSpan = new TimeValue(instance.getBucketSpan().millis() + (between(1, 1000) * 1000));
            builder.setBucketSpan(bucketSpan);

            // There is a dependency between model_prune_window and bucket_span: model_prune window must be
            // at least twice the size of bucket_span.
            builder.setModelPruneWindow(new TimeValue(between(2, 1000) * bucketSpan.millis()));
            break;
        case 2:
            if (instance.getLatency() == null) {
                builder.setLatency(new TimeValue(between(1, 1000) * 1000));
            } else {
                builder.setLatency(new TimeValue(instance.getLatency().millis() + (between(1, 1000) * 1000)));
            }
            break;
        case 3:
            if (instance.getCategorizationFieldName() == null) {
                String categorizationFieldName = instance.getCategorizationFieldName() + randomAlphaOfLengthBetween(1, 10);
                builder.setCategorizationFieldName(categorizationFieldName);
                List<Detector> newDetectors = new ArrayList<>(instance.getDetectors());
                Detector.Builder catDetector = new Detector.Builder();
                catDetector.setFunction("mean");
                catDetector.setFieldName(randomAlphaOfLengthBetween(10, 20));
                catDetector.setPartitionFieldName("mlcategory");
                newDetectors.add(catDetector.build());
                builder.setDetectors(newDetectors);
            } else {
                builder.setCategorizationFieldName(instance.getCategorizationFieldName() + randomAlphaOfLengthBetween(1, 10));
            }
            break;
        case 4:
            List<String> filters;
            if (instance.getCategorizationFilters() == null) {
                filters = new ArrayList<>();
            } else {
                filters = new ArrayList<>(instance.getCategorizationFilters());
            }
            filters.add(randomAlphaOfLengthBetween(1, 20));
            builder.setCategorizationFilters(filters);
            builder.setCategorizationAnalyzerConfig(null);
            if (instance.getCategorizationFieldName() == null) {
                builder.setCategorizationFieldName(randomAlphaOfLengthBetween(1, 10));
                List<Detector> newDetectors = new ArrayList<>(instance.getDetectors());
                Detector.Builder catDetector = new Detector.Builder();
                catDetector.setFunction("mean");
                catDetector.setFieldName(randomAlphaOfLengthBetween(10, 20));
                catDetector.setPartitionFieldName("mlcategory");
                newDetectors.add(catDetector.build());
                builder.setDetectors(newDetectors);
            }
            break;
        case 5:
            builder.setCategorizationFilters(null);
            builder.setCategorizationAnalyzerConfig(CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(
                    Collections.singletonList(randomAlphaOfLengthBetween(1, 20))));
            if (instance.getCategorizationFieldName() == null) {
                builder.setCategorizationFieldName(randomAlphaOfLengthBetween(1, 10));
                List<Detector> newDetectors = new ArrayList<>(instance.getDetectors());
                Detector.Builder catDetector = new Detector.Builder();
                catDetector.setFunction("count");
                catDetector.setByFieldName("mlcategory");
                newDetectors.add(catDetector.build());
                builder.setDetectors(newDetectors);
            }
            break;
        case 6:
            builder.setSummaryCountFieldName(instance.getSummaryCountFieldName() + randomAlphaOfLengthBetween(1, 5));
            break;
        case 7:
            builder.setInfluencers(CollectionUtils.appendToCopy(instance.getInfluencers(), randomAlphaOfLengthBetween(5, 10)));
            break;
        case 8:
            if (instance.getMultivariateByFields() == null) {
                builder.setMultivariateByFields(randomBoolean());
            } else {
                builder.setMultivariateByFields(instance.getMultivariateByFields() == false);
            }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return builder.build();
    }
}
