/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.PerPartitionCategorizationConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.lessThan;

public class TransportEstimateModelMemoryActionTests extends ESTestCase {

    public void testCalculateDetectorRequirementBytes() {

        Map<String, Long> overallCardinality = new HashMap<>();
        overallCardinality.put("part", 100L);
        overallCardinality.put("buy", 200L);
        overallCardinality.put("ovr", 300L);

        String function = randomFrom("mean", "min", "max", "sum");

        Detector noSplit = createDetector(function, "field", null, null, null);
        assertThat(TransportEstimateModelMemoryAction.calculateDetectorRequirementBytes(noSplit, 900,
            overallCardinality), is(49152L));

        Detector withByField = createDetector(function, "field", "buy", null, null);
        assertThat(TransportEstimateModelMemoryAction.calculateDetectorRequirementBytes(withByField, 900,
            overallCardinality), is(134 * 49152L));

        Detector withPartitionField = createDetector(function, "field", null, null, "part");
        assertThat(TransportEstimateModelMemoryAction.calculateDetectorRequirementBytes(withPartitionField, 900,
            overallCardinality), is(100 * 49152L));

        Detector withByAndPartitionFields = createDetector(function, "field", "buy", null, "part");
        assertThat(TransportEstimateModelMemoryAction.calculateDetectorRequirementBytes(withByAndPartitionFields, 900,
            overallCardinality), is((long) Math.ceil(200 / Math.sqrt(100) * 2 / 3) * 100 * 49152L));
    }

    public void testCalculateInfluencerRequirementBytes() {

        Map<String, Long> maxBucketCardinality = new HashMap<>();
        maxBucketCardinality.put("part", 100L);
        maxBucketCardinality.put("inf1", 200L);
        maxBucketCardinality.put("inf2", 300L);

        AnalysisConfig noInfluencers = createCountAnalysisConfig(null, null);
        assertThat(TransportEstimateModelMemoryAction.calculateInfluencerRequirementBytes(noInfluencers,
            maxBucketCardinality), is(0L));

        AnalysisConfig influencerAlsoPartitionField = createCountAnalysisConfig(null, "part", "part");
        assertThat(TransportEstimateModelMemoryAction.calculateInfluencerRequirementBytes(influencerAlsoPartitionField,
            maxBucketCardinality), is(0L));

        AnalysisConfig influencerNotPartitionField = createCountAnalysisConfig(null, "part", "inf1");
        assertThat(TransportEstimateModelMemoryAction.calculateInfluencerRequirementBytes(influencerNotPartitionField,
            maxBucketCardinality), is(200 * TransportEstimateModelMemoryAction.BYTES_PER_INFLUENCER_VALUE));

        AnalysisConfig otherInfluencerAsWellAsPartitionField = createCountAnalysisConfig(null, "part", "part", "inf1");
        assertThat(TransportEstimateModelMemoryAction.calculateInfluencerRequirementBytes(otherInfluencerAsWellAsPartitionField,
            maxBucketCardinality), is(200 * TransportEstimateModelMemoryAction.BYTES_PER_INFLUENCER_VALUE));

        AnalysisConfig twoInfluencersNotPartitionField = createCountAnalysisConfig(null, "part", "part", "inf1", "inf2");
        assertThat(TransportEstimateModelMemoryAction.calculateInfluencerRequirementBytes(twoInfluencersNotPartitionField,
            maxBucketCardinality), is((200 + 300) * TransportEstimateModelMemoryAction.BYTES_PER_INFLUENCER_VALUE));
    }

    public void testCalculateCategorizationRequirementBytesNoCategorization() {

        Map<String, Long> overallCardinality = new HashMap<>();
        overallCardinality.put("part", randomLongBetween(10, 1000));

        AnalysisConfig analysisConfig = createCountAnalysisConfig(null, randomBoolean() ? "part" : null);
        assertThat(TransportEstimateModelMemoryAction.calculateCategorizationRequirementBytes(analysisConfig, overallCardinality), is(0L));
    }

    public void testCalculateCategorizationRequirementBytesSimpleCategorization() {

        Map<String, Long> overallCardinality = new HashMap<>();
        overallCardinality.put("part", randomLongBetween(10, 1000));

        AnalysisConfig analysisConfig =
            createCountAnalysisConfig(randomAlphaOfLength(10), randomBoolean() ? "part" : null);
        assertThat(TransportEstimateModelMemoryAction.calculateCategorizationRequirementBytes(analysisConfig, overallCardinality),
            is(40L * 1024 * 1024));
    }

    public void testCalculateCategorizationRequirementBytesPerPartitionCategorization() {

        long partitionCardinality = randomLongBetween(10, 1000);
        Map<String, Long> overallCardinality = new HashMap<>();
        overallCardinality.put("part", partitionCardinality);

        boolean isStopOnWarn = randomBoolean();
        AnalysisConfig analysisConfig = createCountAnalysisConfigBuilder(randomAlphaOfLength(10), "part")
            .setPerPartitionCategorizationConfig(new PerPartitionCategorizationConfig(true, isStopOnWarn)).build();
        assertThat(TransportEstimateModelMemoryAction.calculateCategorizationRequirementBytes(analysisConfig, overallCardinality),
            is(partitionCardinality * 20L * (isStopOnWarn ? 1 : 2) * 1024 * 1024));
    }

    public void testRoundUpToNextMb() {

        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(0),
            equalTo(ByteSizeValue.ofBytes(0)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(1),
            equalTo(ByteSizeValue.ofMb(1)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(randomIntBetween(1, 1024 * 1024)),
            equalTo(ByteSizeValue.ofMb(1)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(1024 * 1024),
            equalTo(ByteSizeValue.ofMb(1)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(1024 * 1024 + 1),
            equalTo(ByteSizeValue.ofMb(2)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(2 * 1024 * 1024),
            equalTo(ByteSizeValue.ofMb(2)));
        // We don't round up at the extremes, to ensure that the resulting value can be represented as bytes in a long
        // (At such extreme scale it won't be possible to actually run the analysis, so ease of use trumps precision)
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(Long.MAX_VALUE - randomIntBetween(0, 1000000)),
            equalTo(ByteSizeValue.ofMb(Long.MAX_VALUE / ByteSizeValue.ofMb(1).getBytes() )));
    }

    public void testReducedCardinality() {

        long cardinalityToReduce = randomIntBetween(1001, Integer.MAX_VALUE);
        long saneBucketSpan = randomFrom(1, 30, 60, 300, 600, 900, 1800, 3600, 10800, 21600, 43200, 86400);

        assertThat(TransportEstimateModelMemoryAction.reducedCardinality(0, randomNonNegativeLong(), saneBucketSpan),
            closeTo(0.0, 1e-15));
        assertThat(TransportEstimateModelMemoryAction.reducedCardinality(cardinalityToReduce, 1, saneBucketSpan),
            closeTo(cardinalityToReduce, 1e-6));
        assertThat(TransportEstimateModelMemoryAction.reducedCardinality(cardinalityToReduce, 1000, 900),
            closeTo(cardinalityToReduce / Math.sqrt(1000), cardinalityToReduce / 20.0));
        assertThat(TransportEstimateModelMemoryAction.reducedCardinality(
            cardinalityToReduce, randomIntBetween(2, Integer.MAX_VALUE), saneBucketSpan),
            lessThan((double) cardinalityToReduce));
        assertThat(TransportEstimateModelMemoryAction.reducedCardinality(cardinalityToReduce, 1000, 10000000),
            closeTo(cardinalityToReduce / 1000.0, 1e-4));
    }

    public void testAddNonNegativeLongsWithMaxValueCap() {

        assertThat(TransportEstimateModelMemoryAction.addNonNegativeLongsWithMaxValueCap(0, 0), is(0L));
        assertThat(TransportEstimateModelMemoryAction.addNonNegativeLongsWithMaxValueCap(0, 1), is(1L));
        assertThat(TransportEstimateModelMemoryAction.addNonNegativeLongsWithMaxValueCap(1, 0), is(1L));
        assertThat(TransportEstimateModelMemoryAction.addNonNegativeLongsWithMaxValueCap(1, 1), is(2L));
        assertThat(TransportEstimateModelMemoryAction.addNonNegativeLongsWithMaxValueCap(Long.MAX_VALUE, Long.MAX_VALUE),
            is(Long.MAX_VALUE));
        assertThat(TransportEstimateModelMemoryAction.addNonNegativeLongsWithMaxValueCap(
            Long.MAX_VALUE - randomIntBetween(1, Integer.MAX_VALUE), Long.MAX_VALUE - randomIntBetween(1, Integer.MAX_VALUE)),
            is(Long.MAX_VALUE));
    }

    public void testMultiplyNonNegativeLongsWithMaxValueCap() {

        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(0, 0), is(0L));
        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(randomNonNegativeLong(), 0), is(0L));
        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(0, randomNonNegativeLong()), is(0L));
        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(1, 1), is(1L));
        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(Long.MAX_VALUE, Long.MAX_VALUE),
            is(Long.MAX_VALUE));
        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(
            Long.MAX_VALUE, Math.max(1L, randomNonNegativeLong())),
            is(Long.MAX_VALUE));
        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(
            Math.max(1L, randomNonNegativeLong()), Long.MAX_VALUE),
            is(Long.MAX_VALUE));
        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(0, Long.MAX_VALUE), is(0L));
        assertThat(TransportEstimateModelMemoryAction.multiplyNonNegativeLongsWithMaxValueCap(Long.MAX_VALUE, 0), is(0L));
    }

    public static Detector createDetector(String function, String fieldName, String byFieldName,
                                          String overFieldName, String partitionFieldName) {

        Detector.Builder detectorBuilder = new Detector.Builder(function, fieldName);
        detectorBuilder.setByFieldName(byFieldName);
        detectorBuilder.setOverFieldName(overFieldName);
        detectorBuilder.setPartitionFieldName(partitionFieldName);
        return detectorBuilder.build();
    }

    public static AnalysisConfig createCountAnalysisConfig(String categorizationFieldName, String partitionFieldName,
                                                           String... influencerFieldNames) {
        return createCountAnalysisConfigBuilder(categorizationFieldName, partitionFieldName, influencerFieldNames).build();
    }

    public static AnalysisConfig.Builder createCountAnalysisConfigBuilder(String categorizationFieldName, String partitionFieldName,
                                                                          String... influencerFieldNames) {

        Detector.Builder detectorBuilder = new Detector.Builder("count", null);
        detectorBuilder.setByFieldName((categorizationFieldName != null) ? AnalysisConfig.ML_CATEGORY_FIELD : null);
        detectorBuilder.setPartitionFieldName(partitionFieldName);

        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(detectorBuilder.build()));

        if (categorizationFieldName != null) {
            builder.setCategorizationFieldName(categorizationFieldName);
        }

        if (influencerFieldNames.length > 0) {
            builder.setInfluencers(Arrays.asList(influencerFieldNames));
        }

        return builder;
    }
}
