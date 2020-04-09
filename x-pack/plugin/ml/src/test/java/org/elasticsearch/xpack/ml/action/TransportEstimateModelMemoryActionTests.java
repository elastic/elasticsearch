/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Detector;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportEstimateModelMemoryActionTests extends ESTestCase {

    public void testCalculateDetectorRequirementBytes() {

        Map<String, Long> overallCardinality = new HashMap<>();
        overallCardinality.put("part", 100L);
        overallCardinality.put("buy", 200L);
        overallCardinality.put("ovr", 300L);

        String function = randomFrom("mean", "min", "max", "sum");

        Detector noSplit = createDetector(function, "field", null, null, null);
        assertThat(TransportEstimateModelMemoryAction.calculateDetectorRequirementBytes(noSplit,
            overallCardinality), is(65536L));

        Detector withByField = createDetector(function, "field", "buy", null, null);
        assertThat(TransportEstimateModelMemoryAction.calculateDetectorRequirementBytes(withByField,
            overallCardinality), is(200 * 65536L));

        Detector withPartitionField = createDetector(function, "field", null, null, "part");
        assertThat(TransportEstimateModelMemoryAction.calculateDetectorRequirementBytes(withPartitionField,
            overallCardinality), is(100 * 65536L));

        Detector withByAndPartitionFields = createDetector(function, "field", "buy", null, "part");
        assertThat(TransportEstimateModelMemoryAction.calculateDetectorRequirementBytes(withByAndPartitionFields,
            overallCardinality), is(200 * 100 * 65536L));
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

    public void testCalculateCategorizationRequirementBytes() {

        AnalysisConfig analysisConfigWithoutCategorization = createCountAnalysisConfig(null, null);
        assertThat(TransportEstimateModelMemoryAction.calculateCategorizationRequirementBytes(analysisConfigWithoutCategorization), is(0L));

        AnalysisConfig analysisConfigWithCategorization = createCountAnalysisConfig(randomAlphaOfLength(10), null);
        assertThat(TransportEstimateModelMemoryAction.calculateCategorizationRequirementBytes(analysisConfigWithCategorization), is(1L));
    }

    public void testRoundUpToNextMb() {

        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(0),
            equalTo(new ByteSizeValue(0, ByteSizeUnit.BYTES)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(1),
            equalTo(new ByteSizeValue(1, ByteSizeUnit.MB)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(randomIntBetween(1, 1024 * 1024)),
            equalTo(new ByteSizeValue(1, ByteSizeUnit.MB)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(1024 * 1024),
            equalTo(new ByteSizeValue(1, ByteSizeUnit.MB)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(1024 * 1024 + 1),
            equalTo(new ByteSizeValue(2, ByteSizeUnit.MB)));
        assertThat(TransportEstimateModelMemoryAction.roundUpToNextMb(2 * 1024 * 1024),
            equalTo(new ByteSizeValue(2, ByteSizeUnit.MB)));
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

        Detector.Builder detectorBuilder = new Detector.Builder("count", null);
        detectorBuilder.setPartitionFieldName((categorizationFieldName != null) ? AnalysisConfig.ML_CATEGORY_FIELD : partitionFieldName);

        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(detectorBuilder.build()));

        if (categorizationFieldName != null) {
            builder.setCategorizationFieldName(categorizationFieldName);
        }

        if (influencerFieldNames.length > 0) {
            builder.setInfluencers(Arrays.asList(influencerFieldNames));
        }

        return builder.build();
    }
}
