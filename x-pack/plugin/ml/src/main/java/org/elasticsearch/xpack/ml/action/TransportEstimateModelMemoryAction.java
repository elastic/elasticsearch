/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.EstimateModelMemoryAction;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Detector;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Calculates the estimated model memory requirement of an anomaly detection job
 * from its analysis config and estimates of the cardinality of the various fields
 * referenced in it.
 *
 * Answers are capped at <code>Long.MAX_VALUE</code> bytes, to avoid returning
 * values with bigger units that cannot trivially be converted back to bytes.
 * (In reality if the memory estimate is greater than <code>Long.MAX_VALUE</code>
 * bytes then the job will be impossible to run successfully, so this is not a
 * major limitation.)
 */
public class TransportEstimateModelMemoryAction
    extends HandledTransportAction<EstimateModelMemoryAction.Request, EstimateModelMemoryAction.Response> {

    static final ByteSizeValue BASIC_REQUIREMENT = new ByteSizeValue(10, ByteSizeUnit.MB);
    static final long BYTES_PER_INFLUENCER_VALUE = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();
    private static final long BYTES_IN_MB = new ByteSizeValue(1, ByteSizeUnit.MB).getBytes();

    @Inject
    public TransportEstimateModelMemoryAction(TransportService transportService,
                                              ActionFilters actionFilters) {
        super(EstimateModelMemoryAction.NAME, transportService, actionFilters, EstimateModelMemoryAction.Request::new);
    }

    @Override
    protected void doExecute(Task task,
                             EstimateModelMemoryAction.Request request,
                             ActionListener<EstimateModelMemoryAction.Response> listener) {

        AnalysisConfig analysisConfig = request.getAnalysisConfig();
        Map<String, Long> overallCardinality = request.getOverallCardinality();
        Map<String, Long> maxBucketCardinality = request.getMaxBucketCardinality();

        long answer = BASIC_REQUIREMENT.getBytes();
        answer = addNonNegativeLongsWithMaxValueCap(answer, calculateDetectorsRequirementBytes(analysisConfig, overallCardinality));
        answer = addNonNegativeLongsWithMaxValueCap(answer, calculateInfluencerRequirementBytes(analysisConfig, maxBucketCardinality));
        answer = addNonNegativeLongsWithMaxValueCap(answer, calculateCategorizationRequirementBytes(analysisConfig, overallCardinality));

        listener.onResponse(new EstimateModelMemoryAction.Response(roundUpToNextMb(answer)));
    }

    static long calculateDetectorsRequirementBytes(AnalysisConfig analysisConfig, Map<String, Long> overallCardinality) {
        long bucketSpanSeconds = analysisConfig.getBucketSpan().getSeconds();
        return analysisConfig.getDetectors().stream()
            .map(detector -> calculateDetectorRequirementBytes(detector, bucketSpanSeconds, overallCardinality))
            .reduce(0L, TransportEstimateModelMemoryAction::addNonNegativeLongsWithMaxValueCap);
    }

    @SuppressWarnings("fallthrough")
    static long calculateDetectorRequirementBytes(Detector detector, long bucketSpanSeconds, Map<String, Long> overallCardinality) {

        long answer = 0;
        boolean addFieldValueWorkspace = false;

        // These values for detectors assume splitting is via a partition field
        switch (detector.getFunction()) {
            case DISTINCT_COUNT:
            case LOW_DISTINCT_COUNT:
            case HIGH_DISTINCT_COUNT:
                addFieldValueWorkspace = true;
            case COUNT:
            case LOW_COUNT:
            case HIGH_COUNT:
            case NON_ZERO_COUNT:
            case LOW_NON_ZERO_COUNT:
            case HIGH_NON_ZERO_COUNT:
                answer = new ByteSizeValue(32, ByteSizeUnit.KB).getBytes();
                break;
            case RARE:
            case FREQ_RARE:
                answer = new ByteSizeValue(2, ByteSizeUnit.KB).getBytes();
                break;
            case INFO_CONTENT:
            case LOW_INFO_CONTENT:
            case HIGH_INFO_CONTENT:
                addFieldValueWorkspace = true;
            case MEAN:
            case LOW_MEAN:
            case HIGH_MEAN:
            case AVG:
            case LOW_AVG:
            case HIGH_AVG:
            case MIN:
            case MAX:
            case SUM:
            case LOW_SUM:
            case HIGH_SUM:
            case NON_NULL_SUM:
            case LOW_NON_NULL_SUM:
            case HIGH_NON_NULL_SUM:
            case VARP:
            case LOW_VARP:
            case HIGH_VARP:
                answer = new ByteSizeValue(48, ByteSizeUnit.KB).getBytes();
                break;
            case METRIC:
                // metric analyses mean, min and max simultaneously, and uses about 2.5 times the memory of one of these
                answer = new ByteSizeValue(120, ByteSizeUnit.KB).getBytes();
                break;
            case MEDIAN:
            case LOW_MEDIAN:
            case HIGH_MEDIAN:
                answer = new ByteSizeValue(64, ByteSizeUnit.KB).getBytes();
                break;
            case TIME_OF_DAY:
            case TIME_OF_WEEK:
                answer = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();
                break;
            case LAT_LONG:
                answer = new ByteSizeValue(64, ByteSizeUnit.KB).getBytes();
                break;
            default:
                assert false : "unhandled detector function: " + detector.getFunction().getFullName();
        }

        long partitionFieldCardinalityEstimate = 1;
        String partitionFieldName = detector.getPartitionFieldName();
        if (partitionFieldName != null) {
            partitionFieldCardinalityEstimate = Math.max(1,
                cardinalityEstimate(Detector.PARTITION_FIELD_NAME_FIELD.getPreferredName(), partitionFieldName, overallCardinality, true));
        }

        String byFieldName = detector.getByFieldName();
        if (byFieldName != null) {
            long byFieldCardinalityEstimate =
                cardinalityEstimate(Detector.BY_FIELD_NAME_FIELD.getPreferredName(), byFieldName, overallCardinality, true);
            // Assume the number of by field values in each partition reduces if the cardinality of both by and partition fields is high
            // The memory cost of a by field is about 2/3rds that of a partition field
            double multiplier =
                Math.ceil(reducedCardinality(byFieldCardinalityEstimate, partitionFieldCardinalityEstimate, bucketSpanSeconds) * 2.0 / 3.0);
            answer = multiplyNonNegativeLongsWithMaxValueCap(answer, (long) multiplier);
        }

        String overFieldName = detector.getOverFieldName();
        if (overFieldName != null) {
            long overFieldCardinalityEstimate =
                cardinalityEstimate(Detector.OVER_FIELD_NAME_FIELD.getPreferredName(), overFieldName, overallCardinality, true);
            // Assume the number of over field values in each partition reduces if the cardinality of both over and partition fields is high
            double multiplier =
                Math.ceil(reducedCardinality(overFieldCardinalityEstimate, partitionFieldCardinalityEstimate, bucketSpanSeconds));
            // Over fields don't multiply the whole estimate, just add a small amount (estimate 768 bytes) per value
            answer = addNonNegativeLongsWithMaxValueCap(answer, multiplyNonNegativeLongsWithMaxValueCap(768, (long) multiplier));
        }

        if (partitionFieldName != null) {
            answer = multiplyNonNegativeLongsWithMaxValueCap(answer, partitionFieldCardinalityEstimate);
        }

        if (addFieldValueWorkspace) {
            // The field value workspace should really be the maximum over all buckets of the
            // length of all the distinct values of the function field concatenated in the bucket.
            // However, that would be very expensive and complex for the caller to calculate so
            // we just allow a fixed amount.
            answer = addNonNegativeLongsWithMaxValueCap(answer, new ByteSizeValue(5, ByteSizeUnit.MB).getBytes());
        }

        return answer;
    }

    static long calculateInfluencerRequirementBytes(AnalysisConfig analysisConfig, Map<String, Long> maxBucketCardinality) {

        // Influencers that are also by/over/partition fields do not consume extra memory by being influencers
        Set<String> pureInfluencers = new HashSet<>(analysisConfig.getInfluencers());
        for (Detector detector : analysisConfig.getDetectors()) {
            pureInfluencers.removeAll(detector.extractAnalysisFields());
        }

        long totalInfluencerCardinality = pureInfluencers.stream()
            .map(influencer -> cardinalityEstimate(AnalysisConfig.INFLUENCERS.getPreferredName(), influencer, maxBucketCardinality, false))
            .reduce(0L, TransportEstimateModelMemoryAction::addNonNegativeLongsWithMaxValueCap);
        return multiplyNonNegativeLongsWithMaxValueCap(BYTES_PER_INFLUENCER_VALUE, totalInfluencerCardinality);
    }

    static long calculateCategorizationRequirementBytes(AnalysisConfig analysisConfig, Map<String, Long> overallCardinality) {

        if (analysisConfig.getCategorizationFieldName() == null) {
            return 0;
        }

        long relevantPartitionFieldCardinalityEstimate = 1;
        if (analysisConfig.getPerPartitionCategorizationConfig().isEnabled()) {
            // It is enforced that only one partition field name be configured when per-partition categorization
            // is enabled, so we can stop after finding a non-null partition field name
            for (Detector detector : analysisConfig.getDetectors()) {
                String partitionFieldName = detector.getPartitionFieldName();
                if (partitionFieldName != null) {
                    relevantPartitionFieldCardinalityEstimate = Math.max(1, cardinalityEstimate(
                        Detector.PARTITION_FIELD_NAME_FIELD.getPreferredName(), partitionFieldName, overallCardinality, true));
                    break;
                }
            }
        }

        // 5MB is a pretty conservative estimate of the memory requirement for categorization.
        // Often it is considerably less, but it's very hard to predict from simple statistics.
        return new ByteSizeValue(5 * relevantPartitionFieldCardinalityEstimate, ByteSizeUnit.MB).getBytes();
    }

    static long cardinalityEstimate(String description, String fieldName, Map<String, Long> suppliedCardinailityEstimates,
                                    boolean isOverall) {
        Long suppliedEstimate = suppliedCardinailityEstimates.get(fieldName);
        if (suppliedEstimate != null) {
            return suppliedEstimate;
        }
        // Don't expect the user to supply cardinality estimates for the mlcategory field that we create ourselves
        if (AnalysisConfig.ML_CATEGORY_FIELD.equals(fieldName)) {
            return isOverall ? 500 : 50;
        }
        throw new IllegalArgumentException("[" + (isOverall ? "Overall" : "Bucket max") + "] cardinality estimate required for [" +
            description + "] [" + fieldName + "] but not supplied");
    }

    static ByteSizeValue roundUpToNextMb(long bytes) {
        assert bytes >= 0 : "negative bytes " + bytes;
        return new ByteSizeValue(addNonNegativeLongsWithMaxValueCap(bytes, BYTES_IN_MB - 1) / BYTES_IN_MB, ByteSizeUnit.MB);
    }

    /**
     * The idea here is to reduce a by or over field cardinality to reflect the likelihood that only a subset of by or
     * over field values will exist in any partition in a given bucket.  The principles are:
     * 1. The greater the partition field cardinality, the greater the reduction
     * 2. The shorter the bucket span, the greater the reduction
     * A partition field cardinality of 1 means no reduction.  (And remember usenull is effectively always true for partition
     * fields, so there will be at least one partition even if the partition field doesn't exist in any input documents.)
     * A bucket span of 15 minutes means the cardinality to be reduced is divided by approximately the square root of the
     * smaller of the two cardinalities.
     */
    static double reducedCardinality(long cardinalityToReduce, long partitionFieldCardinalityEstimate, long bucketSpanSeconds) {
        assert cardinalityToReduce >= 0 : "negative cardinality to reduce " + cardinalityToReduce;
        assert partitionFieldCardinalityEstimate > 0 : "non-positive partition field cardinality " + partitionFieldCardinalityEstimate;
        assert bucketSpanSeconds > 0 : "non-positive bucket span " + bucketSpanSeconds;
        if (cardinalityToReduce == 0) {
            return 0;
        }
        double power = Math.min(1.0, (Math.log10(bucketSpanSeconds) + 1.0) / 8.0);
        return cardinalityToReduce / Math.pow(Math.min(cardinalityToReduce, partitionFieldCardinalityEstimate), power);
    }

    static long addNonNegativeLongsWithMaxValueCap(long a, long b) {
        assert a >= 0;
        assert b >= 0;
        if (Long.MAX_VALUE - a - b < 0) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    static long multiplyNonNegativeLongsWithMaxValueCap(long a, long b) {
        assert a >= 0;
        assert b >= 0;
        if (a == 0 || b == 0) {
            return 0;
        }
        if (Long.MAX_VALUE / a < b) {
            return Long.MAX_VALUE;
        }
        return a * b;
    }
}
