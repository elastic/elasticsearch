/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger logger = LogManager.getLogger(TransportEstimateModelMemoryAction.class);

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
        answer = addNonNegativeLongsWithMaxValueCap(answer, calculateCategorizationRequirementBytes(analysisConfig));

        listener.onResponse(new EstimateModelMemoryAction.Response(roundUpToNextMb(answer)));
    }

    static long calculateDetectorsRequirementBytes(AnalysisConfig analysisConfig, Map<String, Long> overallCardinality) {
        return analysisConfig.getDetectors().stream().map(detector -> calculateDetectorRequirementBytes(detector, overallCardinality))
            .reduce(0L, TransportEstimateModelMemoryAction::addNonNegativeLongsWithMaxValueCap);
    }

    static long calculateDetectorRequirementBytes(Detector detector, Map<String, Long> overallCardinality) {

        long answer = 0;

        // These values for detectors assume splitting is via a partition field
        switch (detector.getFunction()) {
            case COUNT:
            case LOW_COUNT:
            case HIGH_COUNT:
            case NON_ZERO_COUNT:
            case LOW_NON_ZERO_COUNT:
            case HIGH_NON_ZERO_COUNT:
                answer = new ByteSizeValue(32, ByteSizeUnit.KB).getBytes();
                break;
            case DISTINCT_COUNT:
            case LOW_DISTINCT_COUNT:
            case HIGH_DISTINCT_COUNT:
                answer = 1; // TODO add realistic number
                break;
            case RARE:
            case FREQ_RARE:
                answer = 1; // TODO add realistic number
                break;
            case INFO_CONTENT:
            case LOW_INFO_CONTENT:
            case HIGH_INFO_CONTENT:
                answer = 1; // TODO add realistic number
                break;
            case METRIC:
                // metric analyses mean, min and max simultaneously, and uses about 2.5 times the memory of one of these
                answer = new ByteSizeValue(160, ByteSizeUnit.KB).getBytes();
                break;
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
            case MEDIAN:
            case LOW_MEDIAN:
            case HIGH_MEDIAN:
            case VARP:
            case LOW_VARP:
            case HIGH_VARP:
                // 64 comes from https://github.com/elastic/kibana/issues/18722
                answer = new ByteSizeValue(64, ByteSizeUnit.KB).getBytes();
                break;
            case TIME_OF_DAY:
            case TIME_OF_WEEK:
                answer = 1; // TODO add realistic number
                break;
            case LAT_LONG:
                answer = 1; // TODO add realistic number
                break;
            default:
                assert false : "unhandled detector function: " + detector.getFunction().getFullName();
        }

        String byFieldName = detector.getByFieldName();
        if (byFieldName != null) {
            long cardinalityEstimate =
                cardinalityEstimate(Detector.BY_FIELD_NAME_FIELD.getPreferredName(), byFieldName, overallCardinality, true);
            // The memory cost of a by field is about 2/3rds that of a partition field
            long multiplier = addNonNegativeLongsWithMaxValueCap(cardinalityEstimate, 2) / 3 * 2;
            answer = multiplyNonNegativeLongsWithMaxValueCap(answer, multiplier);
        }

        String overFieldName = detector.getOverFieldName();
        if (overFieldName != null) {
            long cardinalityEstimate =
                cardinalityEstimate(Detector.OVER_FIELD_NAME_FIELD.getPreferredName(), overFieldName, overallCardinality, true);
            // Over fields don't multiply the whole estimate, just add a small amount (estimate 512 bytes) per value
            answer = addNonNegativeLongsWithMaxValueCap(answer, multiplyNonNegativeLongsWithMaxValueCap(cardinalityEstimate, 512));
        }

        String partitionFieldName = detector.getPartitionFieldName();
        if (partitionFieldName != null) {
            long multiplier =
                cardinalityEstimate(Detector.PARTITION_FIELD_NAME_FIELD.getPreferredName(), partitionFieldName, overallCardinality, true);
            answer = multiplyNonNegativeLongsWithMaxValueCap(answer, multiplier);
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

    static long calculateCategorizationRequirementBytes(AnalysisConfig analysisConfig) {

        if (analysisConfig.getCategorizationFieldName() != null) {
            return 1; // TODO add realistic number
        } else {
            return 0;
        }
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
        logger.warn("[{}] cardinality estimate required for [{}] [{}] but not supplied",
            isOverall ? "Overall" : "Bucket max", description, fieldName);
        return 0;
    }

    static ByteSizeValue roundUpToNextMb(long bytes) {
        assert bytes >= 0 : "negative bytes " + bytes;
        return new ByteSizeValue(addNonNegativeLongsWithMaxValueCap(bytes, BYTES_IN_MB - 1) / BYTES_IN_MB, ByteSizeUnit.MB);
    }

    private static long addNonNegativeLongsWithMaxValueCap(long a, long b) {
        assert a >= 0;
        assert b >= 0;
        if (Long.MAX_VALUE - a - b < 0) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    private static long multiplyNonNegativeLongsWithMaxValueCap(long a, long b) {
        assert a >= 0;
        assert b >= 0;
        if (Long.MAX_VALUE / a < b) {
            return Long.MAX_VALUE;
        }
        return a * b;
    }
}
