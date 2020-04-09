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

        long answer = BASIC_REQUIREMENT.getBytes()
            + calculateDetectorsRequirementBytes(analysisConfig, overallCardinality)
            + calculateInfluencerRequirementBytes(analysisConfig, maxBucketCardinality)
            + calculateCategorizationRequirementBytes(analysisConfig);

        listener.onResponse(new EstimateModelMemoryAction.Response(roundUpToNextMb(answer)));
    }

    static long calculateDetectorsRequirementBytes(AnalysisConfig analysisConfig, Map<String, Long> overallCardinality) {
        return analysisConfig.getDetectors().stream().map(detector -> calculateDetectorRequirementBytes(detector, overallCardinality))
            .reduce(0L, Long::sum);
    }

    static long calculateDetectorRequirementBytes(Detector detector, Map<String, Long> overallCardinality) {

        long answer = 0;

        switch (detector.getFunction()) {
            case COUNT:
            case LOW_COUNT:
            case HIGH_COUNT:
            case NON_ZERO_COUNT:
            case LOW_NON_ZERO_COUNT:
            case HIGH_NON_ZERO_COUNT:
                answer = 1; // TODO add realistic number
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
                answer = 1; // TODO add realistic number
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
                // 64 comes from https://github.com/elastic/kibana/issues/18722
                answer = new ByteSizeValue(64, ByteSizeUnit.KB).getBytes();
                break;
            case MEDIAN:
            case LOW_MEDIAN:
            case HIGH_MEDIAN:
                answer = 1; // TODO add realistic number
                break;
            case VARP:
            case LOW_VARP:
            case HIGH_VARP:
                answer = 1; // TODO add realistic number
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
            answer *= cardinalityEstimate(Detector.BY_FIELD_NAME_FIELD.getPreferredName(), byFieldName, overallCardinality, true);
        }

        String overFieldName = detector.getOverFieldName();
        if (overFieldName != null) {
            cardinalityEstimate(Detector.OVER_FIELD_NAME_FIELD.getPreferredName(), overFieldName, overallCardinality, true);
            // TODO - how should "over" field cardinality affect estimate?
        }

        String partitionFieldName = detector.getPartitionFieldName();
        if (partitionFieldName != null) {
            answer *=
                cardinalityEstimate(Detector.PARTITION_FIELD_NAME_FIELD.getPreferredName(), partitionFieldName, overallCardinality, true);
        }

        return answer;
    }

    static long calculateInfluencerRequirementBytes(AnalysisConfig analysisConfig, Map<String, Long> maxBucketCardinality) {

        // Influencers that are also by/over/partition fields do not consume extra memory by being influencers
        Set<String> pureInfluencers = new HashSet<>(analysisConfig.getInfluencers());
        for (Detector detector : analysisConfig.getDetectors()) {
            pureInfluencers.removeAll(detector.extractAnalysisFields());
        }

        return pureInfluencers.stream()
            .map(influencer -> cardinalityEstimate(AnalysisConfig.INFLUENCERS.getPreferredName(), influencer, maxBucketCardinality, false)
                * BYTES_PER_INFLUENCER_VALUE)
            .reduce(0L, Long::sum);
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
        assert bytes >= 0;
        return new ByteSizeValue((BYTES_IN_MB - 1 + bytes) / BYTES_IN_MB, ByteSizeUnit.MB);
    }
}
