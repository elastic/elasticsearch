/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Map;

public final class DatafeedJobValidator {

    private DatafeedJobValidator() {}

    /**
     * Validates a datafeedConfig in relation to the job it refers to
     * @param datafeedConfig the datafeed config
     * @param job the job
     * @param xContentRegistry the named xcontent registry for parsing datafeed aggs
     */
    public static void validate(DatafeedConfig datafeedConfig, Job job, NamedXContentRegistry xContentRegistry) {
        AnalysisConfig analysisConfig = job.getAnalysisConfig();
        if (analysisConfig.getLatency() != null && analysisConfig.getLatency().seconds() > 0) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_DOES_NOT_SUPPORT_JOB_WITH_LATENCY));
        }
        // TODO should we validate that the aggs define the fields requested in the analysis config?
        if (datafeedConfig.hasAggregations()) {
            checkSummaryCountFieldNameIsSet(analysisConfig);
            checkValidHistogramInterval(datafeedConfig, analysisConfig, xContentRegistry);
            checkFrequencyIsMultipleOfHistogramInterval(datafeedConfig, xContentRegistry);
        }

        DelayedDataCheckConfig delayedDataCheckConfig = datafeedConfig.getDelayedDataCheckConfig();
        TimeValue bucketSpan = analysisConfig.getBucketSpan();
        if (delayedDataCheckConfig.isEnabled()) {
            checkValidDelayedDataCheckConfig(bucketSpan, delayedDataCheckConfig);
        }

        checkTimeFieldIsNotASearchRuntimeField(datafeedConfig, job.getDataDescription().getTimeField());
    }

    private static void checkValidDelayedDataCheckConfig(TimeValue bucketSpan, DelayedDataCheckConfig delayedDataCheckConfig) {
        TimeValue delayedDataCheckWindow = delayedDataCheckConfig.getCheckWindow();
        if (delayedDataCheckWindow != null) { // NULL implies we calculate on use and thus is always valid
            if (delayedDataCheckWindow.compareTo(bucketSpan) < 0) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_TOO_SMALL, delayedDataCheckWindow, bucketSpan)
                );
            }
            if (delayedDataCheckWindow.millis() > bucketSpan.millis() * DelayedDataCheckConfig.MAX_NUMBER_SPANABLE_BUCKETS) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(
                        Messages.DATAFEED_CONFIG_DELAYED_DATA_CHECK_SPANS_TOO_MANY_BUCKETS,
                        delayedDataCheckWindow,
                        bucketSpan
                    )
                );
            }
        }
    }

    private static void checkSummaryCountFieldNameIsSet(AnalysisConfig analysisConfig) {
        if (Strings.isNullOrEmpty(analysisConfig.getSummaryCountFieldName())) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(Messages.DATAFEED_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD)
            );
        }
    }

    private static void checkValidHistogramInterval(
        DatafeedConfig datafeedConfig,
        AnalysisConfig analysisConfig,
        NamedXContentRegistry xContentRegistry
    ) {
        long histogramIntervalMillis = datafeedConfig.getHistogramIntervalMillis(xContentRegistry);
        long bucketSpanMillis = analysisConfig.getBucketSpan().millis();
        if (histogramIntervalMillis > bucketSpanMillis) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(
                    Messages.DATAFEED_AGGREGATIONS_INTERVAL_MUST_LESS_OR_EQUAL_TO_BUCKET_SPAN,
                    TimeValue.timeValueMillis(histogramIntervalMillis).getStringRep(),
                    TimeValue.timeValueMillis(bucketSpanMillis).getStringRep()
                )
            );
        }

        if (bucketSpanMillis % histogramIntervalMillis != 0) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(
                    Messages.DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_DIVISOR_OF_BUCKET_SPAN,
                    TimeValue.timeValueMillis(histogramIntervalMillis).getStringRep(),
                    TimeValue.timeValueMillis(bucketSpanMillis).getStringRep()
                )
            );
        }
    }

    private static void checkFrequencyIsMultipleOfHistogramInterval(DatafeedConfig datafeedConfig, NamedXContentRegistry xContentRegistry) {
        TimeValue frequency = datafeedConfig.getFrequency();
        if (frequency != null) {
            long histogramIntervalMillis = datafeedConfig.getHistogramIntervalMillis(xContentRegistry);
            long frequencyMillis = frequency.millis();
            if (frequencyMillis % histogramIntervalMillis != 0) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(
                        Messages.DATAFEED_FREQUENCY_MUST_BE_MULTIPLE_OF_AGGREGATIONS_INTERVAL,
                        frequency,
                        TimeValue.timeValueMillis(histogramIntervalMillis).getStringRep()
                    )
                );
            }
        }
    }

    private static void checkTimeFieldIsNotASearchRuntimeField(DatafeedConfig datafeedConfig, String timeField) {
        // check the search RT mappings defined in the datafeed
        Map<String, Object> runtimeMappings = datafeedConfig.getRuntimeMappings();
        for (Map.Entry<String, Object> entry : runtimeMappings.entrySet()) {
            // top level objects are fields
            String fieldName = entry.getKey();
            if (timeField.equals(fieldName)) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.JOB_CONFIG_TIME_FIELD_CANNOT_BE_RUNTIME, timeField)
                );
            }
        }
    }
}
