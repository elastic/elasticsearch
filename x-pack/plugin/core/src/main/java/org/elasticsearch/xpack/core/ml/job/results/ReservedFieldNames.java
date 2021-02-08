/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshotField;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * Defines the field names that we use for our results.
 * Fields from the raw data with these names are not added to any result.  Even
 * different types of results will not have raw data fields with reserved names
 * added to them, as it could create confusion if in some results a given field
 * contains raw data and in others it contains some aspect of our output.
 */
public final class ReservedFieldNames {
    private static final Pattern DOT_PATTERN = Pattern.compile("\\.");

    /**
     * This array should be updated to contain all the field names that appear
     * in any documents we store in our results index.  (The reason it's any
     * documents we store and not just results documents is that Elasticsearch
     * 2.x requires mappings for given fields be consistent across all types
     * in a given index.)
     */
    private static final String[] RESERVED_RESULT_FIELD_NAME_ARRAY = {
            ElasticsearchMappings.ALL_FIELD_VALUES,

            Job.ID.getPreferredName(),

            AnomalyCause.PROBABILITY.getPreferredName(),
            AnomalyCause.OVER_FIELD_NAME.getPreferredName(),
            AnomalyCause.OVER_FIELD_VALUE.getPreferredName(),
            AnomalyCause.BY_FIELD_NAME.getPreferredName(),
            AnomalyCause.BY_FIELD_VALUE.getPreferredName(),
            AnomalyCause.CORRELATED_BY_FIELD_VALUE.getPreferredName(),
            AnomalyCause.PARTITION_FIELD_NAME.getPreferredName(),
            AnomalyCause.PARTITION_FIELD_VALUE.getPreferredName(),
            AnomalyCause.FUNCTION.getPreferredName(),
            AnomalyCause.FUNCTION_DESCRIPTION.getPreferredName(),
            AnomalyCause.TYPICAL.getPreferredName(),
            AnomalyCause.ACTUAL.getPreferredName(),
            AnomalyCause.GEO_RESULTS.getPreferredName(),
            AnomalyCause.INFLUENCERS.getPreferredName(),
            AnomalyCause.FIELD_NAME.getPreferredName(),

            AnomalyRecord.PROBABILITY.getPreferredName(),
            AnomalyRecord.MULTI_BUCKET_IMPACT.getPreferredName(),
            AnomalyRecord.BY_FIELD_NAME.getPreferredName(),
            AnomalyRecord.BY_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.CORRELATED_BY_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.PARTITION_FIELD_NAME.getPreferredName(),
            AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.FUNCTION.getPreferredName(),
            AnomalyRecord.FUNCTION_DESCRIPTION.getPreferredName(),
            AnomalyRecord.TYPICAL.getPreferredName(),
            AnomalyRecord.ACTUAL.getPreferredName(),
            AnomalyRecord.GEO_RESULTS.getPreferredName(),
            AnomalyRecord.INFLUENCERS.getPreferredName(),
            AnomalyRecord.FIELD_NAME.getPreferredName(),
            AnomalyRecord.OVER_FIELD_NAME.getPreferredName(),
            AnomalyRecord.OVER_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.CAUSES.getPreferredName(),
            AnomalyRecord.RECORD_SCORE.getPreferredName(),
            AnomalyRecord.INITIAL_RECORD_SCORE.getPreferredName(),
            AnomalyRecord.BUCKET_SPAN.getPreferredName(),

            GeoResults.TYPICAL_POINT.getPreferredName(),
            GeoResults.ACTUAL_POINT.getPreferredName(),

            Bucket.ANOMALY_SCORE.getPreferredName(),
            Bucket.BUCKET_INFLUENCERS.getPreferredName(),
            Bucket.BUCKET_SPAN.getPreferredName(),
            Bucket.EVENT_COUNT.getPreferredName(),
            Bucket.INITIAL_ANOMALY_SCORE.getPreferredName(),
            Bucket.PROCESSING_TIME_MS.getPreferredName(),
            Bucket.SCHEDULED_EVENTS.getPreferredName(),

            BucketInfluencer.INITIAL_ANOMALY_SCORE.getPreferredName(), BucketInfluencer.ANOMALY_SCORE.getPreferredName(),
            BucketInfluencer.RAW_ANOMALY_SCORE.getPreferredName(), BucketInfluencer.PROBABILITY.getPreferredName(),

            CategoryDefinition.CATEGORY_ID.getPreferredName(),
            CategoryDefinition.TERMS.getPreferredName(),
            CategoryDefinition.REGEX.getPreferredName(),
            CategoryDefinition.MAX_MATCHING_LENGTH.getPreferredName(),
            CategoryDefinition.EXAMPLES.getPreferredName(),
            CategoryDefinition.NUM_MATCHES.getPreferredName(),
            CategoryDefinition.PREFERRED_TO_CATEGORIES.getPreferredName(),

            DataCounts.PROCESSED_RECORD_COUNT.getPreferredName(),
            DataCounts.PROCESSED_FIELD_COUNT.getPreferredName(),
            DataCounts.INPUT_BYTES.getPreferredName(),
            DataCounts.INPUT_RECORD_COUNT.getPreferredName(),
            DataCounts.INPUT_FIELD_COUNT.getPreferredName(),
            DataCounts.INVALID_DATE_COUNT.getPreferredName(),
            DataCounts.MISSING_FIELD_COUNT.getPreferredName(),
            DataCounts.OUT_OF_ORDER_TIME_COUNT.getPreferredName(),
            DataCounts.EMPTY_BUCKET_COUNT.getPreferredName(),
            DataCounts.SPARSE_BUCKET_COUNT.getPreferredName(),
            DataCounts.BUCKET_COUNT.getPreferredName(),
            DataCounts.LATEST_RECORD_TIME.getPreferredName(),
            DataCounts.EARLIEST_RECORD_TIME.getPreferredName(),
            DataCounts.LAST_DATA_TIME.getPreferredName(),
            DataCounts.LATEST_EMPTY_BUCKET_TIME.getPreferredName(),
            DataCounts.LATEST_SPARSE_BUCKET_TIME.getPreferredName(),
            DataCounts.LOG_TIME.getPreferredName(),

            Detector.DETECTOR_INDEX.getPreferredName(),

            Influence.INFLUENCER_FIELD_NAME.getPreferredName(),
            Influence.INFLUENCER_FIELD_VALUES.getPreferredName(),

            Influencer.PROBABILITY.getPreferredName(),
            Influencer.INFLUENCER_FIELD_NAME.getPreferredName(),
            Influencer.INFLUENCER_FIELD_VALUE.getPreferredName(),
            Influencer.INITIAL_INFLUENCER_SCORE.getPreferredName(),
            Influencer.INFLUENCER_SCORE.getPreferredName(),
            Influencer.BUCKET_SPAN.getPreferredName(),

            ModelPlot.PARTITION_FIELD_NAME.getPreferredName(), ModelPlot.PARTITION_FIELD_VALUE.getPreferredName(),
            ModelPlot.OVER_FIELD_NAME.getPreferredName(), ModelPlot.OVER_FIELD_VALUE.getPreferredName(),
            ModelPlot.BY_FIELD_NAME.getPreferredName(), ModelPlot.BY_FIELD_VALUE.getPreferredName(),
            ModelPlot.MODEL_FEATURE.getPreferredName(), ModelPlot.MODEL_LOWER.getPreferredName(),
            ModelPlot.MODEL_UPPER.getPreferredName(), ModelPlot.MODEL_MEDIAN.getPreferredName(),
            ModelPlot.ACTUAL.getPreferredName(),

            Forecast.FORECAST_LOWER.getPreferredName(), Forecast.FORECAST_UPPER.getPreferredName(),
            Forecast.FORECAST_PREDICTION.getPreferredName(),
            Forecast.FORECAST_ID.getPreferredName(),

            //re-use: TIMESTAMP
            ForecastRequestStats.START_TIME.getPreferredName(),
            ForecastRequestStats.END_TIME.getPreferredName(),
            ForecastRequestStats.CREATE_TIME.getPreferredName(),
            ForecastRequestStats.EXPIRY_TIME.getPreferredName(),
            ForecastRequestStats.MESSAGES.getPreferredName(),
            ForecastRequestStats.PROGRESS.getPreferredName(),
            ForecastRequestStats.STATUS.getPreferredName(),
            ForecastRequestStats.MEMORY_USAGE.getPreferredName(),

            ModelSizeStats.MODEL_BYTES_FIELD.getPreferredName(),
            ModelSizeStats.PEAK_MODEL_BYTES_FIELD.getPreferredName(),
            ModelSizeStats.TOTAL_BY_FIELD_COUNT_FIELD.getPreferredName(),
            ModelSizeStats.TOTAL_OVER_FIELD_COUNT_FIELD.getPreferredName(),
            ModelSizeStats.TOTAL_PARTITION_FIELD_COUNT_FIELD.getPreferredName(),
            ModelSizeStats.BUCKET_ALLOCATION_FAILURES_COUNT_FIELD.getPreferredName(),
            ModelSizeStats.MEMORY_STATUS_FIELD.getPreferredName(),
            ModelSizeStats.ASSIGNMENT_MEMORY_BASIS_FIELD.getPreferredName(),
            ModelSizeStats.LOG_TIME_FIELD.getPreferredName(),

            ModelSnapshot.DESCRIPTION.getPreferredName(),
            ModelSnapshotField.SNAPSHOT_ID.getPreferredName(),
            ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName(),
            ModelSnapshot.LATEST_RECORD_TIME.getPreferredName(),
            ModelSnapshot.LATEST_RESULT_TIME.getPreferredName(),
            ModelSnapshot.RETAIN.getPreferredName(),
            ModelSnapshot.MIN_VERSION.getPreferredName(),

            Result.RESULT_TYPE.getPreferredName(),
            Result.TIMESTAMP.getPreferredName(),
            Result.IS_INTERIM.getPreferredName(),

            TimingStats.BUCKET_COUNT.getPreferredName(),
            TimingStats.MIN_BUCKET_PROCESSING_TIME_MS.getPreferredName(),
            TimingStats.MAX_BUCKET_PROCESSING_TIME_MS.getPreferredName(),
            TimingStats.AVG_BUCKET_PROCESSING_TIME_MS.getPreferredName(),
            TimingStats.EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS.getPreferredName(),
            TimingStats.EXPONENTIAL_AVG_CALCULATION_CONTEXT.getPreferredName(),

            DatafeedTimingStats.SEARCH_COUNT.getPreferredName(),
            DatafeedTimingStats.BUCKET_COUNT.getPreferredName(),
            DatafeedTimingStats.TOTAL_SEARCH_TIME_MS.getPreferredName(),
            DatafeedTimingStats.EXPONENTIAL_AVG_CALCULATION_CONTEXT.getPreferredName(),

            ExponentialAverageCalculationContext.INCREMENTAL_METRIC_VALUE_MS.getPreferredName(),
            ExponentialAverageCalculationContext.LATEST_TIMESTAMP.getPreferredName(),
            ExponentialAverageCalculationContext.PREVIOUS_EXPONENTIAL_AVERAGE_MS.getPreferredName(),

            GetResult._ID,
            GetResult._INDEX
    };

    /**
     * Test if fieldName is one of the reserved result fieldnames or if it contains
     * dots then that the segment before the first dot is not a reserved results
     * fieldname. A fieldName containing dots represents nested fields in which
     * case we only care about the top level.
     *
     * @param fieldName Document field name. This may contain dots '.'
     * @return True if fieldName is not a reserved results fieldname or the top level segment
     * is not a reserved name.
     */
    public static boolean isValidFieldName(String fieldName) {
        String[] segments = DOT_PATTERN.split(fieldName);
        return RESERVED_RESULT_FIELD_NAMES.contains(segments[0]) == false;
    }

    /**
     * A set of all reserved field names in our results.  Fields from the raw
     * data with these names are not added to any result.
     */
    public static final Set<String> RESERVED_RESULT_FIELD_NAMES = new HashSet<>(Arrays.asList(RESERVED_RESULT_FIELD_NAME_ARRAY));

    private ReservedFieldNames() {
    }
}
