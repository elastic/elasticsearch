/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.results;

import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.job.usage.Usage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * Defines the field names that we use for our results.
 * Fields from the raw data with these names are not added to any result.  Even
 * different types of results will not have raw data fields with reserved names
 * added to them, as it could create confusion if in some results a given field
 * contains raw data and in others it contains some aspect of our output.
 */
public final class ReservedFieldNames {
    /**
     * jobId isn't in this package, so redefine.
     */
    private static final String JOB_ID_NAME = "jobId";

    /**
     * @timestamp isn't in this package, so redefine.
     */
    private static final String ES_TIMESTAMP = "timestamp";

    public static final String BUCKET_PROCESSING_TIME_TYPE = "bucketProcessingTime";
    public static final String AVERAGE_PROCESSING_TIME_MS = "averageProcessingTimeMs";

    public static final String PARTITION_NORMALIZED_PROB_TYPE = "partitionNormalizedProb";
    public static final String PARTITION_NORMALIZED_PROBS = "partitionNormalizedProbs";

    /**
     * This array should be updated to contain all the field names that appear
     * in any documents we store in our results index.  (The reason it's any
     * documents we store and not just results documents is that Elasticsearch
     * 2.x requires mappings for given fields be consistent across all types
     * in a given index.)
     */
    private static final String[] RESERVED_FIELD_NAME_ARRAY = {
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
            AnomalyCause.INFLUENCERS.getPreferredName(),
            AnomalyCause.FIELD_NAME.getPreferredName(),

            AnomalyRecord.DETECTOR_INDEX.getPreferredName(),
            AnomalyRecord.PROBABILITY.getPreferredName(),
            AnomalyRecord.BY_FIELD_NAME.getPreferredName(),
            AnomalyRecord.BY_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.CORRELATED_BY_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.PARTITION_FIELD_NAME.getPreferredName(),
            AnomalyRecord.PARTITION_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.FUNCTION.getPreferredName(),
            AnomalyRecord.FUNCTION_DESCRIPTION.getPreferredName(),
            AnomalyRecord.TYPICAL.getPreferredName(),
            AnomalyRecord.ACTUAL.getPreferredName(),
            AnomalyRecord.IS_INTERIM.getPreferredName(),
            AnomalyRecord.INFLUENCERS.getPreferredName(),
            AnomalyRecord.FIELD_NAME.getPreferredName(),
            AnomalyRecord.OVER_FIELD_NAME.getPreferredName(),
            AnomalyRecord.OVER_FIELD_VALUE.getPreferredName(),
            AnomalyRecord.CAUSES.getPreferredName(),
            AnomalyRecord.ANOMALY_SCORE.getPreferredName(),
            AnomalyRecord.NORMALIZED_PROBABILITY.getPreferredName(),
            AnomalyRecord.INITIAL_NORMALIZED_PROBABILITY.getPreferredName(),
            AnomalyRecord.BUCKET_SPAN.getPreferredName(),

            Bucket.ANOMALY_SCORE.getPreferredName(),
            Bucket.BUCKET_SPAN.getPreferredName(),
            Bucket.MAX_NORMALIZED_PROBABILITY.getPreferredName(),
            Bucket.IS_INTERIM.getPreferredName(),
            Bucket.RECORD_COUNT.getPreferredName(),
            Bucket.EVENT_COUNT.getPreferredName(),
            Bucket.RECORDS.getPreferredName(),
            Bucket.BUCKET_INFLUENCERS.getPreferredName(),
            Bucket.INITIAL_ANOMALY_SCORE.getPreferredName(),
            Bucket.PROCESSING_TIME_MS.getPreferredName(),
            Bucket.PARTITION_SCORES.getPreferredName(),

            BucketInfluencer.BUCKET_TIME.getPreferredName(), BucketInfluencer.INFLUENCER_FIELD_NAME.getPreferredName(),
            BucketInfluencer.INITIAL_ANOMALY_SCORE.getPreferredName(), BucketInfluencer.ANOMALY_SCORE.getPreferredName(),
            BucketInfluencer.RAW_ANOMALY_SCORE.getPreferredName(), BucketInfluencer.PROBABILITY.getPreferredName(),

            AVERAGE_PROCESSING_TIME_MS,

            PARTITION_NORMALIZED_PROBS,
            PARTITION_NORMALIZED_PROB_TYPE,

            CategoryDefinition.CATEGORY_ID.getPreferredName(),
            CategoryDefinition.TERMS.getPreferredName(),
            CategoryDefinition.REGEX.getPreferredName(),
            CategoryDefinition.MAX_MATCHING_LENGTH.getPreferredName(),
            CategoryDefinition.EXAMPLES.getPreferredName(),

            DataCounts.PROCESSED_RECORD_COUNT.getPreferredName(),
            DataCounts.PROCESSED_FIELD_COUNT.getPreferredName(),
            DataCounts.INPUT_BYTES.getPreferredName(),
            DataCounts.INPUT_RECORD_COUNT.getPreferredName(),
            DataCounts.INPUT_FIELD_COUNT.getPreferredName(),
            DataCounts.INVALID_DATE_COUNT.getPreferredName(),
            DataCounts.MISSING_FIELD_COUNT.getPreferredName(),
            DataCounts.OUT_OF_ORDER_TIME_COUNT.getPreferredName(),
            DataCounts.LATEST_RECORD_TIME.getPreferredName(),
            DataCounts.EARLIEST_RECORD_TIME.getPreferredName(),

            Influence.INFLUENCER_FIELD_NAME.getPreferredName(),
            Influence.INFLUENCER_FIELD_VALUES.getPreferredName(),

            Influencer.PROBABILITY.getPreferredName(),
            Influencer.INFLUENCER_FIELD_NAME.getPreferredName(),
            Influencer.INFLUENCER_FIELD_VALUE.getPreferredName(),
            Influencer.INITIAL_ANOMALY_SCORE.getPreferredName(),
            Influencer.ANOMALY_SCORE.getPreferredName(),

            ModelDebugOutput.PARTITION_FIELD_NAME.getPreferredName(), ModelDebugOutput.PARTITION_FIELD_VALUE.getPreferredName(),
            ModelDebugOutput.OVER_FIELD_NAME.getPreferredName(), ModelDebugOutput.OVER_FIELD_VALUE.getPreferredName(),
            ModelDebugOutput.BY_FIELD_NAME.getPreferredName(), ModelDebugOutput.BY_FIELD_VALUE.getPreferredName(),
            ModelDebugOutput.DEBUG_FEATURE.getPreferredName(), ModelDebugOutput.DEBUG_LOWER.getPreferredName(),
            ModelDebugOutput.DEBUG_UPPER.getPreferredName(), ModelDebugOutput.DEBUG_MEDIAN.getPreferredName(),
            ModelDebugOutput.ACTUAL.getPreferredName(),

            ModelSizeStats.MODEL_BYTES_FIELD.getPreferredName(),
            ModelSizeStats.TOTAL_BY_FIELD_COUNT_FIELD.getPreferredName(),
            ModelSizeStats.TOTAL_OVER_FIELD_COUNT_FIELD.getPreferredName(),
            ModelSizeStats.TOTAL_PARTITION_FIELD_COUNT_FIELD.getPreferredName(),
            ModelSizeStats.BUCKET_ALLOCATION_FAILURES_COUNT_FIELD.getPreferredName(),
            ModelSizeStats.MEMORY_STATUS_FIELD.getPreferredName(),
            ModelSizeStats.LOG_TIME_FIELD.getPreferredName(),

            // ModelSnapshot.DESCRIPTION is not reserved because it is an analyzed string
            ModelSnapshot.RESTORE_PRIORITY.getPreferredName(),
            ModelSnapshot.SNAPSHOT_ID.getPreferredName(),
            ModelSnapshot.SNAPSHOT_DOC_COUNT.getPreferredName(),
            ModelSizeStats.TYPE.getPreferredName(),
            ModelSnapshot.LATEST_RECORD_TIME.getPreferredName(),
            ModelSnapshot.LATEST_RESULT_TIME.getPreferredName(),

            Quantiles.QUANTILE_STATE.getPreferredName(),

            Usage.INPUT_BYTES,
            Usage.INPUT_FIELD_COUNT,
            Usage.INPUT_RECORD_COUNT,
            Usage.TIMESTAMP,
            Usage.TYPE,


            JOB_ID_NAME,
            ES_TIMESTAMP
    };

    /**
     * A set of all reserved field names in our results.  Fields from the raw
     * data with these names are not added to any result.
     */
    public static final Set<String> RESERVED_FIELD_NAMES = new HashSet<>(Arrays.asList(RESERVED_FIELD_NAME_ARRAY));

    private ReservedFieldNames() {
    }
}
