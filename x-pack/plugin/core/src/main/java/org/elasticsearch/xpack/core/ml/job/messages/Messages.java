/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.messages;

import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.text.MessageFormat;
import java.util.Locale;

/**
 * Log and audit message strings
 */
public final class Messages {

    public static final String DATAFEED_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD =
            "A job configured with a datafeed with aggregations must set summary_count_field_name; use doc_count or suitable alternative";
    public static final String DATAFEED_CANNOT_DELETE_IN_CURRENT_STATE = "Cannot delete datafeed [{0}] while its status is {1}";
    public static final String DATAFEED_CANNOT_UPDATE_IN_CURRENT_STATE = "Cannot update datafeed [{0}] while its status is {1}";
    public static final String DATAFEED_CONFIG_CANNOT_USE_SCRIPT_FIELDS_WITH_AGGS =
            "script_fields cannot be used in combination with aggregations";
    public static final String DATAFEED_CONFIG_INVALID_OPTION_VALUE = "Invalid {0} value ''{1}'' in datafeed configuration";
    public static final String DATAFEED_CONFIG_DELAYED_DATA_CHECK_TOO_SMALL =
        "delayed_data_check_config: check_window [{0}] must be greater than the bucket_span [{1}]";
    public static final String DATAFEED_CONFIG_DELAYED_DATA_CHECK_SPANS_TOO_MANY_BUCKETS =
        "delayed_data_check_config: check_window [{0}] must be less than 10,000x the bucket_span [{1}]";
    public static final String DATAFEED_CONFIG_QUERY_BAD_FORMAT = "Datafeed query is not parsable";
    public static final String DATAFEED_CONFIG_AGG_BAD_FORMAT = "Datafeed aggregations are not parsable";

    public static final String DATAFEED_DOES_NOT_SUPPORT_JOB_WITH_LATENCY = "A job configured with datafeed cannot support latency";
    public static final String DATAFEED_NOT_FOUND = "No datafeed with id [{0}] exists";
    public static final String DATAFEED_AGGREGATIONS_REQUIRES_DATE_HISTOGRAM =
            "A date_histogram (or histogram) aggregation is required";
    public static final String DATAFEED_AGGREGATIONS_MAX_ONE_DATE_HISTOGRAM =
            "Aggregations can only have 1 date_histogram or histogram aggregation";
    public static final String DATAFEED_AGGREGATIONS_REQUIRES_DATE_HISTOGRAM_NO_SIBLINGS =
            "The date_histogram (or histogram) aggregation cannot have sibling aggregations";
    public static final String DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_GREATER_THAN_ZERO =
            "Aggregation interval must be greater than 0";
    public static final String DATAFEED_AGGREGATIONS_INTERVAL_MUST_BE_DIVISOR_OF_BUCKET_SPAN =
            "Aggregation interval [{0}] must be a divisor of the bucket_span [{1}]";
    public static final String DATAFEED_AGGREGATIONS_INTERVAL_MUST_LESS_OR_EQUAL_TO_BUCKET_SPAN =
            "Aggregation interval [{0}] must be less than or equal to the bucket_span [{1}]";
    public static final String DATAFEED_DATA_HISTOGRAM_MUST_HAVE_NESTED_MAX_AGGREGATION =
            "Date histogram must have nested max aggregation for time_field [{0}]";
    public static final String DATAFEED_MISSING_MAX_AGGREGATION_FOR_TIME_FIELD = "Missing max aggregation for time_field [{0}]";
    public static final String DATAFEED_FREQUENCY_MUST_BE_MULTIPLE_OF_AGGREGATIONS_INTERVAL =
            "Datafeed frequency [{0}] must be a multiple of the aggregation interval [{1}]";
    public static final String DATAFEED_AGGREGATIONS_COMPOSITE_AGG_MUST_HAVE_SINGLE_DATE_SOURCE =
        "Composite aggregation [{0}] must have exactly one date_histogram source";
    public static final String DATAFEED_AGGREGATIONS_COMPOSITE_AGG_DATE_HISTOGRAM_SOURCE_MISSING_BUCKET =
        "Datafeed composite aggregation [{0}] date_histogram [{1}] source does not support missing_buckets";
    public static final String DATAFEED_AGGREGATIONS_COMPOSITE_AGG_DATE_HISTOGRAM_SORT =
        "Datafeed composite aggregation [{0}] date_histogram [{1}] must be sorted in ascending order";
    public static final String DATAFEED_AGGREGATIONS_COMPOSITE_AGG_MUST_BE_TOP_LEVEL_AND_ALONE =
        "Composite aggregation [{0}] must be the only composite agg and should be the only top level aggregation";
    public static final String DATAFEED_ID_ALREADY_TAKEN = "A datafeed with id [{0}] already exists";
    public static final String DATAFEED_NEEDS_REMOTE_CLUSTER_SEARCH = "Datafeed [{0}] is configured with a remote index pattern(s) {1}" +
        " but the current node [{2}] is not allowed to connect to remote clusters." +
        " Please enable node.remote_cluster_client for all machine learning nodes and master-eligible nodes.";

    public static final String DATA_FRAME_ANALYTICS_BAD_QUERY_FORMAT = "Data Frame Analytics config query is not parsable";
    public static final String DATA_FRAME_ANALYTICS_BAD_FIELD_FILTER = "No field [{0}] could be detected";

    public static final String DATA_FRAME_ANALYTICS_AUDIT_CREATED = "Created analytics with type [{0}]";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_UPDATED = "Updated analytics settings: {0}";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_STARTED = "Started analytics";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_STOPPED = "Stopped analytics";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_FORCE_STOPPED = "Stopped analytics (forced)";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_DELETED = "Deleted analytics";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_UPDATED_STATE_WITH_REASON =
            "Updated analytics task state to [{0}] with reason [{1}]";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_ESTIMATED_MEMORY_USAGE = "Estimated memory usage [{0}]";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_ESTIMATED_MEMORY_USAGE_HIGHER_THAN_CONFIGURED =
        "Configured model memory limit [{0}] is lower than the expected memory usage [{1}]. " +
            "The analytics job may fail due to configured memory constraints.";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_CREATING_DEST_INDEX = "Creating destination index [{0}]";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_REUSING_DEST_INDEX = "Using existing destination index [{0}]";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_STARTED_REINDEXING = "Started reindexing to destination index [{0}]";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_FINISHED_REINDEXING =
        "Finished reindexing to destination index [{0}], took [{1}]";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_FINISHED_ANALYSIS = "Finished analysis";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_RESTORING_STATE = "Restoring from previous model state";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_STARTED_LOADING_DATA = "Started loading data";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_STARTED_ANALYZING = "Started analyzing";
    public static final String DATA_FRAME_ANALYTICS_AUDIT_STARTED_WRITING_RESULTS = "Started writing results";
    public static final String DATA_FRAME_ANALYTICS_CANNOT_UPDATE_IN_CURRENT_STATE = "Cannot update analytics [{0}] unless it''s stopped";

    public static final String FILTER_CANNOT_DELETE = "Cannot delete filter [{0}] currently used by jobs {1}";
    public static final String FILTER_CONTAINS_TOO_MANY_ITEMS = "Filter [{0}] contains too many items; up to [{1}] items are allowed";
    public static final String FILTER_NOT_FOUND = "No filter with id [{0}] exists";

    public static final String INCONSISTENT_ID =
            "Inconsistent {0}; ''{1}'' specified in the body differs from ''{2}'' specified as a URL argument";
    public static final String INVALID_ID = "Invalid {0}; ''{1}'' can contain lowercase alphanumeric (a-z and 0-9), hyphens or " +
            "underscores; must start and end with alphanumeric";
    public static final String ID_TOO_LONG = "Invalid {0}; ''{1}'' cannot contain more than {2} characters.";
    public static final String INVALID_GROUP = "Invalid group id ''{0}''; must be non-empty string and may contain lowercase alphanumeric" +
            " (a-z and 0-9), hyphens or underscores; must start and end with alphanumeric";

    public static final String INFERENCE_TRAINED_MODEL_EXISTS = "Trained machine learning model [{0}] already exists";
    public static final String INFERENCE_TRAINED_MODEL_DOC_EXISTS = "Trained machine learning model chunked doc [{0}][{1}] already exists";
    public static final String INFERENCE_TRAINED_MODEL_METADATA_EXISTS = "Trained machine learning model metadata [{0}] already exists";
    public static final String INFERENCE_FAILED_TO_STORE_MODEL = "Failed to store trained machine learning model [{0}]";
    public static final String INFERENCE_FAILED_TO_STORE_MODEL_METADATA = "Failed to store trained machine learning model metadata [{0}]";
    public static final String INFERENCE_NOT_FOUND = "Could not find trained model [{0}]";
    public static final String INFERENCE_NOT_FOUND_MULTIPLE = "Could not find trained models {0}";
    public static final String INFERENCE_CONFIG_NOT_SUPPORTED_ON_VERSION =
        "Configuration [{0}] requires minimum node version [{1}] (current minimum node version [{2}]";
    public static final String MODEL_DEFINITION_NOT_FOUND = "Could not find trained model definition [{0}]";
    public static final String MODEL_METADATA_NOT_FOUND = "Could not find trained model metadata {0}";
    public static final String VOCABULARY_NOT_FOUND = "[{0}] Could not find vocabulary document [{1}] for model ";
    public static final String INFERENCE_CANNOT_DELETE_ML_MANAGED_MODEL =
        "Unable to delete model [{0}] as it is required by machine learning";
    public static final String MODEL_DEFINITION_TRUNCATED =
        "Model definition truncated. Unable to deserialize trained model definition [{0}]";
    public static final String INFERENCE_FAILED_TO_DESERIALIZE = "Could not deserialize trained model [{0}]";
    public static final String INFERENCE_TOO_MANY_DEFINITIONS_REQUESTED =
        "Getting model definition is not supported when getting more than one model";
    public static final String INFERENCE_WARNING_ALL_FIELDS_MISSING = "Model [{0}] could not be inferred as all fields were missing";
    public static final String INFERENCE_INVALID_TAGS = "Invalid tags {0}; must only can contain lowercase alphanumeric (a-z and 0-9), " +
        "hyphens or underscores, must start and end with alphanumeric, and must be less than {1} characters.";
    public static final String INFERENCE_TAGS_AND_MODEL_IDS_UNIQUE = "The provided tags {0} must not match existing model_ids.";
    public static final String INFERENCE_MODEL_ID_AND_TAGS_UNIQUE = "The provided model_id {0} must not match existing tags.";

    public static final String INVALID_MODEL_ALIAS = "Invalid model_alias; ''{0}'' can contain lowercase alphanumeric (a-z and 0-9), " +
        "hyphens or underscores; must start with alphanumeric and cannot end with numbers";
    public static final String TRAINED_MODEL_INPUTS_DIFFER_SIGNIFICANTLY =
        "The input fields for new model [{0}] and for old model [{1}] differ significantly, model results may change drastically.";

    public static final String JOB_AUDIT_DATAFEED_DATA_SEEN_AGAIN = "Datafeed has started retrieving data again";
    public static final String JOB_AUDIT_CREATED = "Job created";
    public static final String JOB_AUDIT_UPDATED = "Job updated: {0}";
    public static final String JOB_AUDIT_CLOSING = "Job is closing";
    public static final String JOB_AUDIT_RESET = "Job has been reset";
    public static final String JOB_AUDIT_FORCE_CLOSING = "Job is closing (forced)";
    public static final String JOB_AUDIT_DATAFEED_CONTINUED_REALTIME = "Datafeed continued in real-time";
    public static final String JOB_AUDIT_DATAFEED_DATA_ANALYSIS_ERROR = "Datafeed is encountering errors submitting data for analysis: {0}";
    public static final String JOB_AUDIT_DATAFEED_DATA_EXTRACTION_ERROR = "Datafeed is encountering errors extracting data: {0}";
    public static final String JOB_AUDIT_DATAFEED_LOOKBACK_COMPLETED = "Datafeed lookback completed";
    public static final String JOB_AUDIT_DATAFEED_LOOKBACK_NO_DATA = "Datafeed lookback retrieved no data";
    public static final String JOB_AUDIT_DATAFEED_NO_DATA = "Datafeed has been retrieving no data for a while";
    public static final String JOB_AUDIT_DATAFEED_MISSING_DATA =
        "Datafeed has missed {0} documents due to ingest latency, latest bucket with missing data is [{1}]." +
            " Consider increasing query_delay";
    public static final String JOB_AUDIT_DATAFEED_RECOVERED = "Datafeed has recovered data extraction and analysis";
    public static final String JOB_AUDIT_DATAFEED_STARTED_FROM_TO = "Datafeed started (from: {0} to: {1}) with frequency [{2}]";
    public static final String JOB_AUDIT_DATAFEED_STARTED_REALTIME = "Datafeed started in real-time";
    public static final String JOB_AUDIT_DATAFEED_STOPPED = "Datafeed stopped";
    public static final String JOB_AUDIT_DATAFEED_ISOLATED = "Datafeed isolated";
    public static final String JOB_AUDIT_DELETING = "Deleting job by task with id ''{0}''";
    public static final String JOB_AUDIT_DELETING_FAILED = "Error deleting job: {0}";
    public static final String JOB_AUDIT_DELETED = "Job deleted";
    public static final String JOB_AUDIT_KILLING = "Killing job";
    public static final String JOB_AUDIT_OLD_RESULTS_DELETED = "Deleted results prior to {0}";
    public static final String JOB_AUDIT_OLD_ANNOTATIONS_DELETED = "Deleted annotations prior to {0}";
    public static final String JOB_AUDIT_SNAPSHOT_STORED = "Job model snapshot with id [{0}] stored";
    public static final String JOB_AUDIT_REVERTED = "Job model snapshot reverted to ''{0}''";
    public static final String JOB_AUDIT_SNAPSHOT_DELETED = "Model snapshot [{0}] with description ''{1}'' deleted";
    public static final String JOB_AUDIT_SNAPSHOTS_DELETED = "[{0}] expired model snapshots deleted";
    public static final String JOB_AUDIT_CALENDARS_UPDATED_ON_PROCESS = "Updated calendars in running process";
    public static final String JOB_AUDIT_MEMORY_STATUS_SOFT_LIMIT = "Job memory status changed to soft_limit; memory pruning will now be " +
            "more aggressive";
    public static final String JOB_AUDIT_MEMORY_STATUS_HARD_LIMIT = "Job memory status changed to hard_limit; " +
            "job exceeded model memory limit {0} by {1}. " +
            "Adjust the analysis_limits.model_memory_limit setting to ensure all data is analyzed";
    public static final String JOB_AUDIT_MEMORY_STATUS_HARD_LIMIT_PRE_7_2 = "Job memory status changed to hard_limit at {0}; adjust the " +
        "analysis_limits.model_memory_limit setting to ensure all data is analyzed";

    public static final String JOB_AUDIT_REQUIRES_MORE_MEMORY_TO_RUN = "Job requires at least [{0}] free memory "
        + "on a machine learning capable node to run; [{1}] are available. "
        + "The current total capacity for ML [total: {2}, largest node: {3}].";

    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_DUPLICATES = "categorization_filters contain duplicates";
    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_EMPTY =
            "categorization_filters are not allowed to contain empty strings";
    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_INVALID_REGEX =
            "categorization_filters contains invalid regular expression ''{0}''";
    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_INCOMPATIBLE_WITH_CATEGORIZATION_ANALYZER =
            "categorization_filters cannot be used with categorization_analyzer - " +
                    "instead specify them as pattern_replace char_filters in the analyzer";
    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_REQUIRE_CATEGORIZATION_FIELD_NAME =
            "categorization_filters require setting categorization_field_name";
    public static final String JOB_CONFIG_CATEGORIZATION_ANALYZER_REQUIRES_CATEGORIZATION_FIELD_NAME =
            "categorization_analyzer requires setting categorization_field_name";
    public static final String JOB_CONFIG_DETECTION_RULE_NOT_SUPPORTED_BY_FUNCTION =
            "Invalid detector rule: function {0} only supports conditions that apply to time";
    public static final String JOB_CONFIG_DETECTION_RULE_REQUIRES_SCOPE_OR_CONDITION =
            "Invalid detector rule: at least scope or a condition is required";
    public static final String JOB_CONFIG_DETECTION_RULE_SCOPE_NO_AVAILABLE_FIELDS =
            "Invalid detector rule: scope field ''{0}'' is invalid; detector has no available fields for scoping";
    public static final String JOB_CONFIG_DETECTION_RULE_SCOPE_HAS_INVALID_FIELD =
            "Invalid detector rule: scope field ''{0}'' is invalid; select from {1}";
    public static final String JOB_CONFIG_FIELDNAME_INCOMPATIBLE_FUNCTION = "field_name cannot be used with function ''{0}''";
    public static final String JOB_CONFIG_FIELD_VALUE_TOO_LOW = "{0} cannot be less than {1,number}. Value = {2,number}";
    public static final String JOB_CONFIG_MODEL_MEMORY_LIMIT_TOO_LOW = "model_memory_limit must be at least {1}. Value = {0}";
    public static final String JOB_CONFIG_MODEL_MEMORY_LIMIT_GREATER_THAN_MAX =
            "model_memory_limit [{0}] must be less than the value of the " +
                    MachineLearningField.MAX_MODEL_MEMORY_LIMIT.getKey() +
                    " setting [{1}]";
    public static final String JOB_CONFIG_FUNCTION_INCOMPATIBLE_PRESUMMARIZED =
            "The ''{0}'' function cannot be used in jobs that will take pre-summarized input";
    public static final String JOB_CONFIG_FUNCTION_REQUIRES_BYFIELD = "by_field_name must be set when the ''{0}'' function is used";
    public static final String JOB_CONFIG_FUNCTION_REQUIRES_FIELDNAME = "field_name must be set when the ''{0}'' function is used";
    public static final String JOB_CONFIG_FUNCTION_REQUIRES_OVERFIELD = "over_field_name must be set when the ''{0}'' function is used";
    public static final String JOB_CONFIG_ID_ALREADY_TAKEN = "The job cannot be created with the Id ''{0}''. The Id is already used.";
    public static final String JOB_CONFIG_ID_TOO_LONG = "The job id cannot contain more than {0,number,integer} characters.";
    public static final String JOB_CONFIG_INVALID_CREATE_SETTINGS =
            "The job is configured with fields [{0}] that are illegal to set at job creation";
    public static final String JOB_CONFIG_INVALID_FIELDNAME_CHARS =
            "Invalid field name ''{0}''. Field names including over, by and partition " +
                    "fields cannot contain any of these characters: {1}";
    public static final String JOB_CONFIG_INVALID_FIELDNAME =
            "Invalid field name ''{0}''. Field names including over, by and partition fields cannot be ''{1}''";
    public static final String JOB_CONFIG_INVALID_TIMEFORMAT = "Invalid Time format string ''{0}''";
    public static final String JOB_CONFIG_MISSING_ANALYSISCONFIG = "An analysis_config must be set";
    public static final String JOB_CONFIG_MISSING_DATA_DESCRIPTION = "A data_description must be set";
    public static final String JOB_CONFIG_ANALYSIS_FIELD_MUST_BE_SET =
            "Unless a count or temporal function is used one of field_name, by_field_name or over_field_name must be set";
    public static final String JOB_CONFIG_NO_DETECTORS = "No detectors configured";
    public static final String JOB_CONFIG_OVERFIELD_INCOMPATIBLE_FUNCTION =
            "over_field_name cannot be used with function ''{0}''";
    public static final String JOB_CONFIG_UNKNOWN_FUNCTION = "Unknown function ''{0}''";
    public static final String JOB_CONFIG_UPDATE_ANALYSIS_LIMITS_MODEL_MEMORY_LIMIT_CANNOT_BE_DECREASED =
            "Invalid update value for analysis_limits: model_memory_limit cannot be decreased below current usage; " +
                    "current usage [{0}], update had [{1}]";
    public static final String JOB_CONFIG_DUPLICATE_DETECTORS_DISALLOWED =
            "Duplicate detectors are not allowed: [{0}]";
    public static final String JOB_CONFIG_DETECTOR_DUPLICATE_FIELD_NAME =
            "{0} and {1} cannot be the same: ''{2}''";
    public static final String JOB_CONFIG_DETECTOR_COUNT_DISALLOWED =
            "''count'' is not a permitted value for {0}";
    public static final String JOB_CONFIG_DETECTOR_BY_DISALLOWED =
            "''by'' is not a permitted value for {0}";
    public static final String JOB_CONFIG_DETECTOR_OVER_DISALLOWED =
            "''over'' is not a permitted value for {0}";
    public static final String JOB_CONFIG_MAPPING_TYPE_CLASH =
            "This job would cause a mapping clash with existing field [{0}] - avoid the clash by assigning a dedicated results index";
    public static final String JOB_CONFIG_TIME_FIELD_NOT_ALLOWED_IN_ANALYSIS_CONFIG =
            "data_description.time_field may not be used in the analysis_config";
    public static final String JOB_CONFIG_TIME_FIELD_CANNOT_BE_RUNTIME =
        "data_description.time_field [{0}] cannot be a runtime field";
    public static final String JOB_CONFIG_MODEL_SNAPSHOT_RETENTION_SETTINGS_INCONSISTENT =
            "The value of '" + Job.DAILY_MODEL_SNAPSHOT_RETENTION_AFTER_DAYS + "' [{0}] cannot be greater than '" +
                Job.MODEL_SNAPSHOT_RETENTION_DAYS + "' [{1}]";
    public static final String JOB_CONFIG_DATAFEED_CONFIG_JOB_ID_MISMATCH =
        "datafeed job_id [{0}] does not equal job id [{1}}";

    public static final String JOB_AND_GROUP_NAMES_MUST_BE_UNIQUE =
            "job and group names must be unique but job [{0}] and group [{0}] have the same name";

    public static final String JOB_UNKNOWN_ID = "No known job with id ''{0}''";

    public static final String JOB_FORECAST_NATIVE_PROCESS_KILLED = "forecast unable to complete as native process was killed.";

    public static final String REST_CANNOT_DELETE_HIGHEST_PRIORITY =
            "Model snapshot ''{0}'' is the active snapshot for job ''{1}'', so cannot be deleted";
    public static final String REST_INVALID_DATETIME_PARAMS =
            "Query param [{0}] with value [{1}] cannot be parsed as a date or converted to a number (epoch).";
    public static final String REST_INVALID_FLUSH_PARAMS_MISSING = "Invalid flush parameters: ''{0}'' has not been specified.";
    public static final String REST_INVALID_FLUSH_PARAMS_UNEXPECTED = "Invalid flush parameters: unexpected ''{0}''.";
    public static final String REST_JOB_NOT_CLOSED_REVERT = "Can only revert to a model snapshot when the job is closed.";
    public static final String REST_JOB_NOT_CLOSED_RESET = "Can only reset a job when it is closed.";
    public static final String REST_NO_SUCH_MODEL_SNAPSHOT = "No model snapshot with id [{0}] exists for job [{1}]";
    public static final String REST_START_AFTER_END = "Invalid time range: end time ''{0}'' is earlier than start time ''{1}''.";
    public static final String REST_NO_SUCH_FORECAST = "No forecast(s) [{0}] exists for job [{1}]";
    public static final String REST_CANNOT_DELETE_FORECAST_IN_CURRENT_STATE =
        "Forecast(s) [{0}] for job [{1}] needs to be either FAILED or FINISHED to be deleted";
    public static final String FIELD_CANNOT_BE_NULL = "Field [{0}] cannot be null";

    private Messages() {
    }

    /**
     * Returns the message parameter
     *
     * @param message Should be one of the statics defined in this class
     */
    public static String getMessage(String message) {
        return message;
    }

    /**
     * Format the message with the supplied arguments
     *
     * @param message Should be one of the statics defined in this class
     * @param args MessageFormat arguments. See {@linkplain MessageFormat#format(Object)}]
     */
    public static String getMessage(String message, Object...args) {
        return new MessageFormat(message, Locale.ROOT).format(args);
    }
}
