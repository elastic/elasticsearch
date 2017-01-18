/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.messages;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Defines the keys for all the message strings
 */
public final class Messages {
    /**
     * The base name of the bundle without the .properties extension
     * or locale
     */
    private static final String BUNDLE_NAME = "org.elasticsearch.xpack.ml.job.messages.ml_messages";
    public static final String AUTODETECT_FLUSH_UNEXPTECTED_DEATH = "autodetect.flush.failed.unexpected.death";
    public static final String AUTODETECT_FLUSH_TIMEOUT = "autodetect.flush.timeout";

    public static final String CPU_LIMIT_JOB = "cpu.limit.jobs";

    public static final String DATASTORE_ERROR_DELETING = "datastore.error.deleting";
    public static final String DATASTORE_ERROR_DELETING_MISSING_INDEX = "datastore.error.deleting.missing.index";
    public static final String DATASTORE_ERROR_EXECUTING_SCRIPT = "datastore.error.executing.script";

    public static final String INVALID_ID = "invalid.id";
    public static final String INCONSISTENT_ID = "inconsistent.id";

    public static final String LICENSE_LIMIT_DETECTORS = "license.limit.detectors";
    public static final String LICENSE_LIMIT_JOBS = "license.limit.jobs";
    public static final String LICENSE_LIMIT_DETECTORS_REACTIVATE = "license.limit.detectors.reactivate";
    public static final String LICENSE_LIMIT_JOBS_REACTIVATE = "license.limit.jobs.reactivate";
    public static final String LICENSE_LIMIT_PARTITIONS = "license.limit.partitions";

    public static final String LOGFILE_INVALID_CHARS_IN_PATH = "logfile.invalid.chars.path";
    public static final String LOGFILE_INVALID_PATH = "logfile.invalid.path";
    public static final String LOGFILE_MISSING = "logfile.missing";
    public static final String LOGFILE_MISSING_DIRECTORY = "logfile.missing.directory";


    public static final String JOB_AUDIT_CREATED = "job.audit.created";
    public static final String JOB_AUDIT_DELETED = "job.audit.deleted";
    public static final String JOB_AUDIT_PAUSED = "job.audit.paused";
    public static final String JOB_AUDIT_RESUMED = "job.audit.resumed";
    public static final String JOB_AUDIT_UPDATED = "job.audit.updated";
    public static final String JOB_AUDIT_REVERTED = "job.audit.reverted";
    public static final String JOB_AUDIT_OLD_RESULTS_DELETED = "job.audit.old.results.deleted";
    public static final String JOB_AUDIT_SNAPSHOT_DELETED = "job.audit.snapshot.deleted";
    public static final String JOB_AUDIT_SCHEDULER_STARTED_FROM_TO = "job.audit.scheduler.started.from.to";
    public static final String JOB_AUDIT_SCHEDULER_CONTINUED_REALTIME = "job.audit.scheduler.continued.realtime";
    public static final String JOB_AUDIT_SCHEDULER_STARTED_REALTIME = "job.audit.scheduler.started.realtime";
    public static final String JOB_AUDIT_SCHEDULER_LOOKBACK_COMPLETED = "job.audit.scheduler.lookback.completed";
    public static final String JOB_AUDIT_SCHEDULER_STOPPED = "job.audit.scheduler.stopped";
    public static final String JOB_AUDIT_SCHEDULER_NO_DATA = "job.audit.scheduler.no.data";
    public static final String JOB_AUDIR_SCHEDULER_DATA_SEEN_AGAIN = "job.audit.scheduler.data.seen.again";
    public static final String JOB_AUDIT_SCHEDULER_DATA_ANALYSIS_ERROR = "job.audit.scheduler.data.analysis.error";
    public static final String JOB_AUDIT_SCHEDULER_DATA_EXTRACTION_ERROR = "job.audit.scheduler.data.extraction.error";
    public static final String JOB_AUDIT_SCHEDULER_RECOVERED = "job.audit.scheduler.recovered";

    public static final String SYSTEM_AUDIT_STARTED = "system.audit.started";
    public static final String SYSTEM_AUDIT_SHUTDOWN = "system.audit.shutdown";

    public static final String JOB_CANNOT_DELETE_WHILE_RUNNING = "job.cannot.delete.while.running";
    public static final String JOB_CANNOT_PAUSE = "job.cannot.pause";
    public static final String JOB_CANNOT_RESUME = "job.cannot.resume";

    public static final String JOB_CONFIG_BYFIELD_INCOMPATIBLE_FUNCTION = "job.config.byField.incompatible.function";
    public static final String JOB_CONFIG_BYFIELD_NEEDS_ANOTHER = "job.config.byField.needs.another";
    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_REQUIRE_CATEGORIZATION_FIELD_NAME = "job.config.categorization.filters."
            + "require.categorization.field.name";
    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_DUPLICATES = "job.config.categorization.filters.contains"
            + ".duplicates";
    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_EMPTY = "job.config.categorization.filter.contains.empty";
    public static final String JOB_CONFIG_CATEGORIZATION_FILTERS_CONTAINS_INVALID_REGEX = "job.config.categorization.filter.contains."
            + "invalid.regex";
    public static final String JOB_CONFIG_CONDITION_INVALID_OPERATOR = "job.config.condition.invalid.operator";
    public static final String JOB_CONFIG_CONDITION_INVALID_VALUE_NULL = "job.config.condition.invalid.value.null";
    public static final String JOB_CONFIG_CONDITION_INVALID_VALUE_NUMBER = "job.config.condition.invalid.value.numeric";
    public static final String JOB_CONFIG_CONDITION_INVALID_VALUE_REGEX = "job.config.condition.invalid.value.regex";
    public static final String JOB_CONFIG_CONDITION_UNKNOWN_OPERATOR = "job.config.condition.unknown.operator";
    public static final String JOB_CONFIG_DATAFORMAT_REQUIRES_TRANSFORM = "job.config.dataformat.requires.transform";
    public static final String JOB_CONFIG_DETECTION_RULE_CONDITION_CATEGORICAL_INVALID_OPTION = "job.config.detectionrule.condition."
            + "categorical.invalid.option";
    public static final String JOB_CONFIG_DETECTION_RULE_CONDITION_CATEGORICAL_MISSING_OPTION = "job.config.detectionrule.condition."
            + "categorical.missing.option";
    public static final String JOB_CONFIG_DETECTION_RULE_CONDITION_INVALID_FIELD_NAME = "job.config.detectionrule.condition.invalid."
            + "fieldname";
    public static final String JOB_CONFIG_DETECTION_RULE_CONDITION_MISSING_FIELD_NAME = "job.config.detectionrule.condition.missing."
            + "fieldname";
    public static final String JOB_CONFIG_DETECTION_RULE_CONDITION_NUMERICAL_INVALID_OPERATOR = "job.config.detectionrule.condition."
            + "numerical.invalid.operator";
    public static final String JOB_CONFIG_DETECTION_RULE_CONDITION_NUMERICAL_INVALID_OPTION = "job.config.detectionrule.condition."
            + "numerical.invalid.option";
    public static final String JOB_CONFIG_DETECTION_RULE_CONDITION_NUMERICAL_MISSING_OPTION = "job.config.detectionrule.condition."
            + "numerical.missing.option";
    public static final String JOB_CONFIG_DETECTION_RULE_CONDITION_NUMERICAL_WITH_FIELD_NAME_REQUIRES_FIELD_VALUE = "job.config."
            + "detectionrule.condition.numerical.with.fieldname.requires.fieldvalue";
    public static final String JOB_CONFIG_DETECTION_RULE_INVALID_TARGET_FIELD_NAME = "job.config.detectionrule.invalid.targetfieldname";
    public static final String JOB_CONFIG_DETECTION_RULE_MISSING_TARGET_FIELD_NAME = "job.config.detectionrule.missing.targetfieldname";
    public static final String JOB_CONFIG_DETECTION_RULE_NOT_SUPPORTED_BY_FUNCTION = "job.config.detectionrule.not.supported.by.function";
    public static final String JOB_CONFIG_DETECTION_RULE_REQUIRES_AT_LEAST_ONE_CONDITION = "job.config.detectionrule.requires.at."
            + "least.one.condition";
    public static final String JOB_CONFIG_FIELDNAME_INCOMPATIBLE_FUNCTION = "job.config.fieldname.incompatible.function";
    public static final String JOB_CONFIG_FUNCTION_REQUIRES_BYFIELD = "job.config.function.requires.byfield";
    public static final String JOB_CONFIG_FUNCTION_REQUIRES_FIELDNAME = "job.config.function.requires.fieldname";
    public static final String JOB_CONFIG_FUNCTION_REQUIRES_OVERFIELD = "job.config.function.requires.overfield";
    public static final String JOB_CONFIG_ID_TOO_LONG = "job.config.id.too.long";
    public static final String JOB_CONFIG_ID_ALREADY_TAKEN = "job.config.id.already.taken";
    public static final String JOB_CONFIG_INVALID_FIELDNAME_CHARS = "job.config.invalid.fieldname.chars";
    public static final String JOB_CONFIG_INVALID_TIMEFORMAT = "job.config.invalid.timeformat";
    public static final String JOB_CONFIG_FUNCTION_INCOMPATIBLE_PRESUMMARIZED = "job.config.function.incompatible.presummarized";
    public static final String JOB_CONFIG_MISSING_ANALYSISCONFIG = "job.config.missing.analysisconfig";
    public static final String JOB_CONFIG_MODEL_DEBUG_CONFIG_INVALID_BOUNDS_PERCENTILE = "job.config.model.debug.config.invalid.bounds."
            + "percentile";
    public static final String JOB_CONFIG_FIELD_VALUE_TOO_LOW = "job.config.field.value.too.low";
    public static final String JOB_CONFIG_NO_ANALYSIS_FIELD = "job.config.no.analysis.field";
    public static final String JOB_CONFIG_NO_ANALYSIS_FIELD_NOT_COUNT = "job.config.no.analysis.field.not.count";
    public static final String JOB_CONFIG_NO_DETECTORS = "job.config.no.detectors";
    public static final String JOB_CONFIG_OVERFIELD_INCOMPATIBLE_FUNCTION = "job.config.overField.incompatible.function";
    public static final String JOB_CONFIG_OVERLAPPING_BUCKETS_INCOMPATIBLE_FUNCTION = "job.config.overlapping.buckets.incompatible."
            + "function";
    public static final String JOB_CONFIG_OVERFIELD_NEEDS_ANOTHER = "job.config.overField.needs.another";
    public static final String JOB_CONFIG_MULTIPLE_BUCKETSPANS_REQUIRE_BUCKETSPAN = "job.config.multiple.bucketspans.require.bucket_span";
    public static final String JOB_CONFIG_MULTIPLE_BUCKETSPANS_MUST_BE_MULTIPLE = "job.config.multiple.bucketspans.must.be.multiple";
    public static final String JOB_CONFIG_PER_PARTITION_NORMALIZATION_REQUIRES_PARTITION_FIELD = "job.config.per.partition.normalization."
            + "requires.partition.field";
    public static final String JOB_CONFIG_PER_PARTITION_NORMALIZATION_CANNOT_USE_INFLUENCERS = "job.config.per.partition.normalization."
            + "cannot.use.influencers";


    public static final String JOB_CONFIG_UPDATE_ANALYSIS_LIMITS_PARSE_ERROR = "job.config.update.analysis.limits.parse.error";
    public static final String JOB_CONFIG_UPDATE_ANALYSIS_LIMITS_CANNOT_BE_NULL = "job.config.update.analysis.limits.cannot.be.null";
    public static final String JOB_CONFIG_UPDATE_ANALYSIS_LIMITS_MODEL_MEMORY_LIMIT_CANNOT_BE_DECREASED = "job.config.update.analysis."
            + "limits.model.memory.limit.cannot.be.decreased";
    public static final String JOB_CONFIG_UPDATE_CATEGORIZATION_FILTERS_INVALID = "job.config.update.categorization.filters.invalid";
    public static final String JOB_CONFIG_UPDATE_CUSTOM_SETTINGS_INVALID = "job.config.update.custom.settings.invalid";
    public static final String JOB_CONFIG_UPDATE_DESCRIPTION_INVALID = "job.config.update.description.invalid";
    public static final String JOB_CONFIG_UPDATE_DETECTORS_INVALID = "job.config.update.detectors.invalid";
    public static final String JOB_CONFIG_UPDATE_DETECTORS_INVALID_DETECTOR_INDEX = "job.config.update.detectors.invalid.detector.index";
    public static final String JOB_CONFIG_UPDATE_DETECTORS_DETECTOR_INDEX_SHOULD_BE_INTEGER = "job.config.update.detectors.detector.index."
            + "should.be.integer";
    public static final String JOB_CONFIG_UPDATE_DETECTORS_MISSING_PARAMS = "job.config.update.detectors.missing.params";
    public static final String JOB_CONFIG_UPDATE_DETECTORS_DESCRIPTION_SHOULD_BE_STRING = "job.config.update.detectors.description.should"
            + ".be.string";
    public static final String JOB_CONFIG_UPDATE_DETECTOR_RULES_PARSE_ERROR = "job.config.update.detectors.rules.parse.error";
    public static final String JOB_CONFIG_UPDATE_FAILED = "job.config.update.failed";
    public static final String JOB_CONFIG_UPDATE_INVALID_KEY = "job.config.update.invalid.key";
    public static final String JOB_CONFIG_UPDATE_IGNORE_DOWNTIME_PARSE_ERROR = "job.config.update.ignore.downtime.parse.error";
    public static final String JOB_CONFIG_UPDATE_JOB_IS_NOT_CLOSED = "job.config.update.job.is.not.closed";
    public static final String JOB_CONFIG_UPDATE_MODEL_DEBUG_CONFIG_PARSE_ERROR = "job.config.update.model.debug.config.parse.error";
    public static final String JOB_CONFIG_UPDATE_REQUIRES_NON_EMPTY_OBJECT = "job.config.update.requires.non.empty.object";
    public static final String JOB_CONFIG_UPDATE_PARSE_ERROR = "job.config.update.parse.error";
    public static final String JOB_CONFIG_UPDATE_BACKGROUND_PERSIST_INTERVAL_INVALID = "job.config.update.background.persist.interval."
            + "invalid";
    public static final String JOB_CONFIG_UPDATE_RENORMALIZATION_WINDOW_DAYS_INVALID = "job.config.update.renormalization.window.days."
            + "invalid";
    public static final String JOB_CONFIG_UPDATE_MODEL_SNAPSHOT_RETENTION_DAYS_INVALID = "job.config.update.model.snapshot.retention.days."
            + "invalid";
    public static final String JOB_CONFIG_UPDATE_RESULTS_RETENTION_DAYS_INVALID = "job.config.update.results.retention.days.invalid";
    public static final String JOB_CONFIG_UPDATE_SCHEDULE_CONFIG_PARSE_ERROR = "job.config.update.scheduler.config.parse.error";
    public static final String JOB_CONFIG_UPDATE_SCHEDULE_CONFIG_CANNOT_BE_NULL = "job.config.update.scheduler.config.cannot.be.null";

    public static final String JOB_CONFIG_TRANSFORM_CIRCULAR_DEPENDENCY = "job.config.transform.circular.dependency";
    public static final String JOB_CONFIG_TRANSFORM_CONDITION_REQUIRED = "job.config.transform.condition.required";
    public static final String JOB_CONFIG_TRANSFORM_DUPLICATED_OUTPUT_NAME = "job.config.transform.duplicated.output.name";
    public static final String JOB_CONFIG_TRANSFORM_EXTRACT_GROUPS_SHOULD_MATCH_OUTPUT_COUNT = "job.config.transform.extract.groups.should."
            + "match.output.count";
    public static final String JOB_CONFIG_TRANSFORM_INPUTS_CONTAIN_EMPTY_STRING = "job.config.transform.inputs.contain.empty.string";
    public static final String JOB_CONFIG_TRANSFORM_INVALID_ARGUMENT = "job.config.transform.invalid.argument";
    public static final String JOB_CONFIG_TRANSFORM_INVALID_ARGUMENT_COUNT = "job.config.transform.invalid.argument.count";
    public static final String JOB_CONFIG_TRANSFORM_INVALID_INPUT_COUNT = "job.config.transform.invalid.input.count";
    public static final String JOB_CONFIG_TRANSFORM_INVALID_OUTPUT_COUNT = "job.config.transform.invalid.output.count";
    public static final String JOB_CONFIG_TRANSFORM_OUTPUTS_CONTAIN_EMPTY_STRING = "job.config.transform.outputs.contain.empty.string";
    public static final String JOB_CONFIG_TRANSFORM_OUTPUTS_UNUSED = "job.config.transform.outputs.unused";
    public static final String JOB_CONFIG_TRANSFORM_OUTPUT_NAME_USED_MORE_THAN_ONCE = "job.config.transform.output.name.used.more.than"
            + ".once";
    public static final String JOB_CONFIG_TRANSFORM_UNKNOWN_TYPE = "job.config.transform.unknown.type";
    public static final String JOB_CONFIG_UNKNOWN_FUNCTION = "job.config.unknown.function";

    public static final String JOB_INDEX_ALREADY_EXISTS = "job.index.already.exists";

    public static final String JOB_DATA_CONCURRENT_USE_CLOSE = "job.data.concurrent.use.close";
    public static final String JOB_DATA_CONCURRENT_USE_FLUSH = "job.data.concurrent.use.flush";
    public static final String JOB_DATA_CONCURRENT_USE_PAUSE = "job.data.concurrent.use.pause";
    public static final String JOB_DATA_CONCURRENT_USE_RESUME = "job.data.concurrent.use.resume";
    public static final String JOB_DATA_CONCURRENT_USE_REVERT = "job.data.concurrent.use.revert";
    public static final String JOB_DATA_CONCURRENT_USE_UPDATE = "job.data.concurrent.use.update";
    public static final String JOB_DATA_CONCURRENT_USE_UPLOAD = "job.data.concurrent.use.upload";

    public static final String SCHEDULER_CONFIG_INVALID_OPTION_VALUE = "scheduler.config.invalid.option.value";

    public static final String SCHEDULER_DOES_NOT_SUPPORT_JOB_WITH_LATENCY = "scheduler.does.not.support.job.with.latency";
    public static final String SCHEDULER_AGGREGATIONS_REQUIRES_JOB_WITH_SUMMARY_COUNT_FIELD =
            "scheduler.aggregations.requires.job.with.summary.count.field";

    public static final String SCHEDULER_CANNOT_START = "scheduler.cannot.start";
    public static final String SCHEDULER_CANNOT_STOP_IN_CURRENT_STATE = "scheduler.cannot.stop.in.current.state";
    public static final String SCHEDULER_CANNOT_UPDATE_IN_CURRENT_STATE = "scheduler.cannot.update.in.current.state";
    public static final String SCHEDULER_CANNOT_DELETE_IN_CURRENT_STATE = "scheduler.cannot.delete.in.current.state";
    public static final String SCHEDULER_FAILED_TO_STOP = "scheduler.failed.to.stop";
    public static final String SCHEDULER_NOT_FOUND = "scheduler.not.found";

    public static final String JOB_MISSING_QUANTILES = "job.missing.quantiles";
    public static final String JOB_UNKNOWN_ID = "job.unknown.id";

    public static final String JSON_JOB_CONFIG_MAPPING = "json.job.config.mapping.error";
    public static final String JSON_JOB_CONFIG_PARSE = "json.job.config.parse.error";

    public static final String JSON_DETECTOR_CONFIG_MAPPING = "json.detector.config.mapping.error";
    public static final String JSON_DETECTOR_CONFIG_PARSE = "json.detector.config.parse.error";

    public static final String JSON_LIST_DOCUMENT_MAPPING_ERROR = "json.list.document.mapping.error";
    public static final String JSON_LIST_DOCUMENT_PARSE_ERROR = "json.list.document.parse.error";

    public static final String JSON_TRANSFORM_CONFIG_MAPPING = "json.transform.config.mapping.error";
    public static final String JSON_TRANSFORM_CONFIG_PARSE = "json.transform.config.parse.error";

    public static final String ON_HOST = "on.host";

    public static final String REST_ACTION_NOT_ALLOWED_FOR_SCHEDULED_JOB = "rest.action.not.allowed.for.scheduled.job";

    public static final String REST_INVALID_DATETIME_PARAMS = "rest.invalid.datetime.params";
    public static final String REST_INVALID_FLUSH_PARAMS_MISSING = "rest.invalid.flush.params.missing.argument";
    public static final String REST_INVALID_FLUSH_PARAMS_UNEXPECTED = "rest.invalid.flush.params.unexpected";
    public static final String REST_INVALID_RESET_PARAMS = "rest.invalid.reset.params";
    public static final String REST_INVALID_FROM = "rest.invalid.from";
    public static final String REST_INVALID_SIZE = "rest.invalid.size";
    public static final String REST_INVALID_FROM_SIZE_SUM = "rest.invalid.from.size.sum";
    public static final String REST_GZIP_ERROR = "rest.gzip.error";
    public static final String REST_START_AFTER_END = "rest.start.after.end";
    public static final String REST_RESET_BUCKET_NO_LATENCY = "rest.reset.bucket.no.latency";
    public static final String REST_INVALID_REVERT_PARAMS = "rest.invalid.revert.params";
    public static final String REST_JOB_NOT_CLOSED_REVERT = "rest.job.not.closed.revert";
    public static final String REST_NO_SUCH_MODEL_SNAPSHOT = "rest.no.such.model.snapshot";
    public static final String REST_INVALID_DESCRIPTION_PARAMS = "rest.invalid.description.params";
    public static final String REST_DESCRIPTION_ALREADY_USED = "rest.description.already.used";
    public static final String REST_CANNOT_DELETE_HIGHEST_PRIORITY = "rest.cannot.delete.highest.priority";

    public static final String REST_ALERT_MISSING_ARGUMENT = "rest.alert.missing.argument";
    public static final String REST_ALERT_INVALID_TIMEOUT = "rest.alert.invalid.timeout";
    public static final String REST_ALERT_INVALID_THRESHOLD = "rest.alert.invalid.threshold";
    public static final String REST_ALERT_CANT_USE_PROB = "rest.alert.cant.use.prob";
    public static final String REST_ALERT_INVALID_TYPE = "rest.alert.invalid.type";

    public static final String PROCESS_ACTION_SLEEPING_JOB = "process.action.sleeping.job";
    public static final String PROCESS_ACTION_CLOSED_JOB = "process.action.closed.job";
    public static final String PROCESS_ACTION_CLOSING_JOB = "process.action.closing.job";
    public static final String PROCESS_ACTION_DELETING_JOB = "process.action.deleting.job";
    public static final String PROCESS_ACTION_FLUSHING_JOB = "process.action.flushing.job";
    public static final String PROCESS_ACTION_PAUSING_JOB = "process.action.pausing.job";
    public static final String PROCESS_ACTION_RESUMING_JOB = "process.action.resuming.job";
    public static final String PROCESS_ACTION_REVERTING_JOB = "process.action.reverting.job";
    public static final String PROCESS_ACTION_UPDATING_JOB = "process.action.updating.job";
    public static final String PROCESS_ACTION_WRITING_JOB = "process.action.writing.job";

    public static final String SUPPORT_BUNDLE_SCRIPT_ERROR = "support.bundle.script.error";

    private Messages() {
    }

    public static ResourceBundle load() {
        return ResourceBundle.getBundle(Messages.BUNDLE_NAME, Locale.getDefault());
    }

    /**
     * Look up the message string from the resource bundle.
     *
     * @param key Must be one of the statics defined in this file]
     */
    public static String getMessage(String key) {
        return load().getString(key);
    }

    /**
     * Look up the message string from the resource bundle and format with
     * the supplied arguments
     * @param key the key for the message
     * @param args MessageFormat arguments. See {@linkplain MessageFormat#format(Object)}]
     */
    public static String getMessage(String key, Object...args) {
        return new MessageFormat(load().getString(key), Locale.ROOT).format(args);
    }
}
