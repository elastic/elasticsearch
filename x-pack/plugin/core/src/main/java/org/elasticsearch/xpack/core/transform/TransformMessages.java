/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import java.text.MessageFormat;
import java.util.Locale;

public class TransformMessages {

    public static final String REST_STOP_TRANSFORM_WAIT_FOR_COMPLETION_TIMEOUT =
        "Timed out after [{0}] while waiting for transform [{1}] to stop";
    public static final String REST_STOP_TRANSFORM_WAIT_FOR_COMPLETION_INTERRUPT = "Interrupted while waiting for transform [{0}] to stop";
    public static final String REST_PUT_TRANSFORM_EXISTS = "Transform with id [{0}] already exists";
    public static final String REST_UNKNOWN_TRANSFORM = "Transform with id [{0}] could not be found";
    public static final String REST_STOP_TRANSFORM_WITHOUT_CONFIG =
        "Detected transforms with no config [{0}]. Use force to stop/delete them.";
    public static final String REST_PUT_FAILED_PERSIST_TRANSFORM_CONFIGURATION = "Failed to persist transform configuration";
    public static final String REST_PUT_TRANSFORM_FAILED_TO_DEDUCE_DEST_MAPPINGS = "Failed to deduce dest mappings";
    public static final String REST_PUT_TRANSFORM_INCONSISTENT_ID =
        "Inconsistent id; ''{0}'' specified in the body differs from ''{1}'' specified as a URL argument";
    public static final String REST_FAILED_TO_SERIALIZE_TRANSFORM = "Failed to serialise transform [{0}]";
    public static final String TRANSFORM_FAILED_TO_PERSIST_STATS = "Failed to persist transform statistics for transform [{0}]";
    public static final String UNKNOWN_TRANSFORM_STATS = "Statistics for transform [{0}] could not be found";

    public static final String FAILED_TO_UNSET_RESET_MODE =
        "Failed to set [reset_mode] to [false] after {0}. To allow transforms to run, please call the feature reset API again";

    public static final String REST_WARN_NO_TRANSFORM_NODES =
        "Transform requires the transform node role for at least 1 node, found no transform nodes";

    public static final String CANNOT_STOP_FAILED_TRANSFORM = "Unable to stop transform [{0}] as it is in a failed state with reason [{1}]."
        + " Use force stop to stop the transform.";
    public static final String CANNOT_START_FAILED_TRANSFORM =
        "Unable to start transform [{0}] as it is in a failed state with failure: [{1}]. "
            + "Use force stop and then restart the transform once error is resolved.";

    public static final String FAILED_TO_CREATE_DESTINATION_INDEX = "Could not create destination index [{0}] for transform [{1}]";
    public static final String FAILED_TO_RELOAD_TRANSFORM_CONFIGURATION = "Failed to reload transform configuration for transform [{0}]";
    public static final String FAILED_TO_LOAD_TRANSFORM_CONFIGURATION = "Failed to load transform configuration for transform [{0}]";
    public static final String FAILED_TO_PARSE_TRANSFORM_CONFIGURATION = "Failed to parse transform configuration for transform [{0}]";
    public static final String FAILED_TO_PARSE_TRANSFORM_STATISTICS_CONFIGURATION =
        "Failed to parse transform statistics for transform [{0}]";
    public static final String FAILED_TO_LOAD_TRANSFORM_CHECKPOINT = "Failed to load transform checkpoint for transform [{0}]";
    public static final String FAILED_TO_LOAD_TRANSFORM_STATE = "Failed to load transform state for transform [{0}]";
    public static final String TRANSFORM_CONFIGURATION_BAD_FUNCTION_COUNT = "Transform configuration must specify exactly 1 function";
    public static final String TRANSFORM_CONFIGURATION_PIVOT_NO_GROUP_BY = "Pivot transform configuration must specify at least 1 group_by";
    public static final String TRANSFORM_CONFIGURATION_PIVOT_NO_AGGREGATION =
        "Pivot transform configuration must specify at least 1 aggregation";
    public static final String TRANSFORM_FAILED_TO_CREATE_COMPOSITE_AGGREGATION =
        "Failed to create composite aggregation from {0} function";
    public static final String TRANSFORM_CONFIGURATION_INVALID = "Transform configuration [{0}] has invalid elements: [{1}]";
    public static final String TRANSFORM_CONFIGURATION_DEPRECATED = "Transform configuration is at version [{0}]. Use [{1}] or ["
        + TransformField.REST_BASE_PATH_TRANSFORMS
        + "_upgrade] to update.";
    public static final String UNABLE_TO_GATHER_FIELD_MAPPINGS = "Failed to gather field mappings for index [{0}]";
    public static final String TRANSFORM_UPDATE_CANNOT_CHANGE_SYNC_METHOD =
        "Cannot change the current sync configuration of transform [{0}] from [{1}] to [{2}]";
    public static final String LOG_TRANSFORM_CONFIGURATION_BAD_QUERY = "Failed to parse query for transform";
    public static final String LOG_TRANSFORM_CONFIGURATION_BAD_SCRIPT = "Failed to parse script for transform";
    public static final String LOG_TRANSFORM_CONFIGURATION_BAD_GROUP_BY = "Failed to parse group_by for pivot transform";
    public static final String LOG_TRANSFORM_CONFIGURATION_BAD_AGGREGATION = "Failed to parse aggregation for pivot transform";
    public static final String LOG_TRANSFORM_PIVOT_REDUCE_PAGE_SIZE =
        "Insufficient memory for search, reducing number of buckets per search from [{0}] to [{1}]";
    public static final String LOG_TRANSFORM_PIVOT_LOW_PAGE_SIZE_FAILURE =
        "Insufficient memory for search after repeated page size reductions to [{0}], unable to continue pivot, "
            + "please simplify job or increase heap size on data nodes.";
    public static final String LOG_TRANSFORM_PIVOT_SCRIPT_ERROR = "Failed to execute script with error: [{0}], stack trace: {1}";
    public static final String LOG_TRANSFORM_PIVOT_IRRECOVERABLE_BULK_INDEXING_ERROR =
        "Failed to index documents into destination index due to permanent error: [{0}]";

    public static final String FAILED_TO_PARSE_TRANSFORM_CHECKPOINTS = "Failed to parse transform checkpoints for [{0}]";

    public static final String ID_TOO_LONG = "The id cannot contain more than {0} characters.";
    public static final String INVALID_ID = "Invalid {0}; ''{1}'' can contain lowercase alphanumeric (a-z and 0-9), hyphens or "
        + "underscores; must start and end with alphanumeric";

    private TransformMessages() {}

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
    public static String getMessage(String message, Object... args) {
        return new MessageFormat(message, Locale.ROOT).format(args);
    }
}
