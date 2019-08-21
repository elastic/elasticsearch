/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe;

import java.text.MessageFormat;
import java.util.Locale;

public class DataFrameMessages {

    public static final String REST_STOP_TRANSFORM_WAIT_FOR_COMPLETION_TIMEOUT =
            "Timed out after [{0}] while waiting for data frame transform [{1}] to stop";
    public static final String REST_STOP_TRANSFORM_WAIT_FOR_COMPLETION_INTERRUPT =
            "Interrupted while waiting for data frame transform [{0}] to stop";
    public static final String REST_PUT_DATA_FRAME_TRANSFORM_EXISTS = "Transform with id [{0}] already exists";
    public static final String REST_DATA_FRAME_UNKNOWN_TRANSFORM = "Transform with id [{0}] could not be found";
    public static final String REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION =
            "Failed to validate data frame configuration";
    public static final String REST_PUT_DATA_FRAME_FAILED_PERSIST_TRANSFORM_CONFIGURATION = "Failed to persist data frame configuration";
    public static final String REST_PUT_DATA_FRAME_FAILED_TO_DEDUCE_DEST_MAPPINGS = "Failed to deduce dest mappings";
    public static final String REST_PUT_DATA_FRAME_SOURCE_INDEX_MISSING = "Source index [{0}] does not exist";
    public static final String REST_PUT_DATA_FRAME_DEST_IN_SOURCE = "Destination index [{0}] is included in source expression [{1}]";
    public static final String REST_PUT_DATA_FRAME_DEST_SINGLE_INDEX = "Destination index [{0}] should refer to a single index";
    public static final String REST_PUT_DATA_FRAME_INCONSISTENT_ID =
            "Inconsistent id; ''{0}'' specified in the body differs from ''{1}'' specified as a URL argument";
    public static final String DATA_FRAME_CONFIG_INVALID = "Data frame transform configuration is invalid [{0}]";
    public static final String REST_DATA_FRAME_FAILED_TO_SERIALIZE_TRANSFORM = "Failed to serialise transform [{0}]";
    public static final String DATA_FRAME_FAILED_TO_PERSIST_STATS = "Failed to persist data frame statistics for transform [{0}]";
    public static final String DATA_FRAME_UNKNOWN_TRANSFORM_STATS = "Statistics for transform [{0}] could not be found";

    public static final String DATA_FRAME_CANNOT_STOP_FAILED_TRANSFORM =
        "Unable to stop data frame transform [{0}] as it is in a failed state with reason [{1}]." +
            " Use force stop to stop the data frame transform.";
    public static final String DATA_FRAME_CANNOT_START_FAILED_TRANSFORM =
        "Unable to start data frame transform [{0}] as it is in a failed state with failure: [{1}]. " +
            "Use force start to restart data frame transform once error is resolved.";

    public static final String FAILED_TO_CREATE_DESTINATION_INDEX = "Could not create destination index [{0}] for transform [{1}]";
    public static final String FAILED_TO_RELOAD_TRANSFORM_CONFIGURATION =
        "Failed to reload data frame transform configuration for transform [{0}]";
    public static final String FAILED_TO_LOAD_TRANSFORM_CONFIGURATION =
            "Failed to load data frame transform configuration for transform [{0}]";
    public static final String FAILED_TO_PARSE_TRANSFORM_CONFIGURATION =
            "Failed to parse transform configuration for data frame transform [{0}]";
    public static final String FAILED_TO_PARSE_TRANSFORM_STATISTICS_CONFIGURATION =
            "Failed to parse transform statistics for data frame transform [{0}]";
    public static final String FAILED_TO_LOAD_TRANSFORM_CHECKPOINT =
            "Failed to load data frame transform checkpoint for transform [{0}]";
    public static final String FAILED_TO_LOAD_TRANSFORM_STATE =
            "Failed to load data frame transform state for transform [{0}]";
    public static final String DATA_FRAME_TRANSFORM_CONFIGURATION_NO_TRANSFORM =
            "Data frame transform configuration must specify exactly 1 function";
    public static final String DATA_FRAME_TRANSFORM_CONFIGURATION_PIVOT_NO_GROUP_BY =
            "Data frame pivot transform configuration must specify at least 1 group_by";
    public static final String DATA_FRAME_TRANSFORM_CONFIGURATION_PIVOT_NO_AGGREGATION =
            "Data frame pivot transform configuration must specify at least 1 aggregation";
    public static final String DATA_FRAME_TRANSFORM_PIVOT_FAILED_TO_CREATE_COMPOSITE_AGGREGATION =
            "Failed to create composite aggregation from pivot function";
    public static final String DATA_FRAME_TRANSFORM_CONFIGURATION_INVALID =
            "Data frame transform configuration [{0}] has invalid elements";
    public static final String DATA_FRAME_UNABLE_TO_GATHER_FIELD_MAPPINGS = "Failed to gather field mappings for index [{0}]";
    public static final String DATA_FRAME_UPDATE_CANNOT_CHANGE_SYNC_METHOD =
        "Cannot change the current sync configuration of transform [{0}] from [{1}] to [{2}]";
    public static final String LOG_DATA_FRAME_TRANSFORM_CONFIGURATION_BAD_QUERY =
            "Failed to parse query for data frame transform";
    public static final String LOG_DATA_FRAME_TRANSFORM_CONFIGURATION_BAD_GROUP_BY =
            "Failed to parse group_by for data frame pivot transform";
    public static final String LOG_DATA_FRAME_TRANSFORM_CONFIGURATION_BAD_AGGREGATION =
            "Failed to parse aggregation for data frame pivot transform";
    public static final String LOG_DATA_FRAME_TRANSFORM_PIVOT_REDUCE_PAGE_SIZE =
            "Insufficient memory for search, reducing number of buckets per search from [{0}] to [{1}]";
    public static final String LOG_DATA_FRAME_TRANSFORM_PIVOT_LOW_PAGE_SIZE_FAILURE =
            "Insufficient memory for search after repeated page size reductions to [{0}], unable to continue pivot, "
            + "please simplify job or increase heap size on data nodes.";

    public static final String FAILED_TO_PARSE_TRANSFORM_CHECKPOINTS =
            "Failed to parse transform checkpoints for [{0}]";


    public static final String ID_TOO_LONG = "The id cannot contain more than {0} characters.";
    public static final String INVALID_ID = "Invalid {0}; ''{1}'' can contain lowercase alphanumeric (a-z and 0-9), hyphens or " +
        "underscores; must start and end with alphanumeric";
    private DataFrameMessages() {
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
    public static String getMessage(String message, Object... args) {
        return new MessageFormat(message, Locale.ROOT).format(args);
    }
}
