/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe;

import java.text.MessageFormat;
import java.util.Locale;

public class DataFrameMessages {

    public static final String REST_STOP_JOB_WAIT_FOR_COMPLETION_TIMEOUT =
            "Timed out after [{0}] while waiting for data frame job [{1}] to stop";
    public static final String REST_STOP_JOB_WAIT_FOR_COMPLETION_INTERRUPT = "Interrupted while waiting for data frame job [{0}] to stop";
    public static final String REST_PUT_DATA_FRAME_JOB_EXISTS = "Job with id [{0}] already exists";
    public static final String REST_DELETE_DATA_FRAME_UNKNOWN_JOB = "Job with id [{0}] could not be found";
    public static final String REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION =
            "Failed to validate data frame configuration";
    public static final String REST_PUT_DATA_FRAME_FAILED_PERSIST_JOB_CONFIGURATION = "Failed to persist data frame configuration";
    public static final String REST_PUT_DATA_FRAME_FAILED_TO_DEDUCE_TARGET_MAPPINGS = "Failed to deduce target mappings";
    public static final String REST_PUT_DATA_FRAME_FAILED_TO_CREATE_TARGET_INDEX = "Failed to create target index";
    public static final String REST_PUT_DATA_FRAME_FAILED_TO_START_PERSISTENT_TASK =
            "Failed to start persistent task, configuration has been cleaned up: [{0}]";

    public static final String FAILED_TO_CREATE_DESTINATION_INDEX = "Could not create destination index [{0}] for job[{1}]";
    public static final String FAILED_TO_LOAD_JOB_CONFIGURATION = "Failed to load data frame job configuration for job [{0}]";
    public static final String FAILED_TO_PARSE_JOB_CONFIGURATION = "Failed to parse job configuration for data frame job [{0}]";

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
