/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe;

import org.elasticsearch.common.ParseField;

/*
 * Utility class to hold common fields and strings for data frame.
 */
public final class DataFrameField {

    // common parse fields
    public static final ParseField ID = new ParseField("id");
    public static final ParseField TRANSFORMS = new ParseField("transforms");
    public static final ParseField COUNT = new ParseField("count");
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField WAIT_FOR_COMPLETION = new ParseField("wait_for_completion");
    public static final ParseField STATS_FIELD = new ParseField("stats");

    // common strings
    public static final String TASK_NAME = "data_frame/transforms";
    public static final String REST_BASE_PATH = "/_data_frame/";
    public static final String REST_BASE_PATH_TRANSFORMS_BY_ID = REST_BASE_PATH + "transforms/{id}/";

    // note: this is used to match tasks
    public static final String PERSISTENT_TASK_DESCRIPTION_PREFIX = "data_frame_";

    private DataFrameField() {
    }
}
