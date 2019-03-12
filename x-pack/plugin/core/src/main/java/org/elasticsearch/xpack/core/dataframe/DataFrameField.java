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
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField AGGS = new ParseField("aggs");
    public static final ParseField ID = new ParseField("id");
    public static final ParseField TRANSFORMS = new ParseField("transforms");
    public static final ParseField COUNT = new ParseField("count");
    public static final ParseField GROUP_BY = new ParseField("group_by");
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField WAIT_FOR_COMPLETION = new ParseField("wait_for_completion");
    public static final ParseField STATS_FIELD = new ParseField("stats");
    public static final ParseField INDEX_DOC_TYPE = new ParseField("doc_type");
    public static final ParseField SOURCE = new ParseField("source");
    public static final ParseField DESTINATION = new ParseField("dest");

    // common strings
    public static final String TASK_NAME = "data_frame/transforms";
    public static final String REST_BASE_PATH = "/_data_frame/";
    public static final String REST_BASE_PATH_TRANSFORMS = REST_BASE_PATH + "transforms/";
    public static final String REST_BASE_PATH_TRANSFORMS_BY_ID = REST_BASE_PATH_TRANSFORMS + "{id}/";

    // note: this is used to match tasks
    public static final String PERSISTENT_TASK_DESCRIPTION_PREFIX = "data_frame_";

    // strings for meta information
    public static final String META_FIELDNAME = "_data_frame";
    public static final String CREATION_DATE_MILLIS = "creation_date_in_millis";
    public static final String VERSION = "version";
    public static final String CREATED = "created";
    public static final String CREATED_BY = "created_by";
    public static final String TRANSFORM = "transform";
    public static final String DATA_FRAME_SIGNATURE = "data-frame-transform";

    /**
     * Parameter to indicate whether we are serialising to X Content for internal storage. Default the field is invisible (e.g. for get
     * API's)
     */
    public static final String FOR_INTERNAL_STORAGE = "for_internal_storage";

    private DataFrameField() {
    }
}
