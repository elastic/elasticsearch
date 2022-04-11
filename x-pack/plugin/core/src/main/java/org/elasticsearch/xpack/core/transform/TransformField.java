/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.xcontent.ParseField;

/*
 * Utility class to hold common fields and strings for transform.
 */
public final class TransformField {

    // common parse fields
    public static final ParseField AGGREGATIONS = new ParseField("aggregations");
    public static final ParseField AGGS = new ParseField("aggs");
    public static final ParseField ID = new ParseField("id");
    public static final ParseField TRANSFORMS = new ParseField("transforms");
    public static final ParseField COUNT = new ParseField("count");
    public static final ParseField GROUP_BY = new ParseField("group_by");
    public static final ParseField TIMEOUT = new ParseField("timeout");
    public static final ParseField WAIT_FOR_COMPLETION = new ParseField("wait_for_completion");
    public static final ParseField WAIT_FOR_CHECKPOINT = new ParseField("wait_for_checkpoint");
    public static final ParseField STATS_FIELD = new ParseField("stats");
    public static final ParseField INDEX_DOC_TYPE = new ParseField("doc_type");
    public static final ParseField SOURCE = new ParseField("source");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    public static final ParseField DESTINATION = new ParseField("dest");
    public static final ParseField SETTINGS = new ParseField("settings");
    public static final ParseField METADATA = new ParseField("_meta");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField FORCE = new ParseField("force");
    public static final ParseField MAX_PAGE_SEARCH_SIZE = new ParseField("max_page_search_size");
    public static final ParseField DOCS_PER_SECOND = new ParseField("docs_per_second");
    public static final ParseField DATES_AS_EPOCH_MILLIS = new ParseField("dates_as_epoch_millis");
    public static final ParseField ALIGN_CHECKPOINTS = new ParseField("align_checkpoints");
    public static final ParseField USE_PIT = new ParseField("use_point_in_time");
    public static final ParseField DEDUCE_MAPPINGS = new ParseField("deduce_mappings");
    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField SYNC = new ParseField("sync");
    public static final ParseField TIME = new ParseField("time");
    public static final ParseField DELAY = new ParseField("delay");
    // TODO: Rename to "defer_data_validation" or similar to emphasize that not all validation is deferred
    public static final ParseField DEFER_VALIDATION = new ParseField("defer_validation");
    public static final ParseField DRY_RUN = new ParseField("dry_run");
    public static final ParseField RETENTION_POLICY = new ParseField("retention_policy");
    public static final ParseField MAX_AGE = new ParseField("max_age");

    public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match");
    /**
     * Fields for checkpointing
     */
    // the timestamp of the checkpoint, mandatory
    public static final ParseField CHECKPOINT = new ParseField("checkpoint");
    public static final ParseField INDEXER_STATE = new ParseField("indexer_state");
    public static final ParseField POSITION = new ParseField("position");
    public static final ParseField CHECKPOINT_PROGRESS = new ParseField("checkpoint_progress");
    public static final ParseField TIMESTAMP_MILLIS = new ParseField("timestamp_millis");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    // checkpoint for for time based sync
    // TODO: consider a lower bound for usecases where you want to transform on a window of a stream
    public static final ParseField TIME_UPPER_BOUND_MILLIS = new ParseField("time_upper_bound_millis");
    public static final ParseField TIME_UPPER_BOUND = new ParseField("time_upper_bound");

    // common strings
    public static final String TASK_NAME = "data_frame/transforms";
    public static final String REST_BASE_PATH_TRANSFORMS = "/_transform/";
    public static final String REST_BASE_PATH_TRANSFORMS_BY_ID = REST_BASE_PATH_TRANSFORMS + "{id}/";

    public static final String TRANSFORM_ID = "transform_id";

    // note: this is used to match tasks
    public static final String PERSISTENT_TASK_DESCRIPTION_PREFIX = "data_frame_";

    // strings for meta information
    public static final String META_FIELDNAME = "_transform";
    public static final String CREATION_DATE_MILLIS = "creation_date_in_millis";
    public static final String CREATED = "created";
    public static final String CREATED_BY = "created_by";
    public static final String TRANSFORM = "transform";
    public static final String TRANSFORM_SIGNATURE = "transform";

    /**
     * Parameter to indicate whether we are serialising to X Content for internal storage. Default the field is invisible (e.g. for get
     * API's)
     */
    public static final String FOR_INTERNAL_STORAGE = "for_internal_storage";
    public static final String EXCLUDE_GENERATED = "exclude_generated";

    // internal document id
    public static String DOCUMENT_ID_FIELD = "_id";

    private TransformField() {}
}
