/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.core.TimeValue;

import java.time.ZoneId;

/**
 * Sql protocol defaults and end-points shared between JDBC and REST protocol implementations
 */
public final class Protocol {
    /**
     * The attribute names used in the protocol request/response objects.
     */
    // requests
    public static final String QUERY_NAME = "query";
    public static final String CURSOR_NAME = "cursor"; /* request/reply */
    public static final String TIME_ZONE_NAME = "time_zone";
    public static final String FETCH_SIZE_NAME = "fetch_size";
    public static final String REQUEST_TIMEOUT_NAME = "request_timeout";
    public static final String PAGE_TIMEOUT_NAME = "page_timeout";
    public static final String FILTER_NAME = "filter";
    public static final String MODE_NAME = "mode";
    public static final String CLIENT_ID_NAME = "client_id";
    public static final String VERSION_NAME = "version";
    public static final String COLUMNAR_NAME = "columnar";
    public static final String BINARY_FORMAT_NAME = "binary_format";
    public static final String FIELD_MULTI_VALUE_LENIENCY_NAME = "field_multi_value_leniency";
    public static final String INDEX_INCLUDE_FROZEN_NAME = "index_include_frozen";
    public static final String RUNTIME_MAPPINGS_NAME = "runtime_mappings";
    // async
    public static final String WAIT_FOR_COMPLETION_TIMEOUT_NAME = "wait_for_completion_timeout";
    public static final String KEEP_ON_COMPLETION_NAME = "keep_on_completion";
    public static final String KEEP_ALIVE_NAME = "keep_alive";

    // params
    public static final String PARAMS_NAME = "params";
    public static final String PARAMS_TYPE_NAME = "type";
    public static final String PARAMS_VALUE_NAME = "value";
    // responses
    public static final String COLUMNS_NAME = "columns";
    public static final String ROWS_NAME = "rows";
    // responses async
    public static final String ID_NAME = "id";
    public static final String IS_PARTIAL_NAME = "is_partial";
    public static final String IS_RUNNING_NAME = "is_running";

    public static final ZoneId TIME_ZONE = ZoneId.of("Z");

    /**
     * Global choice for the default fetch size.
     */
    public static final int FETCH_SIZE = 1000;
    public static final TimeValue REQUEST_TIMEOUT = TimeValue.timeValueSeconds(90);
    public static final TimeValue PAGE_TIMEOUT = TimeValue.timeValueSeconds(45);
    public static final boolean FIELD_MULTI_VALUE_LENIENCY = false;
    public static final boolean INDEX_INCLUDE_FROZEN = false;

    /*
     * Using the Boolean object here so that SqlTranslateRequest to set this to null (since it doesn't need a "columnar" or
     * binary parameter).
     * See {@code SqlTranslateRequest.toXContent}
     */
    public static final Boolean COLUMNAR = Boolean.FALSE;
    public static final Boolean BINARY_COMMUNICATION = null;

    public static final TimeValue DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT = null;
    public static final Boolean DEFAULT_KEEP_ON_COMPLETION = false;
    public static TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);
    public static TimeValue MIN_KEEP_ALIVE = TimeValue.timeValueMinutes(1);

    /*
     * URL parameters
     */
    public static final String URL_PARAM_FORMAT = "format";
    public static final String URL_PARAM_DELIMITER = "delimiter";

    /**
     * HTTP header names
     */
    public static final String HEADER_NAME_CURSOR = "Cursor";
    public static final String HEADER_NAME_TOOK_NANOS = "Took-nanos";
    public static final String HEADER_NAME_ASYNC_ID = "Async-ID";
    public static final String HEADER_NAME_ASYNC_PARTIAL = "Async-partial";
    public static final String HEADER_NAME_ASYNC_RUNNING = "Async-running";

    /**
     * SQL-related endpoints
     */
    public static final String CLEAR_CURSOR_REST_ENDPOINT = "/_sql/close";
    public static final String SQL_QUERY_REST_ENDPOINT = "/_sql";
    public static final String SQL_TRANSLATE_REST_ENDPOINT = "/_sql/translate";
    public static final String SQL_STATS_REST_ENDPOINT = "/_sql/stats";
    // async
    public static final String SQL_ASYNC_REST_ENDPOINT = "/_sql/async/";
    public static final String SQL_ASYNC_STATUS_REST_ENDPOINT = SQL_ASYNC_REST_ENDPOINT + "status/";
    public static final String SQL_ASYNC_DELETE_REST_ENDPOINT = SQL_ASYNC_REST_ENDPOINT + "delete/";
}
