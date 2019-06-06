/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.common.unit.TimeValue;

import java.time.ZoneId;

/**
 * Sql protocol defaults and end-points shared between JDBC and REST protocol implementations
 */
public final class Protocol {
    public static final ZoneId TIME_ZONE = ZoneId.of("Z");

    /**
     * Global choice for the default fetch size.
     */
    public static final int FETCH_SIZE = 1000;
    public static final TimeValue REQUEST_TIMEOUT = TimeValue.timeValueSeconds(90);
    public static final TimeValue PAGE_TIMEOUT = TimeValue.timeValueSeconds(45);
    public static final boolean FIELD_MULTI_VALUE_LENIENCY = false;
    public static final boolean INDEX_INCLUDE_FROZEN = false;

    /**
     * SQL-related endpoints
     */
    public static final String CLEAR_CURSOR_REST_ENDPOINT = "/_sql/close";
    public static final String CLEAR_CURSOR_DEPRECATED_REST_ENDPOINT = "/_xpack/sql/close";
    public static final String SQL_QUERY_REST_ENDPOINT = "/_sql";
    public static final String SQL_QUERY_DEPRECATED_REST_ENDPOINT = "/_xpack/sql";
    public static final String SQL_TRANSLATE_REST_ENDPOINT = "/_sql/translate";
    public static final String SQL_TRANSLATE_DEPRECATED_REST_ENDPOINT = "/_xpack/sql/translate";
    public static final String SQL_STATS_REST_ENDPOINT = "/_sql/stats";
    public static final String SQL_STATS_DEPRECATED_REST_ENDPOINT = "/_xpack/sql/stats";
}
