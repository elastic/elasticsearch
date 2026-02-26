/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

/**
 * Constants specific to logging queries - search, esql, etc. Common fields use ES_QUERY_FIELDS_PREFIX.
 */
public interface QueryLogging {
    String ES_QUERY_FIELDS_PREFIX = "elasticsearch.querylog.";
    String QUERY_FIELD_QUERY = ES_QUERY_FIELDS_PREFIX + "query";
    String QUERY_FIELD_RESULT_COUNT = ES_QUERY_FIELDS_PREFIX + "result_count";
    String QUERY_FIELD_INDICES = ES_QUERY_FIELDS_PREFIX + "indices";
}
