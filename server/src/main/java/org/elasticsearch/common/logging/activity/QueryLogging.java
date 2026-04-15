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
 * Constants specific to logging queries - DSL search, ESQL, etc.
 */
public interface QueryLogging {
    /**
     * This is the prefix for all query logging specific fields.
     */
    String ES_QUERY_FIELDS_PREFIX = "elasticsearch.querylog.";
    /**
     * Query text.
     */
    String QUERY_FIELD_QUERY = ES_QUERY_FIELDS_PREFIX + "query";
    /**
     * How many results the search or query actually returned.
     */
    String QUERY_FIELD_RESULT_COUNT = ES_QUERY_FIELDS_PREFIX + "result_count";
    /**
     * Which indices were queried. May not apply to some modules like ESQL or SQL.
     */
    String QUERY_FIELD_INDICES = ES_QUERY_FIELDS_PREFIX + "indices";
    /**
     * Shard stats information - successful, skipped, failed.
     */
    String QUERY_FIELD_SHARDS = ES_QUERY_FIELDS_PREFIX + "shards.";
    /**
     * This is the name Log4j logger will use.
     */
    String QUERY_LOGGER_NAME = "elasticsearch.querylog";

    /**
     * Did this query come from another cluster?
     */
    String QUERY_FIELD_IS_REMOTE = ES_QUERY_FIELDS_PREFIX + "is_remote";
    /**
     * Counts of the statuses of the clusters - successful, skipped, failed, etc.
     */
    String QUERY_FIELD_REMOTE_STATUS = ES_QUERY_FIELDS_PREFIX + "clusters.";
    /**
     * How many remote clusters were involved in this query?
     */
    String QUERY_FIELD_REMOTE_COUNT = QUERY_FIELD_REMOTE_STATUS + "remote_count";
    /**
     * List of remote clusters involved in this query.
     */
    String QUERY_FIELD_REMOTES = QUERY_FIELD_REMOTE_STATUS + "remotes";
}
