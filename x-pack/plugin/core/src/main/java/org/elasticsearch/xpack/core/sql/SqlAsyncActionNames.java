/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.sql;

/**
 * Exposes SQL async action names for the RBAC engine
 */
public class SqlAsyncActionNames {
    public static final String SQL_ASYNC_GET_RESULT_ACTION_NAME = "indices:data/read/sql/async/get";
    public static final String SQL_ASYNC_GET_STATUS_ACTION_NAME = "cluster:monitor/xpack/sql/async/status";
}
