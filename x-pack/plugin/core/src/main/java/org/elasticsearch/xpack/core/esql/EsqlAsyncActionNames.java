/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/**
 * Exposes ES|QL async action names for RBACEngine.
 */
public class EsqlAsyncActionNames {
    public static final String ESQL_ASYNC_GET_RESULT_ACTION_NAME = "indices:data/read/esql/async/get";
    public static final String ESQL_ASYNC_STOP_ACTION_NAME = "indices:data/read/esql/async/stop";
}
