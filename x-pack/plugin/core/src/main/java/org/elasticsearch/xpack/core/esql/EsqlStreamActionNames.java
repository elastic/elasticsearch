/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/**
 * Exposes ES|QL stream action names for RBACEngine, the same way {@link EsqlAsyncActionNames} does for the async actions.
 */
public class EsqlStreamActionNames {
    public static final String ESQL_STREAM_GET_RESULT_ACTION_NAME = "indices:data/read/esql/stream/get";
}
