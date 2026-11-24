/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/**
 * Exposes ES|QL stream action names for RBACEngine, the same way {@link EsqlAsyncActionNames} does for the async actions.
 * <p>
 * These actions take a stream id and target no index. They are still named {@code indices:data/read/...}, delete included, so that the
 * {@code read} index privilege that lets a user run the stream query also covers reading and deleting its stored result. This follows
 * the async actions. The privilege is only the baseline: that the caller owns the stream must be checked in the transport actions.
 * Neither is wired yet: the RBACEngine baseline and the ownership check both come with the stream storage.
 */
public class EsqlStreamActionNames {
    public static final String ESQL_STREAM_GET_RESULT_ACTION_NAME = "indices:data/read/esql/stream/get";
    public static final String ESQL_STREAM_DELETE_ACTION_NAME = "indices:data/read/esql/stream/delete";
}
