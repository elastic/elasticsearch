/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.eql;

/**
 * Exposes EQL async action names for RBACEngine
 */
public final class EqlAsyncActionNames {
    public static final String EQL_ASYNC_GET_RESULT_ACTION_NAME = "indices:data/read/eql/async/get";
}
