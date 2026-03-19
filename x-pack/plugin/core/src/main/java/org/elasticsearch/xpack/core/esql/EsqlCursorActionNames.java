/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/**
 * Exposes ES|QL cursor action names for the RBAC engine.
 */
public class EsqlCursorActionNames {
    public static final String ESQL_CURSOR_ACTION_NAME = "indices:data/read/esql/cursor";
    public static final String ESQL_DELETE_CURSOR_ACTION_NAME = "indices:data/read/esql/cursor/delete";
}
