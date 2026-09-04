/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/**
 * Exposes ES|QL view action names for RBACEngine.
 */
public class EsqlViewActionNames {
    public static final String ESQL_PUT_VIEW_ACTION_NAME = "indices:admin/esql/view/put";
    public static final String ESQL_GET_VIEW_ACTION_NAME = "indices:admin/esql/view/get";
    public static final String ESQL_DELETE_VIEW_ACTION_NAME = "indices:admin/esql/view/delete";
}
