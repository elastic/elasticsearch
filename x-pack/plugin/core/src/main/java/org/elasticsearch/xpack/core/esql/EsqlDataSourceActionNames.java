/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/** Action name constants for the ES|QL data source CRUD API. */
public class EsqlDataSourceActionNames {
    public static final String ESQL_PUT_DATA_SOURCE_ACTION_NAME = "cluster:admin/esql/data_source/put";
    public static final String ESQL_GET_DATA_SOURCE_ACTION_NAME = "cluster:admin/esql/data_source/get";
    public static final String ESQL_DELETE_DATA_SOURCE_ACTION_NAME = "cluster:admin/esql/data_source/delete";
}
