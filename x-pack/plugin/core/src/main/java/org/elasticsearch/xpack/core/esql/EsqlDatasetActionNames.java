/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/** Action name constants for the ES|QL dataset CRUD API. */
public class EsqlDatasetActionNames {
    public static final String ESQL_PUT_DATASET_ACTION_NAME = "indices:admin/esql/dataset/put";
    public static final String ESQL_GET_DATASET_ACTION_NAME = "indices:admin/esql/dataset/get";
    public static final String ESQL_DELETE_DATASET_ACTION_NAME = "indices:admin/esql/dataset/delete";
}
