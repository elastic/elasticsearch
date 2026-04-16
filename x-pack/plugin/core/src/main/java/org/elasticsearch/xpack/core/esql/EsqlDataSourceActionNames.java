/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/**
 * Exposes ES|QL data source action names for RBACEngine. The {@code cluster:admin/*} namespace
 * places these actions under the cluster {@code manage} privilege via {@code ALL_CLUSTER_PATTERN}.
 * Data sources are cluster-scoped configuration objects (not index abstractions), so they live in the
 * cluster admin namespace — distinct from datasets, which live under {@code indices:admin/*}.
 */
public class EsqlDataSourceActionNames {
    public static final String ESQL_PUT_DATA_SOURCE_ACTION_NAME = "cluster:admin/esql/data_source/put";
    public static final String ESQL_GET_DATA_SOURCE_ACTION_NAME = "cluster:admin/esql/data_source/get";
    public static final String ESQL_DELETE_DATA_SOURCE_ACTION_NAME = "cluster:admin/esql/data_source/delete";
}
