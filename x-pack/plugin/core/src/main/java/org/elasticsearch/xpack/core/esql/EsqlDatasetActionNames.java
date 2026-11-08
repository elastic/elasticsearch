/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/** Action name constants for the ES|QL dataset CRUD API. */
public class EsqlDatasetActionNames {
    /**
     * Cluster action run before {@link #ESQL_PUT_DATASET_ACTION_NAME} so {@code global.data_source} can authorize use of a datasource
     * when creating or replacing a dataset (see ES|QL datasources security design).
     */
    public static final String ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME = "cluster:admin/esql/dataset/authorize_datasource";

    public static final String ESQL_PUT_DATASET_ACTION_NAME = "indices:admin/esql/dataset/put";
    public static final String ESQL_GET_DATASET_ACTION_NAME = "indices:admin/esql/dataset/get";
    public static final String ESQL_DELETE_DATASET_ACTION_NAME = "indices:admin/esql/dataset/delete";

    /** Read-side resolve action for {@code FROM <dataset>}: read-authorizes the dataset names of an ES|QL query. */
    public static final String ESQL_RESOLVE_DATASET_ACTION_NAME = "indices:data/read/esql/resolve_datasets";

    /**
     * Gate for a dataset a query defines rather than names: an index action, so the caller's index privileges are
     * checked against the data source's name, and one that advertises
     * {@link #ESQL_AUTHORIZE_DATASET_DATASOURCE_ACTION_NAME} so {@code global.data_source} is checked as it is when a
     * dataset is created against that source.
     * <p>
     * Outside {@code indices:data/read/*} deliberately, and that is the whole of why the name looks like an admin
     * one for something a read does: describing a dataset is held to the privilege that registering one is held to,
     * so {@code read} must not reach it. The name sits where {@code manage_dataset} already matches and
     * {@code create_dataset} names it outright, which is the privilege a registration against the same source needs.
     */
    public static final String ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME = "indices:admin/esql/dataset/authorize_inline_datasource";
}
