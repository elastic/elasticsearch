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
     * Cross-project (CPS) variant of {@link #ESQL_RESOLVE_DATASET_ACTION_NAME}: the coordinator dispatches it to a linked
     * project so that project read-authorizes a relation's FROM patterns against its own state and reports whether any
     * resolve to a dataset the caller may read. Same {@code indices:data/read/*} family, so the {@code read} index privilege
     * covers it and the receiving node's {@code IndicesRequest.Replaceable} security narrowing applies, exactly as for the
     * local action. Distinct name because the local action is owned by the node-local action map (no node-to-node transport
     * handler), whereas this one registers its own transport handler for cross-cluster receipt.
     */
    public static final String ESQL_RESOLVE_DATASET_REMOTE_ACTION_NAME = "indices:data/read/esql/resolve_datasets/remote";
}
