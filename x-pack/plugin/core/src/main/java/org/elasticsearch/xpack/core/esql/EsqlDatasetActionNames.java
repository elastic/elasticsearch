/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/**
 * Exposes ES|QL dataset action names for RBACEngine. The {@code indices:admin/*} namespace
 * places these actions under the index {@code manage} privilege via {@code MANAGE_AUTOMATON}.
 * Datasets are {@link org.elasticsearch.cluster.metadata.IndexAbstraction.Type#DATASET index abstractions}
 * that participate in the index namespace, so they live under the index admin namespace — distinct from
 * data sources, which live under {@code cluster:admin/*}.
 */
public class EsqlDatasetActionNames {
    public static final String ESQL_PUT_DATASET_ACTION_NAME = "indices:admin/esql/dataset/put";
    public static final String ESQL_GET_DATASET_ACTION_NAME = "indices:admin/esql/dataset/get";
    public static final String ESQL_DELETE_DATASET_ACTION_NAME = "indices:admin/esql/dataset/delete";
}
