/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

/** REST-handler capability constants for the ES|QL data source + dataset CRUD API. */
public final class EsqlDataSourcesCapabilities {
    /** Advertises that this node exposes the data_sources + datasets CRUD endpoints. */
    public static final String DATA_SOURCES = "data_sources";

    /** The dataset PUT body accepts a declared `mappings` block (types, path renames, format). */
    public static final String DATASET_DECLARED_SCHEMA = "dataset_declared_schema";

    /**
     * Signals that the data_source/dataset CRUD routes ({@code PUT/GET/DELETE /_query/data_source/{name}} and
     * {@code PUT/GET/DELETE /_query/dataset/{name}}) are exposed with {@code @ServerlessScope(Scope.PUBLIC)}.
     * Old nodes in a mixed cluster predate this annotation and will not report this capability via
     * {@code /_capabilities}, so any mixed cluster containing such a node correctly returns {@code supported=false}.
     */
    public static final String DATA_SOURCES_SERVERLESS_SCOPE = "data_sources_serverless_scope";

    /**
     * Registration rejects a column declared {@code text}. Gates the yaml pin on that rejection, because the suite
     * also runs mixed-cluster, where a node without this capability accepts the declaration and answers 200.
     */
    public static final String DATASET_TEXT_TYPE_NOT_DECLARABLE = "dataset_text_type_not_declarable";

    private EsqlDataSourcesCapabilities() {}
}
