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

    private EsqlDataSourcesCapabilities() {}
}
