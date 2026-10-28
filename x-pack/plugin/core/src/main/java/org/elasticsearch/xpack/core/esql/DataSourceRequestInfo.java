/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql;

/**
 * Implemented by transport requests that target named ES|QL data sources. Intended for use by
 * configurable cluster privileges under {@code global.data_source} in role definitions so security
 * can authorize datasource operations without {@code x-pack-core} depending on concrete request
 * classes from the ES|QL plugin.
 */
public interface DataSourceRequestInfo {

    /**
     * Data source names this request operates on (never {@code null}; may be empty only if invalid).
     */
    String[] dataSourceNames();

    /**
     * Transport action name for this request (see {@link org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames}),
     * used when evaluating configurable datasource privileges.
     */
    String dataSourceClusterActionName();
}
