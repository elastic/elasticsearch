/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi;

import java.util.Map;

/**
 * Factory for creating {@link DataSource} instances from configuration.
 *
 * <p>Called during data source resolution with the configuration and settings
 * from the data source registration (or inline definition). A factory is
 * registered via {@link DataSourcePlugin#dataSources} keyed by data source type.
 *
 * @see DataSourcePlugin
 * @see DataSource
 */
@FunctionalInterface
public interface DataSourceFactory {

    /**
     * Create a data source instance.
     *
     * @param configuration data source-specific configuration (connection, auth) — opaque to ES
     * @param settings ES-controlled settings for this data source
     * @return a new data source instance
     */
    DataSource create(Map<String, Object> configuration, Map<String, Object> settings);
}
