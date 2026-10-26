/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.Map;
import java.util.Set;

/** Registers the experimental ClickHouse connector and its stored credential contract. */
public class ClickHouseDataSourcePlugin extends Plugin implements DataSourcePlugin {
    private static final FeatureFlag ENABLED = new FeatureFlag("esql_external_datasources_clickhouse");

    @Override
    public Set<String> supportedConnectorSchemes() {
        return ENABLED.isEnabled() ? Set.of("clickhouse") : Set.of();
    }

    @Override
    public Map<String, ConnectorFactory> connectors(Settings settings) {
        return ENABLED.isEnabled() ? Map.of("clickhouse", new ClickHouseConnectorFactory()) : Map.of();
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        return ENABLED.isEnabled() ? Map.of("clickhouse", new ClickHouseDataSourceValidator()) : Map.of();
    }

    @Override
    public Set<String> datasourceSecretSettingNames() {
        return Set.of("password");
    }
}
