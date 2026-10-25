/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigKeyValidator;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Classifies the password as a secret and prevents datasets from replacing connection settings. */
final class ClickHouseDataSourceValidator implements DataSourceValidator {
    @Override
    public String type() {
        return "clickhouse";
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> settings) {
        return validateDatasource(settings, Set.of());
    }

    @Override
    public Map<String, DataSourceSetting> validateDatasource(Map<String, Object> settings, Set<String> existingSecretKeys) {
        return ClickHouseConfiguration.fromMap(settings, existingSecretKeys).toStoredSettings();
    }

    @Override
    public Map<String, Object> validateDataset(Map<String, DataSourceSetting> parent, String resource, Map<String, Object> settings) {
        ClickHouseTable.parse(resource);
        ConfigKeyValidator.check(settings, List.of());
        return Map.of();
    }

    @Override
    public String authModeOrNull(Map<String, DataSourceSetting> stored) {
        return "basic";
    }
}
