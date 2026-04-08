/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * {@link DatasourceType} implementation for file-based external sources (S3, GCS, Azure).
 * All file-based datasources follow the same pattern: validate settings via a
 * {@link DatasourceConfiguration} subclass, validate datasets via {@link FileDatasetConfiguration}.
 */
public class FileDatasourceType implements DatasourceType {

    private final String type;
    private final Function<Map<String, Object>, DatasourceConfiguration> configFactory;
    private final Set<String> validSchemes;

    public FileDatasourceType(String type, Function<Map<String, Object>, DatasourceConfiguration> configFactory, Set<String> validSchemes) {
        this.type = type;
        this.configFactory = configFactory;
        this.validSchemes = validSchemes;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Map<String, SettingValue> validateDatasource(Map<String, Object> settings) {
        DatasourceConfiguration config = configFactory.apply(settings);
        return config != null ? config.toSettingValues() : Map.of();
    }

    @Override
    public Map<String, SettingValue> validateDataset(
        Map<String, Object> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        return FileDatasetConfiguration.validate(resource, validSchemes, datasetSettings);
    }
}
