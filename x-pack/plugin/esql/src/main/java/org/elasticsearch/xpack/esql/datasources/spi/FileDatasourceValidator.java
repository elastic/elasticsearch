/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * {@link DatasourceValidator} implementation for file-based external sources (S3, GCS, Azure).
 */
public class FileDatasourceValidator implements DatasourceValidator {

    private final String type;
    private final Function<Map<String, Object>, DatasourceConfiguration> configFactory;
    private final Set<String> validSchemes;

    public FileDatasourceValidator(
        String type,
        Function<Map<String, Object>, DatasourceConfiguration> configFactory,
        Set<String> validSchemes
    ) {
        this.type = type;
        this.configFactory = configFactory;
        this.validSchemes = validSchemes;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public List<ConfigSetting> validateDatasource(Map<String, Object> settings) {
        DatasourceConfiguration config = configFactory.apply(settings);
        return config != null ? config.toConfigSettings() : List.of();
    }

    @Override
    public List<ConfigSetting> validateDataset(
        Map<String, Object> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        return FileDatasetConfiguration.validate(resource, validSchemes, datasetSettings);
    }
}
