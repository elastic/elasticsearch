/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.xpack.esql.datasources.spi.DatasourceType;
import org.elasticsearch.xpack.esql.datasources.spi.FileDatasetConfiguration;
import org.elasticsearch.xpack.esql.datasources.spi.SettingValue;

import java.util.Map;
import java.util.Set;

/**
 * Datasource type descriptor for S3. Delegates to {@link S3Configuration}
 * for datasource validation and {@link FileDatasetConfiguration} for dataset validation.
 */
public class S3DatasourceType implements DatasourceType {

    public static final S3DatasourceType INSTANCE = new S3DatasourceType();
    static final Set<String> VALID_SCHEMES = Set.of("s3://", "s3a://", "s3n://");

    @Override
    public String type() {
        return "s3";
    }

    @Override
    public Map<String, SettingValue> validateDatasource(Map<String, Object> settings) {
        S3Configuration config = S3Configuration.fromMap(settings);
        return config != null ? SettingValue.fromMap(config.toMap(), config.fields()) : Map.of();
    }

    @Override
    public Map<String, SettingValue> validateDataset(
        Map<String, Object> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        return FileDatasetConfiguration.validate(resource, VALID_SCHEMES, datasetSettings);
    }
}
