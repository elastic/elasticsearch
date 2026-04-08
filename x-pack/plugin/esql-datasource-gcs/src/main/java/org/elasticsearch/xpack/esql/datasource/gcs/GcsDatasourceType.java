/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.xpack.esql.datasources.spi.DatasourceType;
import org.elasticsearch.xpack.esql.datasources.spi.FileDatasetConfiguration;
import org.elasticsearch.xpack.esql.datasources.spi.SettingValue;

import java.util.Map;
import java.util.Set;

/** Datasource type descriptor for GCS. */
public class GcsDatasourceType implements DatasourceType {

    public static final GcsDatasourceType INSTANCE = new GcsDatasourceType();
    static final Set<String> VALID_SCHEMES = Set.of("gs://");

    @Override
    public String type() {
        return "gcs";
    }

    @Override
    public Map<String, SettingValue> validateDatasource(Map<String, Object> settings) {
        GcsConfiguration config = GcsConfiguration.fromMap(settings);
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
