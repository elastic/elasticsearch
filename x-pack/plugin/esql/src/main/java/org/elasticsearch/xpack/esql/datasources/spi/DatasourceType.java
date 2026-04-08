/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;

/**
 * Describes a datasource type for CRUD-time validation.
 * Each storage plugin (S3, GCS, Azure) provides a stateless singleton implementation.
 * <p>
 * Validation methods return {@code Map<String, SettingValue>} where each entry carries
 * both the validated value and whether the field is a secret — one collection, no sync issues.
 */
public interface DatasourceType {

    /** The type identifier, e.g. {@code "s3"}, {@code "gcs"}, {@code "azure_blob"}. */
    String type();

    /**
     * Validates datasource settings. Rejects unknown fields, validates values, normalizes.
     *
     * @param settings the raw settings from the REST request body
     * @return validated settings with secret classification per field
     * @throws IllegalArgumentException if settings are invalid
     */
    Map<String, SettingValue> validateDatasource(Map<String, Object> settings);

    /**
     * Validates dataset settings against the parent datasource.
     *
     * @param datasourceSettings the parent datasource's validated settings
     * @param resource the resource path from the dataset definition
     * @param datasetSettings the raw dataset settings from the REST request body
     * @return validated dataset settings (typically no secrets)
     * @throws IllegalArgumentException if settings or resource are invalid
     */
    Map<String, SettingValue> validateDataset(Map<String, Object> datasourceSettings, String resource, Map<String, Object> datasetSettings);
}
