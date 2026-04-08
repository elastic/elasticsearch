/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;
import java.util.Map;

/**
 * Describes a datasource type for CRUD-time validation.
 * Each storage plugin provides a stateless singleton implementation.
 */
public interface DatasourceValidator {

    /** The type identifier, e.g. {@code "s3"}, {@code "gcs"}, {@code "azure_blob"}. */
    String type();

    /**
     * Validates datasource settings. Rejects unknown fields, validates values, normalizes.
     *
     * @param settings the raw settings from the REST request body
     * @return validated settings as a list of {@link ConfigSetting}s with values populated
     * @throws IllegalArgumentException if settings are invalid
     */
    List<ConfigSetting> validateDatasource(Map<String, Object> settings);

    /**
     * Validates dataset settings against the parent datasource.
     *
     * @param datasourceSettings the parent datasource's validated settings
     * @param resource the resource path from the dataset definition
     * @param datasetSettings the raw dataset settings from the REST request body
     * @return validated dataset settings as a list of {@link ConfigSetting}s
     * @throws IllegalArgumentException if settings or resource are invalid
     */
    List<ConfigSetting> validateDataset(Map<String, Object> datasourceSettings, String resource, Map<String, Object> datasetSettings);
}
