/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;

/**
 * Validates datasource and dataset settings at CRUD time.
 * Each storage plugin provides a stateless singleton implementation.
 */
public interface DataSourceValidator {

    /** The type identifier, e.g. {@code "s3"}, {@code "gcs"}, {@code "azure_blob"}. */
    String type();

    /**
     * Validates datasource settings. Rejects unknown fields, validates values, normalizes.
     *
     * @param settings the raw settings from the REST request body
     * @return validated settings keyed by field name
     * @throws IllegalArgumentException if settings are invalid
     */
    Map<String, DataSourceStoredSetting> validateDatasource(Map<String, Object> settings);

    /**
     * Validates dataset settings against the parent datasource. Dataset settings never contain
     * secrets — credentials are inherited from the parent datasource at query time. The return
     * type enforces this: plain values with no sensitivity classification, unlike
     * {@link #validateDatasource} which returns {@link DataSourceStoredSetting}.
     *
     * @param datasourceSettings the parent datasource's stored settings (from cluster state)
     * @param resource the resource path from the dataset definition
     * @param datasetSettings the raw dataset settings from the REST request body
     * @return validated dataset settings keyed by field name (values only, no secrets)
     * @throws IllegalArgumentException if settings or resource are invalid
     */
    Map<String, Object> validateDataset(
        Map<String, DataSourceStoredSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    );
}
