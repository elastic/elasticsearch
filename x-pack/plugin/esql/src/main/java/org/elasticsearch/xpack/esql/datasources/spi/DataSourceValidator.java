/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;

import java.util.Map;

/**
 * Validates and normalizes datasource and dataset settings at CRUD time.
 *
 * <p>Each storage plugin provides a stateless singleton implementation. Despite the name,
 * implementations do more than just validate: they parse the raw REST input, reject unknown
 * fields, and wrap the result in the canonical cluster-state shape. Datasource fields marked
 * {@link DataSourceConfigDefinition#caseInsensitive() caseInsensitive} are also lowercased
 * on input. Dataset enum fields are validated against their parser (typically case-insensitive)
 * but the original input case is preserved in the stored value.
 *
 * <p>Two methods, two scopes:
 * <ul>
 *   <li>{@link #validateDatasource} validates and parses the datasource definition itself —
 *       the credentials and connection settings that identify how to reach the external
 *       data provider. Returns {@link DataSourceStoredSetting} entries that may carry
 *       secrets.</li>
 *   <li>{@link #validateDataset} validates the dataset-level settings (partition detection,
 *       error mode, schema sample size, etc.) for a specific resource against its parent
 *       datasource. Returns plain values — datasets structurally cannot hold secrets,
 *       since credentials are inherited from the parent datasource at query time.</li>
 * </ul>
 *
 * <p>Validation helpers (reject unknown fields, validate enums and integers) live in
 * {@link DataSourceValidationUtils}.
 */
public interface DataSourceValidator {

    /** The type identifier, e.g. {@code "s3"}, {@code "gcs"}, {@code "azure"}. */
    String type();

    /**
     * Validates and parses datasource settings from a raw REST request body. Rejects
     * unknown fields, normalizes values, and wraps each entry in
     * {@link DataSourceStoredSetting} so the secret classification travels with the value
     * into cluster state.
     *
     * @param datasourceSettings the raw datasource settings from the REST request body
     * @return validated, normalized settings keyed by field name
     * @throws ValidationException if settings are invalid (may contain multiple errors)
     */
    Map<String, DataSourceStoredSetting> validateDatasource(Map<String, Object> datasourceSettings);

    /**
     * Validates and parses dataset-level settings for a given resource against its parent
     * datasource. Dataset settings never contain secrets — credentials are inherited from
     * the parent datasource at query time. The return type enforces this: plain values
     * with no sensitivity classification, unlike {@link #validateDatasource} which returns
     * {@link DataSourceStoredSetting}.
     *
     * <p>The {@code datasourceSettings} parameter is the already-validated parent (read
     * from cluster state), available to implementations that need to cross-check the
     * dataset against datasource-level constraints (region matching, auth mode rules,
     * format-specific gates). The current {@link FileDataSourceValidator} does not yet use
     * it; the parameter is reserved so future implementations can validate without an SPI
     * signature change.
     *
     * @param datasourceSettings the parent datasource's stored settings (from cluster state)
     * @param resource the resource path from the dataset definition
     * @param datasetSettings the raw dataset settings from the REST request body
     * @return validated dataset settings keyed by field name (values only, no secrets)
     * @throws ValidationException if settings or resource are invalid (may contain multiple errors)
     */
    Map<String, Object> validateDataset(
        Map<String, DataSourceStoredSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    );
}
