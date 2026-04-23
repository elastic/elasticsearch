/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.common.ValidationException;

import java.util.Map;

/**
 * Validates data source and dataset settings at CRUD time. Each plugin provides a stateless
 * singleton implementation. Called synchronously from the transport thread — no blocking I/O.
 * {@link #validateDataset} sees plaintext secrets: implementations MUST NOT log, persist
 * outside cluster state, or leak them in exception messages.
 */
public interface DataSourceValidator {

    /** Type identifier, e.g. {@code "s3"}, {@code "gcs"}, {@code "azure"}. */
    String type();

    /**
     * Validates settings and wraps them in {@link DataSourceSetting} so secret classification
     * travels into cluster state. Throws {@link ValidationException} if invalid.
     */
    Map<String, DataSourceSetting> validateDatasource(Map<String, Object> datasourceSettings);

    /**
     * Validates dataset settings against the parent. Returns plain values only — datasets never
     * carry secrets. The parent's settings are passed so implementations can cross-check
     * region / auth / format. Throws {@link ValidationException} if invalid.
     */
    Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    );
}
