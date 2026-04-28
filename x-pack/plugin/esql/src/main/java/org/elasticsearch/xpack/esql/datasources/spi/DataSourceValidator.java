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
 * Validates data source + dataset settings at CRUD time. No blocking I/O. {@link #validateDataset}
 * sees plaintext secrets — do not log, persist, or leak them.
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
     * Validates dataset settings. Returns plain values — datasets carry no secrets. Parent passed for
     * cross-checks. Throws {@link ValidationException} if invalid.
     */
    Map<String, Object> validateDataset(
        Map<String, DataSourceSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    );
}
