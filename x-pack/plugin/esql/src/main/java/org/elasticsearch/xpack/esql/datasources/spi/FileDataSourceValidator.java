/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator.rejectUnknownFields;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator.validateEnum;
import static org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator.validateInt;

/**
 * {@link DataSourceValidator} for file-based external sources (S3, GCS, Azure).
 */
public class FileDataSourceValidator implements DataSourceValidator {

    // Dataset settings are plain values — no secrets. Credentials are inherited from the parent datasource.
    private static final String PARTITION_DETECTION = "partition_detection";
    private static final String SCHEMA_SAMPLE_SIZE = "schema_sample_size";
    private static final int SCHEMA_SAMPLE_SIZE_MAX = 1000;
    private static final String ERROR_MODE = "error_mode";
    private static final Set<String> DATASET_FIELDS = Set.of(PARTITION_DETECTION, SCHEMA_SAMPLE_SIZE, ERROR_MODE);

    private final String type;
    private final Function<Map<String, Object>, DataSourceConfiguration> configFactory;
    private final Set<String> validSchemes;

    public FileDataSourceValidator(
        String type,
        Function<Map<String, Object>, DataSourceConfiguration> configFactory,
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
    public Map<String, DataSourceStoredSetting> validateDatasource(Map<String, Object> settings) {
        if (settings == null || settings.isEmpty()) {
            return Map.of();
        }
        DataSourceConfiguration config = configFactory.apply(settings);
        return config != null ? config.toStoredSettings() : Map.of();
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceStoredSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        ValidationException errors = new ValidationException();

        validateResource(resource, errors);

        if (datasetSettings == null) {
            datasetSettings = Map.of();
        }
        rejectUnknownFields(datasetSettings, DATASET_FIELDS, errors);

        Map<String, Object> result = new HashMap<>();
        validateEnum(
            datasetSettings,
            result,
            PARTITION_DETECTION,
            PartitionConfig.Strategy.values(),
            PartitionConfig.Strategy::parse,
            errors
        );
        validateEnum(datasetSettings, result, ERROR_MODE, ErrorPolicy.Mode.values(), ErrorPolicy.Mode::parse, errors);
        validateInt(datasetSettings, result, SCHEMA_SAMPLE_SIZE, 1, SCHEMA_SAMPLE_SIZE_MAX, errors);

        errors.throwIfValidationErrorsExist();
        return result;
    }

    private void validateResource(String resource, ValidationException errors) {
        if (resource == null || resource.isBlank()) {
            errors.addValidationError("[resource] is required");
            return;
        }
        // Case-insensitive scheme match, consistent with DataSourceCapabilities.supportsScheme()
        boolean schemeMatch = false;
        for (String scheme : validSchemes) {
            if (resource.regionMatches(true, 0, scheme, 0, scheme.length())) {
                schemeMatch = true;
                break;
            }
        }
        if (schemeMatch == false) {
            errors.addValidationError("[resource] must start with one of " + validSchemes + " but was [" + resource + "]");
        }
    }
}
