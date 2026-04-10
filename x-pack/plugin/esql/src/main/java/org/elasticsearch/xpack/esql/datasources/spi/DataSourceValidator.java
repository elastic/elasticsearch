/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
     * @throws ValidationException if settings are invalid (may contain multiple errors)
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
     * @throws ValidationException if settings or resource are invalid (may contain multiple errors)
     */
    Map<String, Object> validateDataset(
        Map<String, DataSourceStoredSetting> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    );

    // --- Validation utilities for use by all validator implementations ---

    /**
     * Adds an error for any keys in the settings map that are not in the known fields set.
     *
     * @param settings the settings map to check
     * @param knownFields the set of valid field names
     * @param errors the exception to accumulate errors into
     */
    static void rejectUnknownFields(Map<String, ?> settings, Set<String> knownFields, ValidationException errors) {
        for (String key : settings.keySet()) {
            if (knownFields.contains(key) == false) {
                errors.addValidationError("unknown setting [" + key + "]; known settings: " + knownFields);
            }
        }
    }

    /**
     * Validates that a setting value matches one of the values in the given enum.
     * If present and valid, the original value is stored as-is (no case normalization).
     * Case normalization for datasource settings is handled by
     * {@link DataSourceConfigDefinition#caseInsensitive()} in the configuration layer.
     *
     * @param settings the raw settings map
     * @param result the validated result map to add to
     * @param field the setting field name
     * @param values the valid enum values (for error messages)
     * @param parser parses the string value into the enum (should throw on invalid)
     * @param errors the exception to accumulate errors into
     */
    static <E extends Enum<E>> void validateEnum(
        Map<String, Object> settings,
        Map<String, Object> result,
        String field,
        E[] values,
        Function<String, ?> parser,
        ValidationException errors
    ) {
        Object value = settings.get(field);
        if (value != null) {
            Object parsed;
            try {
                parsed = parser.apply(value.toString());
            } catch (IllegalArgumentException e) {
                errors.addValidationError("[" + field + "] must be one of " + Arrays.toString(values) + ", got [" + value + "]");
                return;
            }
            if (parsed == null) {
                errors.addValidationError("[" + field + "] must be one of " + Arrays.toString(values) + ", got [" + value + "]");
                return;
            }
            result.put(field, value);
        }
    }

    /**
     * Validates that a setting value is an integer within the given range.
     * If present and valid, the parsed integer is added to the result map.
     *
     * @param settings the raw settings map
     * @param result the validated result map to add to
     * @param field the setting field name
     * @param min minimum allowed value (inclusive)
     * @param max maximum allowed value (inclusive)
     * @param errors the exception to accumulate errors into
     */
    static void validateInt(
        Map<String, Object> settings,
        Map<String, Object> result,
        String field,
        int min,
        int max,
        ValidationException errors
    ) {
        Object value = settings.get(field);
        if (value != null) {
            int parsed;
            try {
                parsed = Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                errors.addValidationError("[" + field + "] must be a number, got [" + value + "]");
                return;
            }
            if (parsed < min || parsed > max) {
                errors.addValidationError("[" + field + "] must be between " + min + " and " + max + ", got [" + parsed + "]");
                return;
            }
            result.put(field, parsed);
        }
    }
}
