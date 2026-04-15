/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;

import java.util.Map;
import java.util.Set;

public class GcsDataSourceValidatorTests extends ESTestCase {

    private final DataSourceValidator validator = new FileDataSourceValidator("gcs", GcsConfiguration::fromMap, Set.of("gs"));

    public void testType() {
        assertEquals("gcs", validator.type());
    }

    public void testValidateDatasourceWithCredentials() {
        var result = validator.validateDatasource(Map.of("credentials", "{\"type\":\"service_account\"}", "project_id", "proj"));
        assertTrue(result.get("credentials").secret());
        assertFalse(result.get("project_id").secret());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(validator.validateDatasource(Map.of()).isEmpty());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDatasource(Map.of("bucket", "x")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDatasource(Map.of("auth", "oauth2")));
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "credentials", "{\"type\":\"service_account\"}"))
        );
    }

    public void testValidateDatasetValid() {
        var result = validator.validateDataset(Map.of(), "gs://bucket/path/*.parquet", Map.of("partition_detection", "hive"));
        assertEquals("hive", result.get("partition_detection"));
    }

    public void testValidateDatasetRequiresResource() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDataset(Map.of(), null, Map.of()));
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://bucket/path", Map.of())
        );
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("format", "parquet"))
        );
    }

    public void testValidateDatasetSchemaSampleSize() {
        assertEquals(50, validator.validateDataset(Map.of(), "gs://b/p", Map.of("schema_sample_size", 50)).get("schema_sample_size"));
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("schema_sample_size", 0))
        );
    }

    public void testValidateDatasourceNullSettings() {
        assertTrue(validator.validateDatasource(null).isEmpty());
    }

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new java.util.HashMap<String, Object>();
        settings.put("project_id", "my-project");
        settings.put("endpoint", null);
        var result = validator.validateDatasource(settings);
        assertEquals("my-project", result.get("project_id").nonSecretValue());
        assertNull(result.get("endpoint"));
    }

    public void testValidateDatasetBlankResource() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDataset(Map.of(), "", Map.of()));
    }

    public void testValidateDatasetNullSettings() {
        assertTrue(validator.validateDataset(Map.of(), "gs://b/p", null).isEmpty());
    }

    public void testValidateDatasetAccumulatesMultipleErrors() {
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("error_mode", "banana", "schema_sample_size", "abc"))
        );
        assertEquals(2, e.validationErrors().size());
    }

    public void testToStoredSettingsSecretClassification() {
        GcsConfiguration config = GcsConfiguration.fromMap(Map.of("credentials", "{\"type\":\"service_account\"}", "project_id", "proj"));
        var result = config.toStoredSettings();
        assertTrue(result.get("credentials").secret());
        assertFalse(result.get("project_id").secret());
        try (var s = result.get("credentials").secretValue()) {
            assertEquals("{\"type\":\"service_account\"}", s.toString());
        }
    }
}
