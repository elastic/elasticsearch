/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractDataSourceValidatorTests;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;

import java.util.Map;
import java.util.Set;

public class GcsDataSourceValidatorTests extends AbstractDataSourceValidatorTests {

    private final DataSourceValidator validator = new FileDataSourceValidator("gcs", GcsConfiguration::fromMap, Set.of("gs"));

    @Override
    protected DataSourceValidator validator() {
        return validator;
    }

    @Override
    protected String expectedType() {
        return "gcs";
    }

    @Override
    protected Map<String, Object> sampleConfigWithAllSecrets() {
        return Map.of("credentials", "{\"type\":\"service_account\"}", "project_id", "sample-proj");
    }

    @Override
    protected Set<String> expectedSecretFieldNames() {
        return Set.of("credentials");
    }

    @Override
    protected String sampleResource() {
        return "gs://bucket/path/*.parquet";
    }

    @Override
    protected String wrongSchemeResource() {
        return "s3://bucket/path";
    }

    @Override
    protected Map<String, DataSourceSetting> storedSettingsFromSampleConfig() {
        return GcsConfiguration.fromMap(sampleConfigWithAllSecrets()).toStoredSettings();
    }

    @Override
    protected Map<String, Object> datasetSettingsWithMultipleErrors() {
        return Map.of("error_mode", "banana", "schema_sample_size", "abc");
    }

    public void testValidateDatasourceWithCredentials() {
        var result = validator.validateDatasource(Map.of("credentials", "{\"type\":\"service_account\"}", "project_id", "proj"));
        assertTrue(result.get("credentials").secret());
        assertFalse(result.get("project_id").secret());
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

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new java.util.HashMap<String, Object>();
        settings.put("project_id", "my-project");
        settings.put("endpoint", null);
        var result = validator.validateDatasource(settings);
        assertEquals("my-project", result.get("project_id").nonSecretValue());
        assertNull(result.get("endpoint"));
    }
}
