/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractDataSourceValidatorTests;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

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
        return Map.of("credentials", "{\"type\":\"service_account\"}", "access_token", "ya29.sample", "project_id", "sample-proj");
    }

    @Override
    protected Set<String> expectedSecretFieldNames() {
        return Set.of("credentials", "access_token");
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
        expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("bucket", "x")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("auth", "oauth2")));
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "anonymous", "credentials", "{\"type\":\"service_account\"}"))
        );
    }

    public void testValidateDatasourceWithAccessToken() {
        var result = validator.validateDatasource(Map.of("access_token", "ya29.token", "project_id", "proj"));
        assertTrue(result.get("access_token").secret());
        assertEquals("ya29.token", result.get("access_token").rawValue());
        assertFalse(result.get("project_id").secret());
    }

    public void testValidateDatasourceAccessTokenConflictsWithAuthNone() {
        expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "anonymous", "access_token", "ya29.token"))
        );
    }

    public void testValidateDatasourceRejectsWorkloadIdentityWhenDisabled() {
        // default validator has workload identity disabled
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "managed_identity", "project_id", "proj"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.managed_identity.enabled"));
    }

    public void testValidateDatasourceAcceptsWorkloadIdentityWhenEnabled() {
        var workloadIdentityValidator = new FileDataSourceValidator("gcs", GcsConfiguration::fromMap, Set.of("gs"))
            .withManagedIdentityEnabled(() -> true);
        var result = workloadIdentityValidator.validateDatasource(Map.of("auth", "managed_identity", "project_id", "proj"));
        assertEquals("managed_identity", result.get("auth").nonSecretValue());
        assertFalse(result.get("auth").secret());
    }

    public void testValidateDatasourceWorkloadIdentityConflictWithCredentials() {
        var workloadIdentityValidator = new FileDataSourceValidator("gcs", GcsConfiguration::fromMap, Set.of("gs"))
            .withManagedIdentityEnabled(() -> true);
        expectThrows(
            ValidationException.class,
            () -> workloadIdentityValidator.validateDatasource(
                Map.of("auth", "managed_identity", "credentials", "{\"type\":\"service_account\"}")
            )
        );
    }

    public void testValidateDatasetValid() {
        var result = validator.validateDataset(Map.of(), "gs://bucket/path/*.parquet", Map.of("partition_detection", "hive"));
        assertEquals("hive", result.get("partition_detection"));
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("format", "parquet")));
    }

    public void testValidateDatasetSchemaSampleSize() {
        assertEquals(50, validator.validateDataset(Map.of(), "gs://b/p", Map.of("schema_sample_size", 50)).get("schema_sample_size"));
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("schema_sample_size", 0)));
    }

    public void testValidateDatasetSchemaResolution() {
        assertEquals(
            "union_by_name",
            validator.validateDataset(Map.of(), "gs://b/p", Map.of("schema_resolution", "union_by_name")).get("schema_resolution")
        );
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("schema_resolution", "banana"))
        );
    }

    public void testValidateDatasetErrorBudget() {
        assertEquals("100", validator.validateDataset(Map.of(), "gs://b/p", Map.of("max_errors", "100")).get("max_errors"));
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("error_mode", "fail_fast", "max_errors", "10"))
        );
    }

    public void testValidateDatasetTargetSplitSize() {
        assertEquals("64mb", validator.validateDataset(Map.of(), "gs://b/p", Map.of("target_split_size", "64mb")).get("target_split_size"));
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("target_split_size", "abc")));
    }

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new HashMap<String, Object>();
        // auth=anonymous makes the credential-less config resolvable; the null-skipping behavior is what's under test.
        settings.put("auth", "anonymous");
        settings.put("project_id", "my-project");
        settings.put("endpoint", null);
        var result = validator.validateDatasource(settings);
        assertEquals("my-project", result.get("project_id").nonSecretValue());
        assertNull(result.get("endpoint"));
    }
}
