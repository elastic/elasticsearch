/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;

import java.util.Map;
import java.util.Set;

public class S3DataSourceValidatorTests extends ESTestCase {

    private final DataSourceValidator validator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"));

    public void testType() {
        assertEquals("s3", validator.type());
    }

    public void testValidateDatasourceWithCredentials() {
        var result = validator.validateDatasource(Map.of("access_key", "AKIA123", "secret_key", "secret", "region", "us-east-1"));
        assertTrue(result.get("access_key").secret());
        try (var s = result.get("access_key").secretValue()) {
            assertEquals("AKIA123", s.toString());
        }
        assertTrue(result.get("secret_key").secret());
        assertEquals("us-east-1", result.get("region").nonSecretValue());
        assertFalse(result.get("region").secret());
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

    public void testValidateDatasourceAuthCaseInsensitive() {
        var result = validator.validateDatasource(Map.of("auth", "NONE"));
        assertEquals("none", result.get("auth").nonSecretValue());  // case-insensitive fields normalized to lowercase
        assertFalse(result.get("auth").secret());
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "access_key", "AKIA123", "secret_key", "secret"))
        );
    }

    public void testValidateDatasourceAccumulatesMultipleErrors() {
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("unknown_field", "x", "also_unknown", "y"))
        );
        assertEquals(2, e.validationErrors().size());
    }

    public void testValidateDatasourceNullSettings() {
        assertTrue(validator.validateDatasource(null).isEmpty());
    }

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new java.util.HashMap<String, Object>();
        settings.put("region", "us-east-1");
        settings.put("endpoint", null);
        var result = validator.validateDatasource(settings);
        assertEquals("us-east-1", result.get("region").nonSecretValue());
        assertNull(result.get("endpoint"));
    }

    // Dataset settings return plain values, not DataSourceSetting — datasets never contain secrets.
    // Credentials are inherited from the parent datasource at query time. The return type enforces this
    // at compile time: validateDataset() returns Map<String, Object>, not Map<String, DataSourceSetting>.
    public void testValidateDatasetValid() {
        Map<String, Object> result = validator.validateDataset(
            Map.of(),
            "s3://bucket/path/*.parquet",
            Map.of("partition_detection", "hive")
        );
        assertEquals("hive", result.get("partition_detection"));
    }

    public void testValidateDatasetPartitionDetectionInvalid() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "banana"))
        );
    }

    public void testValidateDatasetPartitionDetectionAllValues() {
        for (String strategy : new String[] { "auto", "hive", "template", "none", "AUTO", "HIVE", "TEMPLATE", "NONE" }) {
            assertEquals(
                strategy,
                validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", strategy)).get("partition_detection")
            );
        }
    }

    public void testValidateDatasetSchemeCaseInsensitive() {
        // URI schemes are case-insensitive, consistent with DataSourceCapabilities.supportsScheme()
        assertNotNull(validator.validateDataset(Map.of(), "S3://bucket/path", Map.of()));
    }

    public void testValidateDatasetRequiresResource() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDataset(Map.of(), null, Map.of()));
    }

    public void testValidateDatasetBlankResource() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDataset(Map.of(), "", Map.of()));
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "gs://bucket/path", Map.of())
        );
    }

    public void testValidateDatasetAllSchemes() {
        for (String uri : new String[] { "s3://b/p", "s3a://b/p", "s3n://b/p" }) {
            assertNotNull(validator.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetRejectsSchemePrefixCollision() {
        // The validator must compare against the full "scheme://" form, not just the scheme name,
        // so that resources whose names begin with a known scheme but are not actually that scheme
        // (e.g. "s3foo://...") are correctly rejected.
        for (String uri : new String[] { "s3foo://b/p", "s3abc://b/p", "s3n123://b/p" }) {
            expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetSchemeIsCaseInsensitive() {
        for (String uri : new String[] { "S3://b/p", "S3A://b/p", "S3N://b/p", "S3a://b/p" }) {
            assertNotNull(validator.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("format", "parquet"))
        );
    }

    public void testValidateDatasetErrorModeAllValues() {
        for (String mode : new String[] { "fail_fast", "skip_row", "null_field", "FAIL_FAST", "SKIP_ROW", "NULL_FIELD" }) {
            assertEquals(mode, validator.validateDataset(Map.of(), "s3://b/p", Map.of("error_mode", mode)).get("error_mode"));
        }
    }

    public void testValidateDatasetErrorModeInvalid() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("error_mode", "banana"))
        );
    }

    public void testValidateDatasetErrorModeEmpty() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("error_mode", ""))
        );
    }

    public void testValidateDatasetPartitionDetectionEmpty() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", ""))
        );
    }

    public void testValidateDatasetSchemaSampleSize() {
        assertEquals(50, validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 50)).get("schema_sample_size"));
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 0))
        );
        // upper bound: SCHEMA_SAMPLE_SIZE_MAX = 1000
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 1001))
        );
    }

    public void testValidateDatasetSchemaSampleSizeNonNumber() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", "abc"))
        );
    }

    public void testValidateDatasetAccumulatesResourceAndFieldErrors() {
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "gs://wrong-scheme", Map.of("error_mode", "banana"))
        );
        assertEquals(2, e.validationErrors().size());
    }

    public void testValidateDatasetAccumulatesMultipleErrors() {
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("error_mode", "banana", "schema_sample_size", "abc"))
        );
        assertEquals(2, e.validationErrors().size());
    }

    public void testValidateDatasetNullSettings() {
        assertTrue(validator.validateDataset(Map.of(), "s3://b/p", null).isEmpty());
    }

    public void testToStoredSettingsSecretClassification() {
        S3Configuration config = S3Configuration.fromMap(Map.of("access_key", "AKIA", "secret_key", "secret", "region", "us-east-1"));
        var result = config.toStoredSettings();
        assertTrue(result.get("access_key").secret());
        assertTrue(result.get("secret_key").secret());
        assertFalse(result.get("region").secret());
        try (var s = result.get("access_key").secretValue()) {
            assertEquals("AKIA", s.toString());
        }
    }
}
