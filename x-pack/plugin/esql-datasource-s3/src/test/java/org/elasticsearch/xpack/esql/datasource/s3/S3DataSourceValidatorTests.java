/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractDataSourceValidatorTests;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class S3DataSourceValidatorTests extends AbstractDataSourceValidatorTests {

    private final DataSourceValidator validator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"));

    @Override
    protected DataSourceValidator validator() {
        return validator;
    }

    @Override
    protected String expectedType() {
        return "s3";
    }

    @Override
    protected Map<String, Object> sampleConfigWithAllSecrets() {
        return Map.of("access_key", "AKIA_sample", "secret_key", "wJal_sample", "session_token", "FwoG_sample", "region", "us-east-1");
    }

    @Override
    protected Set<String> expectedSecretFieldNames() {
        return Set.of("access_key", "secret_key", "session_token");
    }

    @Override
    protected String sampleResource() {
        return "s3://bucket/path/*.parquet";
    }

    @Override
    protected String wrongSchemeResource() {
        return "gs://bucket/path";
    }

    @Override
    protected Map<String, DataSourceSetting> storedSettingsFromSampleConfig() {
        return S3Configuration.fromMap(sampleConfigWithAllSecrets()).toStoredSettings();
    }

    @Override
    protected Map<String, Object> datasetSettingsWithMultipleErrors() {
        return Map.of("error_mode", "banana", "schema_sample_size", "abc");
    }

    // Must stay in sync with CsvDataSourcePlugin.FORMAT_CONFIG_KEYS. Direct reference is not
    // possible due to cross-plugin test dependency constraints; CsvFormatReaderRecognizedKeysTests
    // enforces the canonical set against the reader's RECOGNIZED_KEYS.
    private static final Set<String> CSV_CONFIG_KEYS = Set.of(
        "delimiter",
        "quote",
        "escape",
        "comment",
        "null_value",
        "encoding",
        "datetime_format",
        "max_field_size",
        "multi_value_syntax",
        "header_row",
        "column_prefix",
        "schema_sample_size"
    );

    private final DataSourceValidator formatAwareValidator = new FileDataSourceValidator(
        "s3",
        S3Configuration::fromMap,
        Set.of("s3", "s3a", "s3n")
    ).withFormatConfigKeyResolver(ext -> ".csv".equals(ext) ? CSV_CONFIG_KEYS : null, Set.of(".gz"));

    public void testValidateDatasourceWithCredentials() {
        var result = validator.validateDatasource(Map.of("access_key", "AKIA123", "secret_key", "secret", "region", "us-east-1"));
        assertTrue(result.get("access_key").secret());
        assertEquals("AKIA123", result.get("access_key").rawValue());
        assertTrue(result.get("secret_key").secret());
        assertEquals("us-east-1", result.get("region").nonSecretValue());
        assertFalse(result.get("region").secret());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("bucket", "x")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(ValidationException.class, () -> validator.validateDatasource(Map.of("auth", "oauth2")));
    }

    public void testValidateDatasourceAuthCaseInsensitive() {
        var result = validator.validateDatasource(Map.of("auth", "NONE"));
        assertEquals("none", result.get("auth").nonSecretValue());  // case-insensitive fields normalized to lowercase
        assertFalse(result.get("auth").secret());
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "access_key", "AKIA123", "secret_key", "secret"))
        );
    }

    public void testValidateDatasourceRejectsWorkloadIdentityWhenDisabled() {
        // default validator has workload identity disabled
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "workload_identity", "region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.workload_identity.enabled"));
    }

    public void testValidateDatasourceAcceptsWorkloadIdentityWhenEnabled() {
        var workloadIdentityValidator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"))
            .withWorkloadIdentityEnabled(() -> true);
        var result = workloadIdentityValidator.validateDatasource(Map.of("auth", "workload_identity", "region", "us-east-1"));
        assertEquals("workload_identity", result.get("auth").nonSecretValue());
        assertFalse(result.get("auth").secret());
    }

    public void testValidateDatasourceWorkloadIdentityConflictWithCredentials() {
        var workloadIdentityValidator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"))
            .withWorkloadIdentityEnabled(() -> true);
        expectThrows(
            ValidationException.class,
            () -> workloadIdentityValidator.validateDatasource(
                Map.of("auth", "workload_identity", "access_key", "AKIA123", "secret_key", "secret")
            )
        );
    }

    public void testValidateDatasourceWithSessionToken() {
        var result = validator.validateDatasource(
            Map.of("access_key", "AKIA123", "secret_key", "secret", "session_token", "FwoGZXIvYXdz", "region", "us-east-1")
        );
        assertTrue(result.get("session_token").secret());
        assertEquals("FwoGZXIvYXdz", result.get("session_token").rawValue());
        assertTrue(result.get("access_key").secret());
    }

    public void testValidateDatasourceSessionTokenConflictsWithAuthNone() {
        expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "session_token", "FwoGZXIvYXdz"))
        );
    }

    public void testValidateDatasourceAccumulatesMultipleErrors() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("unknown_field", "x", "also_unknown", "y"))
        );
        assertEquals(2, e.validationErrors().size());
    }

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new HashMap<String, Object>();
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
            ValidationException.class,
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
            expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetSchemeIsCaseInsensitive() {
        for (String uri : new String[] { "S3://b/p", "S3A://b/p", "S3N://b/p", "S3a://b/p" }) {
            assertNotNull(validator.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("format", "parquet")));
    }

    public void testValidateDatasetErrorModeAllValues() {
        for (String mode : new String[] { "fail_fast", "skip_row", "null_field", "FAIL_FAST", "SKIP_ROW", "NULL_FIELD" }) {
            assertEquals(mode, validator.validateDataset(Map.of(), "s3://b/p", Map.of("error_mode", mode)).get("error_mode"));
        }
    }

    public void testValidateDatasetErrorModeInvalid() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("error_mode", "banana")));
    }

    public void testValidateDatasetErrorModeEmpty() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("error_mode", "")));
    }

    public void testValidateDatasetPartitionDetectionEmpty() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "")));
    }

    public void testValidateDatasetSchemaSampleSize() {
        assertEquals(50, validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 50)).get("schema_sample_size"));
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 0)));
        // upper bound: SCHEMA_SAMPLE_SIZE_MAX = 1000
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 1001)));
    }

    public void testValidateDatasetSchemaSampleSizeNonNumber() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", "abc")));
    }

    public void testValidateDatasetAccumulatesResourceAndFieldErrors() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "gs://wrong-scheme", Map.of("error_mode", "banana"))
        );
        assertEquals(2, e.validationErrors().size());
    }

    // --- Coordinator data-shape key validation (strict, via the owning query-path parsers) ---

    public void testValidateDatasetSchemaResolutionAllValues() {
        for (String v : new String[] { "first_file_wins", "strict", "union_by_name", "FIRST_FILE_WINS", "Union_By_Name" }) {
            assertEquals(v, validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_resolution", v)).get("schema_resolution"));
        }
    }

    public void testValidateDatasetSchemaResolutionInvalid() {
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_resolution", "banana"))
        );
    }

    public void testValidateDatasetMaxErrors() {
        assertEquals("100", validator.validateDataset(Map.of(), "s3://b/p", Map.of("max_errors", "100")).get("max_errors"));
    }

    public void testValidateDatasetMaxErrorsNonNumber() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("max_errors", "abc")));
    }

    public void testValidateDatasetMaxErrorRatio() {
        assertEquals("0.1", validator.validateDataset(Map.of(), "s3://b/p", Map.of("max_error_ratio", "0.1")).get("max_error_ratio"));
    }

    public void testValidateDatasetMaxErrorRatioOutOfRange() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("max_error_ratio", "2.0")));
    }

    public void testValidateDatasetErrorBudgetConflictsWithFailFast() {
        // fail_fast always aborts on the first error, so a budget key is a contradiction the parser rejects.
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("error_mode", "fail_fast", "max_errors", "10"))
        );
    }

    public void testValidateDatasetPartitionPath() {
        assertEquals(
            "year=*/month=*",
            validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_path", "year=*/month=*")).get("partition_path")
        );
    }

    public void testValidateDatasetHivePartitioning() {
        assertEquals(false, validator.validateDataset(Map.of(), "s3://b/p", Map.of("hive_partitioning", false)).get("hive_partitioning"));
        assertEquals(true, validator.validateDataset(Map.of(), "s3://b/p", Map.of("hive_partitioning", true)).get("hive_partitioning"));
    }

    public void testValidateDatasetTargetSplitSize() {
        assertEquals("64mb", validator.validateDataset(Map.of(), "s3://b/p", Map.of("target_split_size", "64mb")).get("target_split_size"));
    }

    public void testValidateDatasetTargetSplitSizeInvalid() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("target_split_size", "abc")));
    }

    public void testValidateDatasetTargetSplitSizeUnitlessRejected() {
        // ByteSizeValue requires a unit suffix; a bare number is rejected.
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("target_split_size", "1024")));
    }

    public void testValidateDatasetFormatStaysExternalOnly() {
        // format/reader remain EXTERNAL-only dev knobs: they are NOT accepted as dataset settings.
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("format", "csv")));
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("reader", "java")));
    }

    // --- Format-aware validation tests ---

    public void testFormatAwareValidatorAcceptsCsvDelimiter() {
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.csv", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testFormatAwareValidatorAcceptsMultipleCsvOptions() {
        var result = formatAwareValidator.validateDataset(
            Map.of(),
            "s3://bucket/data.csv",
            Map.of("delimiter", "|", "quote", "'", "header_row", false)
        );
        assertEquals("|", result.get("delimiter"));
        assertEquals("'", result.get("quote"));
        assertEquals(false, result.get("header_row"));
    }

    public void testFormatAwareValidatorAcceptsMixOfBaseAndFormatFields() {
        var result = formatAwareValidator.validateDataset(
            Map.of(),
            "s3://bucket/data.csv",
            Map.of("partition_detection", "hive", "delimiter", ";")
        );
        assertEquals("hive", result.get("partition_detection"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testFormatAwareValidatorRejectsCsvFieldOnNonCsvResource() {
        expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.parquet", Map.of("delimiter", ";"))
        );
    }

    public void testFormatAwareValidatorRejectsUnknownFieldOnCsvResource() {
        expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.csv", Map.of("nonexistent_field", "value"))
        );
    }

    public void testFormatAwareValidatorHandlesCompoundExtension() {
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.csv.gz", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testFormatAwareValidatorBaseFieldsStillWork() {
        var result = formatAwareValidator.validateDataset(
            Map.of(),
            "s3://bucket/data.csv",
            Map.of("partition_detection", "hive", "error_mode", "skip_row", "schema_sample_size", 50)
        );
        assertEquals("hive", result.get("partition_detection"));
        assertEquals("skip_row", result.get("error_mode"));
        assertEquals(50, result.get("schema_sample_size"));
    }

    public void testFormatAwareValidatorResourceWithoutExtension() {
        expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data", Map.of("delimiter", ";"))
        );
    }

    public void testFormatAwareValidatorCaseInsensitiveExtension() {
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.CSV", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testWithoutResolverRejectsFormatFields() {
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://bucket/data.csv", Map.of("delimiter", ";"))
        );
    }
}
