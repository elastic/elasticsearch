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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

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

    // The real production resolver (FormatConfigKeyResolver.of, the same factory EsqlPlugin uses),
    // wired here with a single known format: csv, mapped from the ".csv" extension, with
    // CSV_CONFIG_KEYS as its config keys. Only CSV_CONFIG_KEYS is a local copy, because cross-plugin
    // test deps forbid importing the CSV plugin; the resolver behavior itself is the production code.
    private static final FileDataSourceValidator.FormatConfigKeyResolver CSV_RESOLVER = FileDataSourceValidator.FormatConfigKeyResolver.of(
        Map.of("csv", CSV_CONFIG_KEYS),
        Map.of(".csv", "csv")
    );

    // Expected known-format set for unknown-format error assertions; matches CSV_RESOLVER.knownFormats().
    private static final Set<String> KNOWN_FORMATS = Set.of("csv");

    private final DataSourceValidator formatAwareValidator = new FileDataSourceValidator(
        "s3",
        S3Configuration::fromMap,
        Set.of("s3", "s3a", "s3n")
    ).withFormatConfigKeyResolver(CSV_RESOLVER, Set.of(".gz"));

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
        var result = validator.validateDatasource(Map.of("auth", "ANONYMOUS"));
        assertEquals("anonymous", result.get("auth").nonSecretValue());  // case-insensitive fields normalized to lowercase
        assertFalse(result.get("auth").secret());
    }

    public void testValidateDatasourceCanonicalizesDeprecatedAuthNone() {
        // The CRUD store path canonicalizes a deprecated alias and warns: the stored (GET-returned) value is canonical.
        var result = validator.validateDatasource(Map.of("auth", "none"));
        assertEquals("anonymous", result.get("auth").nonSecretValue());
        assertWarnings("auth value [none] is deprecated; the canonical value is [anonymous]");
    }

    public void testValidateDatasourceCanonicalizesDeprecatedWorkloadIdentity() {
        var enabledValidator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"))
            .withManagedIdentityEnabled(() -> true);
        var result = enabledValidator.validateDatasource(Map.of("auth", "workload_identity", "region", "us-east-1"));
        assertEquals("managed_identity", result.get("auth").nonSecretValue());
        assertWarnings("auth value [workload_identity] is deprecated; the canonical value is [managed_identity]");
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "anonymous", "access_key", "AKIA123", "secret_key", "secret"))
        );
    }

    public void testValidateDatasourceRejectsManagedIdentityWhenDisabled() {
        // default validator has managed identity disabled
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "managed_identity", "region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.managed_identity.enabled"));
    }

    public void testValidateDatasourceRejectsDeprecatedWorkloadIdentityWhenDisabled() {
        // The deprecated alias canonicalizes to managed_identity, so the disabled-gate still catches it (and warns).
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "workload_identity", "region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.managed_identity.enabled"));
        assertWarnings("auth value [workload_identity] is deprecated; the canonical value is [managed_identity]");
    }

    public void testValidateDatasourceAcceptsManagedIdentityWhenEnabled() {
        var managedIdentityValidator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"))
            .withManagedIdentityEnabled(() -> true);
        var result = managedIdentityValidator.validateDatasource(Map.of("auth", "managed_identity", "region", "us-east-1"));
        assertEquals("managed_identity", result.get("auth").nonSecretValue());
        assertFalse(result.get("auth").secret());
    }

    public void testValidateDatasourceManagedIdentityConflictWithCredentials() {
        var managedIdentityValidator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"))
            .withManagedIdentityEnabled(() -> true);
        expectThrows(
            ValidationException.class,
            () -> managedIdentityValidator.validateDatasource(
                Map.of("auth", "managed_identity", "access_key", "AKIA123", "secret_key", "secret")
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
            () -> validator.validateDatasource(Map.of("auth", "anonymous", "session_token", "FwoGZXIvYXdz"))
        );
    }

    public void testValidateDatasourceAccumulatesMultipleErrors() {
        // Complete credentials keep auto resolvable, so the only accumulated errors are the two distinct unknown fields.
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("unknown_field", "x", "also_unknown", "y", "access_key", "ak", "secret_key", "sk"))
        );
        assertEquals(2, e.validationErrors().size());
    }

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new HashMap<String, Object>();
        // auth=anonymous makes the credential-less config resolvable; the null-skipping behavior is what's under test.
        settings.put("auth", "anonymous");
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

    public void testReaderStaysExternalOnly() {
        // reader remains an EXTERNAL-only dev knob: it is never accepted as a dataset setting, with or
        // without a format-aware validator.
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("reader", "java")));
        expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://b/data.csv", Map.of("reader", "java"))
        );
    }

    public void testNoResolverRejectsFormat() {
        // Without a FormatConfigKeyResolver the validator cannot validate a format value, so format
        // (and any format-specific key) is rejected, preserving pre-feature behavior.
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("format", "csv")));
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

    // --- Explicit `format` setting + resolved-format validation ---

    public void testExplicitFormatEnablesFormatSettingOnBarePrefix() {
        // The headline bug fix: a bare prefix (no extension) carries no format, but an explicit
        // `format` lets it accept that format's settings.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", "csv", "delimiter", "|"));
        assertEquals("csv", result.get("format"));
        assertEquals("|", result.get("delimiter"));
    }

    public void testExplicitFormatStored() {
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", "csv"));
        assertEquals("csv", result.get("format"));
    }

    public void testExplicitFormatIsCaseInsensitive() {
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", "CSV", "delimiter", "|"));
        // "CSV" resolves case-insensitively to the csv format and is stored in its canonical lowercase form.
        assertEquals("csv", result.get("format"));
        assertEquals("|", result.get("delimiter"));
    }

    public void testExplicitFormatOverridesExtension() {
        // Explicit format wins over the resource extension.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.parquet", Map.of("format", "csv", "delimiter", "|"));
        assertEquals("csv", result.get("format"));
        assertEquals("|", result.get("delimiter"));
    }

    public void testExplicitFormatRejectsForeignSetting() {
        // csv accepts `delimiter`; use a key no registered format recognises to prove that a setting
        // foreign to the resolved format is rejected.
        expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", "csv", "not_a_csv_key", "x"))
        );
    }

    public void testWhitespacePaddedFormatAccepted() {
        // format is normalized (trim, then lowercase) identically at create and query time, so
        // surrounding whitespace is tolerated rather than rejected.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", " CSV ", "delimiter", "|"));
        assertEquals("|", result.get("delimiter"));
        // The stored format value is normalized so round-trip and SchemaCacheKey agree on "csv".
        assertEquals("csv", result.get("format"));
    }

    public void testExplicitFormatStoredNormalized() {
        // format is normalized to lowercase before storage so "CSV" and "csv" produce the same
        // cluster-state representation and the same SchemaCacheKey string at query time.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", "CSV", "delimiter", "|"));
        assertEquals("csv", result.get("format"));
        assertEquals("|", result.get("delimiter"));
    }

    public void testFormatAutoStoredNormalized() {
        // format=auto is stored as the canonical "auto" string so it round-trips cleanly and
        // FormatNameResolver treats it as the extension-inference sentinel at query time.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.csv", Map.of("format", "AUTO", "delimiter", "|"));
        assertEquals("auto", result.get("format"));
        assertEquals("|", result.get("delimiter"));
    }

    public void testNullResourceWithFormatSpecificSettingNoNullInError() {
        // A missing resource yields exactly two errors: the required-resource error and a generic
        // unknown-setting error for the format-specific key. The targeted "set format" hint only fires
        // when a resource URI is present to anchor it, so there is no "cannot determine format for [null]".
        var e = expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), null, Map.of("delimiter", "|"))
        );
        assertThat(e.validationErrors(), hasSize(2));
        assertThat(e.validationErrors(), hasItem("[resource] is required"));
        // The "known settings: [...]" suffix lists an unordered set, so match only the stable prefix.
        assertThat(e.validationErrors(), hasItem(containsString("unknown setting [delimiter]")));
    }

    public void testUnknownExplicitFormatRejected() {
        var e = expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", "bogus"))
        );
        assertEquals(List.of(FileDataSourceValidator.unknownFormatError("bogus", KNOWN_FORMATS)), e.validationErrors());
    }

    public void testUnknownExplicitFormatWithFormatKeyYieldsSingleError() {
        // A bad explicit format short-circuits: exactly one error, no extra "set format"/unknown-setting noise.
        var e = expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", "bogus", "delimiter", "|"))
        );
        assertEquals(List.of(FileDataSourceValidator.unknownFormatError("bogus", KNOWN_FORMATS)), e.validationErrors());
    }

    public void testUnknownFormatWithFormatSettingGivesSetFormatHint() {
        // No explicit format, unknown extension, format-specific setting present: targeted hint.
        var e = expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("delimiter", "|"))
        );
        assertEquals(List.of(FileDataSourceValidator.cannotDetermineFormatError("s3://test", Set.of("delimiter"))), e.validationErrors());
    }

    public void testUnknownFormatGenuineTypoReportedAsUnknownSetting() {
        // No explicit format, unknown extension, a key no registered format recognises: this is a real
        // typo and must read as an unknown setting, not a misleading "set format" hint.
        var e = expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("not_a_setting", "x"))
        );
        assertThat(e.validationErrors(), hasSize(1));
        assertThat(e.validationErrors().get(0), containsString("unknown setting [not_a_setting]"));
        assertThat(e.getMessage(), not(containsString("cannot determine format")));
    }

    public void testUnknownFormatMixedKeysReportBothDiagnoses() {
        // A real format-specific key gets the "set format" hint; a genuine typo gets "unknown setting".
        var e = expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("delimiter", "|", "not_a_setting", "x"))
        );
        assertThat(e.validationErrors(), hasItem(FileDataSourceValidator.cannotDetermineFormatError("s3://test", Set.of("delimiter"))));
        assertThat(e.validationErrors(), hasItem(containsString("unknown setting [not_a_setting]")));
    }

    public void testUnknownFormatBaseSettingsOnlyAccepted() {
        // No explicit format, unknown extension, only base settings -> accepted (resolves per-file at query).
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("partition_detection", "hive"));
        assertEquals("hive", result.get("partition_detection"));
    }

    public void testFormatAutoFallsBackToExtension() {
        // `auto` means "infer from extension": a .csv resource then accepts CSV settings.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.csv", Map.of("format", "auto", "delimiter", "|"));
        assertEquals("|", result.get("delimiter"));
    }

    public void testFormatAutoOnBarePrefixWithFormatSettingRejected() {
        // `auto` + no extension cannot resolve a format, so a format-specific setting is rejected.
        expectThrows(
            ValidationException.class,
            () -> formatAwareValidator.validateDataset(Map.of(), "s3://test", Map.of("format", "auto", "delimiter", "|"))
        );
    }
}
