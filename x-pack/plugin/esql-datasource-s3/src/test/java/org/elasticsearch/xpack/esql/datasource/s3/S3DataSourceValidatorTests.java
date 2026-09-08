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

    private final DataSourceValidator validator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"))
        .withResourceCheck(S3ResourceCheck::validate);

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
        "trim_spaces",
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
        assertThat(e.getMessage(), containsString("esql.external.managed_identity.enabled"));
    }

    public void testValidateDatasourceRejectsDeprecatedWorkloadIdentityWhenDisabled() {
        // The deprecated alias canonicalizes to managed_identity, so the disabled-gate still catches it (and warns).
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "workload_identity", "region", "us-east-1"))
        );
        assertThat(e.getMessage(), containsString("esql.external.managed_identity.enabled"));
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

    public void testValidateDatasourceRejectsExplicitFederatedWhenDisabled() {
        // default validator has federated authentication disabled
        var federatedConfig = Map.<String, Object>of(
            "auth",
            "federated_identity",
            "role_arn",
            "arn:aws:iam::123456789012:role/example",
            "jwt_audience",
            "sts.amazonaws.com",
            "region",
            "us-east-1"
        );
        var e = expectThrows(ValidationException.class, () -> validator.validateDatasource(federatedConfig));
        assertThat(e.getMessage(), containsString("esql.external.federated_identity.enabled"));
    }

    public void testValidateDatasourceRejectsImplicitFederatedWhenDisabled() {
        // default validator has federated authentication disabled
        var federatedConfig = Map.<String, Object>of(
            "role_arn",
            "arn:aws:iam::123456789012:role/example",
            "jwt_audience",
            "sts.amazonaws.com",
            "region",
            "us-east-1"
        );
        var e = expectThrows(ValidationException.class, () -> validator.validateDatasource(federatedConfig));
        assertThat(e.getMessage(), containsString("esql.external.federated_identity.enabled"));
    }

    public void testValidateDatasourceAcceptsFederatedWhenEnabled() {
        var federatedValidator = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"))
            .withFederatedIdentityEnabled(() -> true);
        var result = federatedValidator.validateDatasource(
            Map.of(
                "auth",
                "federated_identity",
                "role_arn",
                "arn:aws:iam::123456789012:role/example",
                "jwt_audience",
                "sts.amazonaws.com",
                "region",
                "us-east-1"
            )
        );
        assertEquals("arn:aws:iam::123456789012:role/example", result.get("role_arn").nonSecretValue());
        assertFalse(result.get("role_arn").secret());
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

    public void testValidateDatasetExclusionSettingsValid() {
        Map<String, Object> result = validator.validateDataset(
            Map.of(),
            "s3://bucket/path/*.parquet",
            Map.of("file_exclusions", List.of("**/_*", "**/.*", "**/_temporary/**"))
        );
        assertEquals(
            "stored verbatim, since patterns are case-sensitive user data",
            List.of("**/_*", "**/.*", "**/_temporary/**"),
            result.get("file_exclusions")
        );
    }

    /** The empty list is a legitimate value: exclude nothing. */
    public void testValidateDatasetExclusionSettingsAcceptEmptyList() {
        Map<String, Object> result = validator.validateDataset(Map.of(), "s3://b/p", Map.of("file_exclusions", List.of()));
        assertEquals(List.of(), result.get("file_exclusions"));
    }

    /** Entries are ordinary resource patterns, so a directory pattern is legal rather than refused. */
    public void testValidateDatasetExclusionAcceptsADirectoryPattern() {
        Map<String, Object> result = validator.validateDataset(
            Map.of(),
            "s3://b/p",
            Map.of("file_exclusions", List.of("**/_temporary/**"))
        );
        assertEquals(List.of("**/_temporary/**"), result.get("file_exclusions"));
    }

    public void testValidateDatasetExclusionRejectsAnUnparseablePattern() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("file_exclusions", List.of("a[b")))
        );
        assertThat(e.validationErrors(), hasSize(1));
        assertThat(e.getMessage(), containsString("must contain only valid patterns"));
        assertThat(e.getMessage(), containsString("unterminated character class"));
    }

    /** A shape problem is reported once, by the list validator, not twice by the owning parser as well. */
    public void testValidateDatasetExclusionRejectsNonListWithOneMessage() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("file_exclusions", "**/_*"))
        );
        assertThat(e.validationErrors(), hasSize(1));
        assertThat(e.getMessage(), containsString("must be a JSON array of strings"));
    }

    public void testValidateDatasetExclusionRejectsNonStringElementWithOneMessage() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("file_exclusions", List.of(42)))
        );
        assertThat(e.validationErrors(), hasSize(1));
        assertThat(e.getMessage(), containsString("must be a JSON array of non-empty strings"));
    }

    public void testValidateDatasetPartitionDetectionInvalid() {
        ValidationException e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "banana"))
        );
        // One actionable message, not that message plus Enum.valueOf's raw "No enum constant ...".
        assertThat(e.validationErrors(), hasSize(1));
        assertThat(e.getMessage(), not(containsString("No enum constant")));
    }

    public void testValidateDatasetPartitionDetectionAllValues() {
        for (String strategy : new String[] { "auto", "hive", "none", "AUTO", "HIVE", "NONE" }) {
            assertEquals(
                strategy,
                validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", strategy)).get("partition_detection")
            );
        }
        // template carries its path template with it; on its own it would be a strategy that detects nothing.
        for (String strategy : new String[] { "template", "TEMPLATE" }) {
            assertEquals(
                strategy,
                validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", strategy, "partition_path", "{year}"))
                    .get("partition_detection")
            );
        }
    }

    /**
     * The three combinations in which one of the partition settings would be silently ignored. Rejected at
     * registration only — {@code PartitionConfig.fromConfig} still resolves them leniently so datasets stored
     * before this validation existed keep reading.
     */
    public void testValidateDatasetRejectsSilentlyIgnoredPartitionSettings() {
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "template"))
        );
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "hive", "hive_partitioning", "false"))
        );
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "none", "partition_path", "{year}"))
        );
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("hive_partitioning", "false", "partition_path", "{year}"))
        );
        // hive never reads a path template, so storing one would store a setting that does nothing.
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "hive", "partition_path", "{year}"))
        );
    }

    /** hive_partitioning:true asserts nothing — it is the default — so it never contradicts a strategy. */
    public void testValidateDatasetAcceptsHivePartitioningTrueWithAnyStrategy() {
        assertEquals(
            "hive",
            validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "hive", "hive_partitioning", "true"))
                .get("partition_detection")
        );
        assertEquals(
            "none",
            validator.validateDataset(Map.of(), "s3://b/p", Map.of("partition_detection", "none", "hive_partitioning", "true"))
                .get("partition_detection")
        );
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
        // The bound must admit the readers' own default (20000); it used to stop at 1000, which made every value
        // from 1001 up -- including the default -- unregisterable. See FileDataSourceValidatorSampleSizeBoundTests.
        assertEquals(1001, validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 1001)).get("schema_sample_size"));
        assertEquals(
            20_000,
            validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 20_000)).get("schema_sample_size")
        );
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 20_001))
        );
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

    public void testValidateDatasetFileSortByRequiresFirstFileWins() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_resolution", "union_by_name", "file_sort_by", "list"))
        );
        assertThat(e.getMessage(), containsString("file_sort_by"));
        assertThat(e.getMessage(), containsString("file_order"));
        assertThat(e.getMessage(), containsString("first_file_wins"));
    }

    public void testValidateDatasetFileOrderRequiresFirstFileWins() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("schema_resolution", "strict", "file_order", "desc"))
        );
        assertThat(e.getMessage(), containsString("file_sort_by"));
        assertThat(e.getMessage(), containsString("file_order"));
        assertThat(e.getMessage(), containsString("first_file_wins"));
    }

    public void testValidateDatasetFileSortByRejectedWhenSchemaResolutionOmitted() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("file_sort_by", "name"))
        );
        assertThat(e.getMessage(), containsString("first_file_wins"));
    }

    public void testValidateDatasetFileSortByAcceptedWithFirstFileWins() {
        Map<String, Object> result = validator.validateDataset(
            Map.of(),
            "s3://b/p",
            Map.of("schema_resolution", "first_file_wins", "file_sort_by", "mtime", "file_order", "desc")
        );
        assertEquals("first_file_wins", result.get("schema_resolution"));
        assertEquals("mtime", result.get("file_sort_by"));
        assertEquals("desc", result.get("file_order"));
    }

    public void testValidateDatasetUnknownFileSortByRejected() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(
                Map.of(),
                "s3://b/p",
                Map.of("schema_resolution", "first_file_wins", "file_sort_by", "created_time")
            )
        );
        assertThat(e.getMessage(), containsString("Unknown file_sort_by value [created_time]"));
        assertThat(e.getMessage(), containsString("list, name, mtime"));
    }

    public void testValidateDatasetLegacySortByAndOrderAreUnknownSettings() {
        var sortBy = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("sort_by", "name"))
        );
        assertThat(sortBy.getMessage(), containsString("unknown setting [sort_by]"));
        var order = expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("order", "desc")));
        assertThat(order.getMessage(), containsString("unknown setting [order]"));
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

    public void testValidateDatasetMaxSplitProbesAboveTheCeilingRejected() {
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("max_split_probes", "10001")));
    }

    /**
     * A window that fits the budget against the default probe count, and one that does not. The absent key has to
     * be resolved to its default for the second to be caught here rather than at query time.
     */
    public void testValidateDatasetProbeBudgetCountsTheAbsentKeysDefault() {
        assertEquals("4mb", validator.validateDataset(Map.of(), "s3://b/p", Map.of("split_probe_window", "4mb")).get("split_probe_window"));
        expectThrows(ValidationException.class, () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("split_probe_window", "8mb")));
    }

    /** Two values each acceptable alone, rejected for the reads they ask for together. */
    public void testValidateDatasetProbeBudgetRejectsTheProductOfTwoValidKeys() {
        assertEquals("1mb", validator.validateDataset(Map.of(), "s3://b/p", Map.of("split_probe_window", "1mb")).get("split_probe_window"));
        assertEquals("8000", validator.validateDataset(Map.of(), "s3://b/p", Map.of("max_split_probes", "8000")).get("max_split_probes"));
        expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://b/p", Map.of("split_probe_window", "1mb", "max_split_probes", "8000"))
        );
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
        // The "known settings: [...]" suffix is sorted, so it is stable across JVM runs and can be matched
        // in full. Matching only the prefix would let the list go back to Set.of iteration order unnoticed.
        assertThat(e.validationErrors(), hasItem(containsString("unknown setting [delimiter]")));
        assertThat(
            e.validationErrors(),
            hasItem(
                containsString(
                    "known settings: [error_mode, file_exclusions, file_order, file_sort_by, format, hive_partitioning, "
                        + "max_error_ratio, max_errors, max_split_probes, partition_detection, partition_path, "
                        + "schema_resolution, schema_sample_size, split_probe_window, target_split_size]"
                )
            )
        );
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
        assertThat(e.validationErrors().get(0), containsString("file_sort_by"));
        assertThat(e.validationErrors().get(0), containsString("file_order"));
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

    // --- ARN and MRAP resource rejection ---

    public void testValidateDatasetRejectsMrapHost() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://mfzwi23gnjvgw.mrap/data/f.parquet", Map.of())
        );
        assertThat(e.getMessage(), containsString("looks like a multi-region access point, which is not supported"));
        assertThat(e.getMessage(), containsString("s3://mfzwi23gnjvgw.mrap/data/f.parquet"));
    }

    public void testValidateDatasetRejectsMrapArn() {
        // ARN whose first path segment ends with .mrap — gets MRAP message, not generic ARN message.
        // The not() assertion catches a missing return after the MRAP branch.
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(
                Map.of(),
                "s3://arn:aws:s3::444732909647:accesspoint/mfzwi23gnjvgw.mrap/data/f.parquet",
                Map.of()
            )
        );
        assertThat(e.getMessage(), containsString("looks like a multi-region access point, which is not supported"));
        assertThat(e.getMessage(), not(containsString("does not accept an ARN")));
    }

    public void testValidateDatasetRejectsMrapFqdn() {
        // Full AWS global endpoint hostname that MRAP aliases resolve to — must get the MRAP message.
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(
                Map.of(),
                "s3://mfzwi23gnjvgw.mrap.accesspoint.s3-global.amazonaws.com/data/f.parquet",
                Map.of()
            )
        );
        assertThat(e.getMessage(), containsString("looks like a multi-region access point, which is not supported"));
        assertThat(e.getMessage(), not(containsString("does not accept an ARN")));
    }

    public void testValidateDatasetRejectsAccessPointArn() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://arn:aws:s3:us-east-1:444732909647:accesspoint/my-ap/data/f.parquet", Map.of())
        );
        assertThat(e.getMessage(), containsString("does not accept an ARN"));
        assertThat(e.getMessage(), containsString("Use a bucket name, or an access point alias"));
    }

    public void testValidateDatasetRejectsObjectLambdaArn() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(
                Map.of(),
                "s3://arn:aws:s3-object-lambda:us-east-1:444732909647:accesspoint/olap/data/f.parquet",
                Map.of()
            )
        );
        assertThat(e.getMessage(), containsString("does not accept an ARN"));
    }

    public void testValidateDatasetRejectsOutpostsArn() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(
                Map.of(),
                "s3://arn:aws:s3-outposts:us-east-1:444732909647:outpost/op-x/accesspoint/ap/data/f.parquet",
                Map.of()
            )
        );
        assertThat(e.getMessage(), containsString("does not accept an ARN"));
    }

    public void testValidateDatasetRejectsS3TablesArn() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://arn:aws:s3tables:us-east-1:444732909647:bucket/tb/ns/tbl", Map.of())
        );
        assertThat(e.getMessage(), containsString("does not accept an ARN"));
    }

    public void testValidateDatasetRejectsAccessGrantsArn() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(
                Map.of(),
                "s3://arn:aws:s3:us-east-1:444732909647:access-grants/default/data/f.parquet",
                Map.of()
            )
        );
        assertThat(e.getMessage(), containsString("does not accept an ARN"));
    }

    public void testValidateDatasetArnCheckIsCaseInsensitive() {
        // ARN prefix matching must be case-insensitive
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://ARN:aws:s3:us-east-1:123:accesspoint/x/f.parquet", Map.of())
        );
        assertThat(e.getMessage(), containsString("does not accept an ARN"));
    }

    public void testValidateDatasetMrapCheckIsCaseInsensitive() {
        // .mrap suffix matching must be case-insensitive
        var e = expectThrows(
            ValidationException.class,
            () -> validator.validateDataset(Map.of(), "s3://mfzwi23gnjvgw.MRAP/data/f.parquet", Map.of())
        );
        assertThat(e.getMessage(), containsString("looks like a multi-region access point, which is not supported"));
    }

    public void testValidateDatasetAcceptsNormalBucket() {
        // control — must not be rejected
        assertNotNull(validator.validateDataset(Map.of(), "s3://my-bucket/data/f.parquet", Map.of()));
    }

    public void testValidateDatasetAcceptsMrapInKey() {
        // .mrap appears in the key path, not the host — legal bucket + key, must be accepted
        assertNotNull(validator.validateDataset(Map.of(), "s3://my-bucket/archive.mrap/data.parquet", Map.of()));
    }

    public void testValidateDatasetAccessPointAlias() {
        assertNotNull(validator.validateDataset(Map.of(), "s3://my-ap-a1b2c3d4e-s3alias/data/f.parquet", Map.of()));
    }

    public void testValidateDatasetObjectLambdaAlias() {
        assertNotNull(validator.validateDataset(Map.of(), "s3://myolap-1a4n8t--ol-s3/data/f.parquet", Map.of()));
    }

    public void testValidateDatasetOutpostsAlias() {
        assertNotNull(validator.validateDataset(Map.of(), "s3://my-access-po-o01ac--op-s3/data/f.parquet", Map.of()));
    }

    public void testValidateDatasetExpressDirectoryBucket() {
        assertNotNull(validator.validateDataset(Map.of(), "s3://my-bucket--use1-az4--x-s3/data/f.parquet", Map.of()));
    }

    public void testValidateDatasetDottedBucketName() {
        assertNotNull(validator.validateDataset(Map.of(), "s3://my.dotted.bucket/data/f.parquet", Map.of()));
    }

    public void testValidateDatasetMrapInsideNameNotRejected() {
        // ends-with check: ".mrap" must appear at the end, not as a substring in the middle
        assertNotNull(validator.validateDataset(Map.of(), "s3://bucket-with.mrap-inside/data/f.parquet", Map.of()));
    }

    public void testValidateDatasetArnPrefixWithoutColonNotRejected() {
        // "arn" without the colon is a legal bucket name prefix — only "arn:" triggers the check
        assertNotNull(validator.validateDataset(Map.of(), "s3://arnold-bucket/data/f.parquet", Map.of()));
    }

    /**
     * The load-bearing rewrap-survival test: the S3 resource check must survive all three EsqlPlugin
     * withers. If the resourceCheck field is missing from any wither's private-constructor call, it is
     * silently dropped and ARN/MRAP resources pass validation after the re-wrap.
     */
    public void testResourceCheckSurvivesAllWithers() {
        FileDataSourceValidator v = new FileDataSourceValidator("s3", S3Configuration::fromMap, Set.of("s3", "s3a", "s3n"))
            .withResourceCheck(S3ResourceCheck::validate)
            .withManagedIdentityEnabled(() -> false)
            .withFederatedIdentityEnabled(() -> false)
            .withFormatConfigKeyResolver(CSV_RESOLVER, Set.of(".gz"));

        var e = expectThrows(
            ValidationException.class,
            () -> v.validateDataset(Map.of(), "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-ap/data/f.parquet", Map.of())
        );
        assertThat(e.getMessage(), containsString("does not accept an ARN"));

        var e2 = expectThrows(
            ValidationException.class,
            () -> v.validateDataset(Map.of(), "s3://mfzwi23gnjvgw.mrap/data/f.parquet", Map.of())
        );
        assertThat(e2.getMessage(), containsString("looks like a multi-region access point, which is not supported"));
    }

    /**
     * Verifies that {@link S3DataSourcePlugin#datasourceValidators} wires the S3 resource check.
     * If {@code .withResourceCheck(...)} were dropped from the plugin, this test would fail while
     * unit tests that construct {@link FileDataSourceValidator} directly would still pass.
     */
    public void testDatasourceValidatorsIncludesResourceCheck() {
        DataSourceValidator v = new S3DataSourcePlugin().datasourceValidators(org.elasticsearch.common.settings.Settings.EMPTY).get("s3");
        var e = expectThrows(
            ValidationException.class,
            () -> v.validateDataset(Map.of(), "s3://arn:aws:s3:us-east-1:123456789012:accesspoint/my-ap/data/f.parquet", Map.of())
        );
        assertThat(e.getMessage(), containsString("does not accept an ARN"));
    }

    // --- Glob metacharacter and object-key special-character tests ---
    // '?' is a first-class glob metacharacter (StoragePath.GLOB_METACHARACTERS). Every object matched by
    // "day?.csv" ends in ".csv", so the format is inferable from the pattern's own extension. The validator
    // must not apply URL query-string semantics (truncation at '?') to object-store paths.

    public void testFormatAwareValidatorInfersFormatThroughQuestionMarkGlob() {
        // '?' is a glob metacharacter; every object this pattern matches ends in .csv.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/logs/day?.csv", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testFormatAwareValidatorInfersFormatThroughQuestionMarkGlobCompoundExtension() {
        // '?' glob + compound extension (.csv.gz): outer ext .gz triggers compression fallback,
        // inner ext .csv resolves the format. A naive strip at '?' would yield "day" (no extension).
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/logs/day?.csv.gz", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testFormatAwareValidatorInfersFormatThroughStarGlob() {
        // '*' glob: same extension guarantee, same fix must not regress it.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/logs/day*.csv", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testFormatAwareValidatorHashInObjectKeyInfersFormat() {
        // '#' is a legal object-store key character; it must not be treated as a URI fragment delimiter.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/report#1.csv", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testFormatAwareValidatorVersionIdQueryStringInfersFormat() {
        // '?' after the extension (e.g. S3 versionId URLs) must not break format inference.
        // FormatNameResolverTests pins this shape as supported at query time; CRUD must agree.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/file.csv?versionId=abc", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testFormatAwareValidatorFormatFlipEdgeCaseDocumented() {
        // An S3 key literally named "data.parquet?x=.csv": last extension is ".csv", so the validator
        // resolves format=csv and accepts CSV settings. Pre-fix this was rejected (ext ".parquet" maps
        // to no format in this test resolver); after fix it is accepted because the last dot wins.
        var result = formatAwareValidator.validateDataset(Map.of(), "s3://bucket/data.parquet?x=.csv", Map.of("delimiter", ";"));
        assertEquals(";", result.get("delimiter"));
    }

    public void testUnsupportedSchemeListsTheSchemesInAStableOrder() {
        // Nine schemes declared out of order. Set.of salts its iteration per JVM run; over nine elements the sorted
        // arrangement is not among the orderings it can produce, so with the renderer's sort removed this fails every
        // time. A three-element set does reach sorted order, which is why one is not a gate.
        DataSourceValidator manySchemes = new FileDataSourceValidator(
            "s3",
            S3Configuration::fromMap,
            Set.of("s3n", "s3a", "zs3", "ms3", "as3", "s3", "ks3", "bs3", "ys3")
        );

        var e = expectThrows(
            ValidationException.class,
            () -> manySchemes.validateDataset(Map.of(), "ftp://bucket/data/good.csv", Map.of())
        );

        assertThat(
            e.validationErrors(),
            hasItem(
                containsString(
                    "[resource] must use one of the supported URI schemes "
                        + "[as3://, bs3://, ks3://, ms3://, s3://, s3a://, s3n://, ys3://, zs3://]"
                )
            )
        );
    }
}
