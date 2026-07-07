/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractDataSourceValidatorTests;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class AzureDataSourceValidatorTests extends AbstractDataSourceValidatorTests {

    private final DataSourceValidator validator = new FileDataSourceValidator(
        "azure",
        AzureConfiguration::fromMap,
        Set.of("wasbs", "wasb")
    );

    @Override
    protected DataSourceValidator validator() {
        return validator;
    }

    @Override
    protected String expectedType() {
        return "azure";
    }

    @Override
    protected Map<String, Object> sampleConfigWithAllSecrets() {
        // Azure accepts one of three authentication forms; we exercise key-based auth which marks the
        // `key` field secret. connection_string and sas_token are mutually exclusive with key (the
        // validator rejects mixed-auth configs), so this sample tests the key path. The other two
        // auth forms are covered by testToStoredSettingsClassifiesAllAuthForms below.
        return Map.of("account", "sampleaccount", "key", "sample-key-base64=");
    }

    @Override
    protected Set<String> expectedSecretFieldNames() {
        return Set.of("key");
    }

    @Override
    protected String sampleResource() {
        return "wasbs://c@a.blob.core.windows.net/p/*.parquet";
    }

    @Override
    protected String wrongSchemeResource() {
        return "s3://bucket/path";
    }

    @Override
    protected Map<String, DataSourceSetting> storedSettingsFromSampleConfig() {
        return AzureConfiguration.fromMap(sampleConfigWithAllSecrets()).toStoredSettings();
    }

    @Override
    protected Map<String, Object> datasetSettingsWithMultipleErrors() {
        return Map.of("error_mode", "banana", "schema_sample_size", "abc");
    }

    public void testValidateDatasourceWithSharedKey() {
        var result = validator.validateDatasource(Map.of("account", "myaccount", "key", "mykey"));
        assertFalse(result.get("account").secret());
        assertTrue(result.get("key").secret());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDatasource(Map.of("container", "x")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "managed_identity"))
        );
    }

    public void testValidateDatasourceAnonymousConflictConnectionString() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "anonymous", "connection_string", "DefaultEndpointsProtocol=https"))
        );
    }

    public void testValidateDatasourceAnonymousConflictSasToken() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "anonymous", "sas_token", "?sv=2020-01-01"))
        );
    }

    public void testValidateDatasourceRejectsWorkloadIdentityWhenDisabled() {
        // default validator has workload identity disabled
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "managed_identity"))
        );
        assertThat(e.getMessage(), containsString("esql.datasource.managed_identity.enabled"));
    }

    public void testValidateDatasourceAcceptsWorkloadIdentityWhenEnabled() {
        var workloadIdentityValidator = new FileDataSourceValidator("azure", AzureConfiguration::fromMap, Set.of("wasbs", "wasb"))
            .withManagedIdentityEnabled(() -> true);
        var result = workloadIdentityValidator.validateDatasource(Map.of("auth", "managed_identity", "account", "myaccount"));
        assertEquals("managed_identity", result.get("auth").nonSecretValue());
        assertFalse(result.get("auth").secret());
    }

    public void testValidateDatasourceWorkloadIdentityConflictWithCredentials() {
        var workloadIdentityValidator = new FileDataSourceValidator("azure", AzureConfiguration::fromMap, Set.of("wasbs", "wasb"))
            .withManagedIdentityEnabled(() -> true);
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> workloadIdentityValidator.validateDatasource(Map.of("auth", "managed_identity", "account", "myaccount", "key", "mykey"))
        );
    }

    public void testValidateDatasourceRejectsExplicitFederatedWhenDisabled() {
        // default validator has federated authentication disabled
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(
                Map.of(
                    "auth",
                    "federated_identity",
                    "tenant_id",
                    "tenant",
                    "client_id",
                    "client",
                    "jwt_audience",
                    "api://AzureADTokenExchange"
                )
            )
        );
        assertThat(e.getMessage(), containsString("esql_external_datasources_federated_identity"));
    }

    public void testValidateDatasourceRejectsImplicitFederatedWhenDisabled() {
        // default validator has federated authentication disabled
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(
                Map.of("tenant_id", "tenant", "client_id", "client", "jwt_audience", "api://AzureADTokenExchange")
            )
        );
        assertThat(e.getMessage(), containsString("esql_external_datasources_federated_identity"));
    }

    public void testValidateDatasourceAcceptsFederatedWhenEnabled() {
        var federatedValidator = new FileDataSourceValidator("azure", AzureConfiguration::fromMap, Set.of("wasbs", "wasb"))
            .withFederatedIdentityEnabled(() -> true);
        var result = federatedValidator.validateDatasource(
            Map.of("auth", "federated_identity", "tenant_id", "tenant", "client_id", "client", "jwt_audience", "api://AzureADTokenExchange")
        );
        assertEquals("tenant", result.get("tenant_id").nonSecretValue());
        assertFalse(result.get("tenant_id").secret());
    }

    public void testValidateDatasourceWithSasToken() {
        // account + sas_token is the complete SAS form (sas_token alone is rejected as incomplete).
        assertTrue(validator.validateDatasource(Map.of("account", "acc", "sas_token", "?sv=2020")).get("sas_token").secret());
    }

    public void testValidateDatasourceWithConnectionString() {
        assertTrue(
            validator.validateDatasource(Map.of("connection_string", "DefaultEndpointsProtocol=https;AccountName=x"))
                .get("connection_string")
                .secret()
        );
    }

    public void testValidateDatasetValid() {
        var result = validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p/*.parquet", Map.of("error_mode", "skip_row"));
        assertEquals("skip_row", result.get("error_mode"));
    }

    public void testValidateDatasetBothSchemes() {
        for (String uri : new String[] { "wasbs://c@a.blob.core.windows.net/p", "wasb://c@a.blob.core.windows.net/p" }) {
            assertNotNull(validator.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("format", "parquet"))
        );
    }

    public void testValidateDatasetSchemaSampleSize() {
        assertEquals(
            50,
            validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("schema_sample_size", 50))
                .get("schema_sample_size")
        );
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("schema_sample_size", 0))
        );
    }

    public void testValidateDatasetSchemaResolution() {
        assertEquals(
            "union_by_name",
            validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("schema_resolution", "union_by_name"))
                .get("schema_resolution")
        );
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("schema_resolution", "banana"))
        );
    }

    public void testValidateDatasetErrorBudget() {
        assertEquals(
            "100",
            validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("max_errors", "100")).get("max_errors")
        );
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(
                Map.of(),
                "wasbs://c@a.blob.core.windows.net/p",
                Map.of("error_mode", "fail_fast", "max_errors", "10")
            )
        );
    }

    public void testValidateDatasetTargetSplitSize() {
        assertEquals(
            "64mb",
            validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("target_split_size", "64mb"))
                .get("target_split_size")
        );
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", Map.of("target_split_size", "abc"))
        );
    }

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new java.util.HashMap<String, Object>();
        // auth=anonymous makes the credential-less config resolvable; the null-skipping behavior is what's under test.
        settings.put("auth", "anonymous");
        settings.put("account", "myaccount");
        settings.put("endpoint", null);
        var result = validator.validateDatasource(settings);
        assertEquals("myaccount", result.get("account").nonSecretValue());
        assertNull(result.get("endpoint"));
    }

    /**
     * Extra Azure-specific coverage on top of the inherited {@code testToStoredSettingsSecretClassification}:
     * walks all three Azure auth forms through {@code fromMap → toStoredSettings} in a single call to
     * confirm each is classified as secret. The inherited base test only covers the key form (which is
     * what {@link #sampleConfigWithAllSecrets()} declares); this fills in sas_token and connection_string.
     */
    public void testToStoredSettingsClassifiesAllAuthForms() {
        AzureConfiguration config = AzureConfiguration.fromMap(Map.of("account", "myaccount", "key", "mykey", "sas_token", "?sv=2020"));
        var result = config.toStoredSettings();
        assertFalse(result.get("account").secret());
        assertTrue(result.get("key").secret());
        assertTrue(result.get("sas_token").secret());
        assertEquals("myaccount", result.get("account").nonSecretValue());
    }
}
