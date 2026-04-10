/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;

import java.util.Map;
import java.util.Set;

public class AzureDataSourceValidatorTests extends ESTestCase {

    private final DataSourceValidator validator = new FileDataSourceValidator(
        "azure",
        AzureConfiguration::fromMap,
        Set.of("wasbs", "wasb")
    );

    public void testType() {
        assertEquals("azure", validator.type());
    }

    public void testValidateDatasourceWithSharedKey() {
        var result = validator.validateDatasource(Map.of("account", "myaccount", "key", "mykey"));
        assertFalse(result.get("account").secret());
        assertTrue(result.get("key").secret());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(validator.validateDatasource(Map.of()).isEmpty());
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
            () -> validator.validateDatasource(Map.of("auth", "none", "connection_string", "DefaultEndpointsProtocol=https"))
        );
    }

    public void testValidateDatasourceAnonymousConflictSasToken() {
        expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "sas_token", "?sv=2020-01-01"))
        );
    }

    public void testValidateDatasourceWithSasToken() {
        assertTrue(validator.validateDatasource(Map.of("sas_token", "?sv=2020")).get("sas_token").secret());
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

    public void testValidateDatasourceNullSettings() {
        assertTrue(validator.validateDatasource(null).isEmpty());
    }

    public void testValidateDatasourceSkipsNullValues() {
        var settings = new java.util.HashMap<String, Object>();
        settings.put("account", "myaccount");
        settings.put("endpoint", null);
        var result = validator.validateDatasource(settings);
        assertEquals("myaccount", result.get("account").value());
        assertNull(result.get("endpoint"));
    }

    public void testValidateDatasetBlankResource() {
        expectThrows(org.elasticsearch.common.ValidationException.class, () -> validator.validateDataset(Map.of(), "", Map.of()));
    }

    public void testValidateDatasetNullSettings() {
        assertTrue(validator.validateDataset(Map.of(), "wasbs://c@a.blob.core.windows.net/p", null).isEmpty());
    }

    public void testValidateDatasetAccumulatesMultipleErrors() {
        var e = expectThrows(
            org.elasticsearch.common.ValidationException.class,
            () -> validator.validateDataset(
                Map.of(),
                "wasbs://c@a.blob.core.windows.net/p",
                Map.of("error_mode", "banana", "schema_sample_size", "abc")
            )
        );
        assertEquals(2, e.validationErrors().size());
    }

    public void testToStoredSettingsSecretClassification() {
        AzureConfiguration config = AzureConfiguration.fromMap(Map.of("account", "myaccount", "key", "mykey", "sas_token", "?sv=2020"));
        var result = config.toStoredSettings();
        assertFalse(result.get("account").secret());
        assertTrue(result.get("key").secret());
        assertTrue(result.get("sas_token").secret());
        assertEquals("myaccount", result.get("account").value());
    }
}
