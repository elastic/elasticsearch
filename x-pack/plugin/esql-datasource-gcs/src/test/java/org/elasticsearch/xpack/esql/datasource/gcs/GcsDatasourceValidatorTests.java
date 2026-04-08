/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ConfigSetting;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDatasourceValidator;

import java.util.Map;
import java.util.Set;

public class GcsDatasourceValidatorTests extends ESTestCase {

    private final DatasourceValidator validator = new FileDatasourceValidator("gcs", GcsConfiguration::fromMap, Set.of("gs://"));

    public void testType() {
        assertEquals("gcs", validator.type());
    }

    public void testValidateDatasourceWithCredentials() {
        var result = validator.validateDatasource(Map.of("credentials", "{\"type\":\"service_account\"}", "project_id", "proj"));
        assertTrue(findKey(result, "credentials").isSecret());
        assertFalse(findKey(result, "project_id").isSecret());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(validator.validateDatasource(Map.of()).isEmpty());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDatasource(Map.of("bucket", "x")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDatasource(Map.of("auth", "oauth2")));
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDatasource(Map.of("auth", "none", "credentials", "{\"type\":\"service_account\"}"))
        );
    }

    public void testValidateDatasetValid() {
        var result = validator.validateDataset(Map.of(), "gs://bucket/path/*.parquet", Map.of("partition_detection", "hive"));
        assertEquals("hive", findValue(result, "partition_detection"));
    }

    public void testValidateDatasetRequiresResource() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), null, Map.of()));
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), "s3://bucket/path", Map.of()));
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("format", "parquet")));
    }

    public void testValidateDatasetSchemaSampleSize() {
        assertEquals(
            50,
            findValue(validator.validateDataset(Map.of(), "gs://b/p", Map.of("schema_sample_size", 50)), "schema_sample_size")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateDataset(Map.of(), "gs://b/p", Map.of("schema_sample_size", 0))
        );
    }

    private static Object findValue(Map<ConfigSetting, Object> settings, String name) {
        return settings.entrySet().stream().filter(e -> e.getKey().name().equals(name)).map(Map.Entry::getValue).findFirst().orElseThrow();
    }

    private static ConfigSetting findKey(Map<ConfigSetting, Object> settings, String name) {
        return settings.keySet().stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
    }
}
