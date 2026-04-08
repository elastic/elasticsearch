/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceType;
import org.elasticsearch.xpack.esql.datasources.spi.FileDatasourceType;

import java.util.Map;
import java.util.Set;

public class S3DatasourceTypeTests extends ESTestCase {

    private final DatasourceType type = new FileDatasourceType("s3", S3Configuration::fromMap, Set.of("s3://", "s3a://", "s3n://"));

    public void testType() {
        assertEquals("s3", type.type());
    }

    public void testValidateDatasourceWithCredentials() {
        var result = type.validateDatasource(Map.of("access_key", "AKIA123", "secret_key", "secret", "region", "us-east-1"));
        assertEquals("AKIA123", result.get("access_key").value());
        assertTrue(result.get("access_key").isSecret());
        assertTrue(result.get("secret_key").isSecret());
        assertEquals("us-east-1", result.get("region").value());
        assertFalse(result.get("region").isSecret());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(type.validateDatasource(Map.of()).isEmpty());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDatasource(Map.of("bucket", "x")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDatasource(Map.of("auth", "oauth2")));
    }

    public void testValidateDatasourceNormalizesAuth() {
        var result = type.validateDatasource(Map.of("auth", "NONE"));
        assertEquals("none", result.get("auth").value());
        assertFalse(result.get("auth").isSecret());
    }

    public void testValidateDatasetValid() {
        var result = type.validateDataset(Map.of(), "s3://bucket/path/*.parquet", Map.of("partition_detection", "hive"));
        assertEquals("hive", result.get("partition_detection").value());
        assertFalse(result.get("partition_detection").isSecret());
    }

    public void testValidateDatasetRequiresResource() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), null, Map.of()));
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "gs://bucket/path", Map.of()));
    }

    public void testValidateDatasetAllSchemes() {
        for (String uri : new String[] { "s3://b/p", "s3a://b/p", "s3n://b/p" }) {
            assertNotNull(type.validateDataset(Map.of(), uri, Map.of()));
        }
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "s3://b/p", Map.of("format", "parquet")));
    }

    public void testValidateDatasetSchemaSampleSize() {
        var result = type.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 50));
        assertEquals("50", result.get("schema_sample_size").value());
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", 0)));
    }

    public void testValidateDatasetSchemaSampleSizeNonNumber() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "s3://b/p", Map.of("schema_sample_size", "abc")));
    }

    public void testValidateDatasetBlankResource() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "", Map.of()));
    }

    public void testValidateDatasetNullSettings() {
        // null dataset settings should be treated as empty
        var result = type.validateDataset(Map.of(), "s3://b/p", null);
        assertTrue(result.isEmpty());
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            IllegalArgumentException.class,
            () -> type.validateDatasource(Map.of("auth", "none", "access_key", "AKIA123", "secret_key", "secret"))
        );
    }

    public void testValidateDatasourceNullSettings() {
        assertTrue(type.validateDatasource(null).isEmpty());
    }

    public void testValidateDatasourceSkipsNullValues() {
        // HashMap allows null values; they should be omitted from result
        var settings = new java.util.HashMap<String, Object>();
        settings.put("region", "us-east-1");
        settings.put("endpoint", null);
        var result = type.validateDatasource(settings);
        assertEquals("us-east-1", result.get("region").value());
        assertNull(result.get("endpoint"));
    }
}
