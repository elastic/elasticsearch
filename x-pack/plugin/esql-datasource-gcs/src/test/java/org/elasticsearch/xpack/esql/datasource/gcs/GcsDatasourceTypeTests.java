/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gcs;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DatasourceType;
import org.elasticsearch.xpack.esql.datasources.spi.FileDatasourceType;

import java.util.Map;
import java.util.Set;

public class GcsDatasourceTypeTests extends ESTestCase {

    private final DatasourceType type = new FileDatasourceType("gcs", GcsConfiguration::fromMap, Set.of("gs://"));

    public void testType() {
        assertEquals("gcs", type.type());
    }

    public void testValidateDatasourceWithCredentials() {
        var result = type.validateDatasource(Map.of("credentials", "{\"type\":\"service_account\"}", "project_id", "proj"));
        assertEquals("{\"type\":\"service_account\"}", result.get("credentials").value());
        assertTrue(result.get("credentials").isSecret());
        assertFalse(result.get("project_id").isSecret());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(type.validateDatasource(Map.of()).isEmpty());
    }

    public void testValidateDatasourceRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDatasource(Map.of("bucket", "x")));
    }

    public void testValidateDatasourceNormalizesAuth() {
        var result = type.validateDatasource(Map.of("auth", "NONE"));
        assertEquals("none", result.get("auth").value());
    }

    public void testValidateDatasetValid() {
        var result = type.validateDataset(Map.of(), "gs://bucket/path/*.parquet", Map.of("partition_detection", "hive"));
        assertEquals("hive", result.get("partition_detection").value());
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "s3://bucket/path", Map.of()));
    }

    public void testValidateDatasetRejectsUnknown() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "gs://b/p", Map.of("format", "parquet")));
    }

    public void testValidateDatasourceRejectsInvalidAuth() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDatasource(Map.of("auth", "oauth2")));
    }

    public void testValidateDatasourceAnonymousConflict() {
        expectThrows(
            IllegalArgumentException.class,
            () -> type.validateDatasource(Map.of("auth", "none", "credentials", "{\"type\":\"service_account\"}"))
        );
    }

    public void testValidateDatasetRequiresResource() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), null, Map.of()));
    }

    public void testValidateDatasetBlankResource() {
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "", Map.of()));
    }

    public void testValidateDatasetSchemaSampleSize() {
        var result = type.validateDataset(Map.of(), "gs://b/p", Map.of("schema_sample_size", 50));
        assertEquals("50", result.get("schema_sample_size").value());
        expectThrows(IllegalArgumentException.class, () -> type.validateDataset(Map.of(), "gs://b/p", Map.of("schema_sample_size", 0)));
    }
}
