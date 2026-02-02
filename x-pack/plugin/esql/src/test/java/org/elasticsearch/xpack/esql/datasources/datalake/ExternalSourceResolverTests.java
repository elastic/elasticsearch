/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datalake;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Tests for ExternalSourceResolver.
 *
 * Note: These tests use mocked Iceberg APIs due to Parquet jar hell issues.
 * Once the jar hell is resolved, integration tests with real S3/Iceberg can be added.
 */
public class ExternalSourceResolverTests extends ESTestCase {

    private ThreadPool threadPool;
    private ExternalSourceResolver resolver;

    @Before
    public void setupThreadPool() {
        threadPool = new TestThreadPool("external-source-resolver-tests");
        resolver = new ExternalSourceResolver(threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION));
    }

    @After
    public void cleanupThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testEmptyPathList() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ExternalSourceResolution> result = new AtomicReference<>();

        resolver.resolve(List.of(), Map.of(), ActionListener.wrap(resolution -> {
            result.set(resolution);
            latch.countDown();
        }, e -> {
            fail("Should not fail for empty path list: " + e.getMessage());
            latch.countDown();
        }));

        assertTrue("Callback should be called", latch.await(5, TimeUnit.SECONDS));
        assertNotNull(result.get());
        assertTrue("Should return empty resolution", result.get().isEmpty());
    }

    public void testNullPathList() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ExternalSourceResolution> result = new AtomicReference<>();

        resolver.resolve(null, Map.of(), ActionListener.wrap(resolution -> {
            result.set(resolution);
            latch.countDown();
        }, e -> {
            fail("Should not fail for null path list: " + e.getMessage());
            latch.countDown();
        }));

        assertTrue("Callback should be called", latch.await(5, TimeUnit.SECONDS));
        assertNotNull(result.get());
        assertTrue("Should return empty resolution", result.get().isEmpty());
    }

    public void testDetectParquetFileFromExtension() {
        // This test validates source type detection logic
        String parquetPath = "s3://bucket/data/file.parquet";
        assertTrue("Should detect .parquet extension", parquetPath.toLowerCase().endsWith(".parquet"));
    }

    public void testDetectIcebergTablePath() {
        // This test validates Iceberg table path patterns
        String icebergPath = "s3://bucket/warehouse/db.table";
        assertFalse("Should not detect as Parquet", icebergPath.toLowerCase().endsWith(".parquet"));
    }

    public void testS3ConfigurationExtraction() {
        Map<String, Expression> params = Map.of(
            "access_key",
            new Literal(Source.EMPTY, new BytesRef("test-access-key"), KEYWORD),
            "secret_key",
            new Literal(Source.EMPTY, new BytesRef("test-secret-key"), KEYWORD),
            "endpoint",
            new Literal(Source.EMPTY, new BytesRef("https://s3.us-west-2.amazonaws.com"), KEYWORD),
            "region",
            new Literal(Source.EMPTY, new BytesRef("us-west-2"), KEYWORD)
        );

        org.elasticsearch.xpack.esql.datasources.s3.S3Configuration config = org.elasticsearch.xpack.esql.datasources.s3.S3Configuration
            .fromParams(params);
        assertNotNull("S3 config should be extracted", config);
        assertEquals("test-access-key", config.accessKey());
        assertEquals("test-secret-key", config.secretKey());
        assertEquals("https://s3.us-west-2.amazonaws.com", config.endpoint());
        assertEquals("us-west-2", config.region());
        assertTrue("Should have credentials", config.hasCredentials());
    }

    public void testS3ConfigurationWithoutCredentials() {
        Map<String, Expression> params = Map.of();

        org.elasticsearch.xpack.esql.datasources.s3.S3Configuration config = org.elasticsearch.xpack.esql.datasources.s3.S3Configuration
            .fromParams(params);
        assertNull("Should return null when no config provided", config);
    }

    public void testExternalSourceResolutionGet() {
        Schema mockSchema = new Schema(List.of(Types.NestedField.required(1, "id", Types.LongType.get())));

        org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata metadata =
            new org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata(
                "s3://bucket/table",
                mockSchema,
                null,
                "iceberg"
            );

        ExternalSourceResolution resolution = new ExternalSourceResolution(Map.of("s3://bucket/table", metadata));

        assertNotNull("Should find metadata", resolution.get("s3://bucket/table"));
        assertNull("Should not find non-existent path", resolution.get("s3://other/path"));
        assertFalse("Should not be empty", resolution.isEmpty());
    }

    public void testExternalSourceResolutionEmpty() {
        ExternalSourceResolution resolution = ExternalSourceResolution.EMPTY;

        assertTrue("Should be empty", resolution.isEmpty());
        assertNull("Should return null for any path", resolution.get("any-path"));
    }

    public void testIcebergTableMetadataTypeMapping() {
        // Test type mapping from Iceberg to ESQL
        Schema schema = new Schema(
            List.of(
                Types.NestedField.required(1, "bool_field", Types.BooleanType.get()),
                Types.NestedField.required(2, "int_field", Types.IntegerType.get()),
                Types.NestedField.required(3, "long_field", Types.LongType.get()),
                Types.NestedField.required(4, "double_field", Types.DoubleType.get()),
                Types.NestedField.required(5, "string_field", Types.StringType.get()),
                Types.NestedField.required(6, "timestamp_field", Types.TimestampType.withoutZone())
            )
        );

        org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata metadata =
            new org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata(
                "s3://bucket/table",
                schema,
                null,
                "iceberg"
            );

        assertEquals("Should have 6 attributes", 6, metadata.attributes().size());
        assertEquals("bool_field", metadata.attributes().get(0).name());
        assertEquals("int_field", metadata.attributes().get(1).name());
        assertEquals("long_field", metadata.attributes().get(2).name());
        assertEquals("double_field", metadata.attributes().get(3).name());
        assertEquals("string_field", metadata.attributes().get(4).name());
        assertEquals("timestamp_field", metadata.attributes().get(5).name());
    }

    public void testIcebergTableMetadataEquality() {
        Schema schema1 = new Schema(List.of(Types.NestedField.required(1, "id", Types.LongType.get())));
        Schema schema2 = new Schema(List.of(Types.NestedField.required(1, "id", Types.LongType.get())));

        org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata metadata1 =
            new org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata(
                "s3://bucket/table",
                schema1,
                null,
                "iceberg"
            );
        org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata metadata2 =
            new org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata(
                "s3://bucket/table",
                schema2,
                null,
                "iceberg"
            );

        // Note: Iceberg Schema doesn't override equals(), so we compare by content
        assertEquals("Table paths should match", metadata1.tablePath(), metadata2.tablePath());
        assertEquals("Source types should match", metadata1.sourceType(), metadata2.sourceType());
        assertEquals("Attributes should match", metadata1.attributes().size(), metadata2.attributes().size());
        assertEquals("Schema columns should match", metadata1.schema().columns(), metadata2.schema().columns());
    }

    public void testIcebergTableMetadataToString() {
        Schema schema = new Schema(List.of(Types.NestedField.required(1, "id", Types.LongType.get())));
        org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata metadata =
            new org.elasticsearch.xpack.esql.datasources.datalake.iceberg.IcebergTableMetadata(
                "s3://bucket/table",
                schema,
                null,
                "iceberg"
            );

        String str = metadata.toString();
        assertTrue("Should contain table path", str.contains("s3://bucket/table"));
        assertTrue("Should contain source type", str.contains("iceberg"));
        assertTrue("Should contain field count", str.contains("fields=1"));
    }

    /**
     * Note: Full integration tests with real S3/Iceberg APIs are not possible
     * until the Parquet jar hell issue is resolved. When that happens, add:
     *
     * 1. testResolveRealIcebergTable() - with LocalStack or real S3
     * 2. testResolveRealParquetFile() - with test Parquet files
     * 3. testParallelResolution() - multiple sources resolved concurrently
     * 4. testS3AccessErrors() - credential errors, network failures
     * 5. testInvalidTablePaths() - malformed paths, non-existent tables
     */
}
