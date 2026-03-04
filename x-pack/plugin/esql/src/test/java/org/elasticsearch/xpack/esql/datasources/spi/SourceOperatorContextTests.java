/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

public class SourceOperatorContextTests extends ESTestCase {

    private static final Executor DIRECT = Runnable::run;

    public void testPartitionColumnNamesPopulated() {
        Set<String> partitionCols = Set.of("year", "month");
        SourceOperatorContext ctx = SourceOperatorContext.builder()
            .sourceType("file")
            .path(StoragePath.of("s3://bucket/file.parquet"))
            .projectedColumns(List.of("a", "b"))
            .attributes(List.of())
            .batchSize(1000)
            .maxBufferSize(10)
            .executor(DIRECT)
            .config(Map.of())
            .partitionColumnNames(partitionCols)
            .build();

        assertEquals(partitionCols, ctx.partitionColumnNames());
    }

    public void testPartitionColumnNamesDefaultsToEmpty() {
        SourceOperatorContext ctx = SourceOperatorContext.builder()
            .sourceType("file")
            .path(StoragePath.of("s3://bucket/file.parquet"))
            .projectedColumns(List.of("a"))
            .attributes(List.of())
            .batchSize(1000)
            .maxBufferSize(10)
            .executor(DIRECT)
            .config(Map.of())
            .build();

        assertNotNull(ctx.partitionColumnNames());
        assertTrue(ctx.partitionColumnNames().isEmpty());
    }

    public void testPartitionColumnNamesImmutable() {
        Set<String> partitionCols = Set.of("year");
        SourceOperatorContext ctx = SourceOperatorContext.builder()
            .sourceType("file")
            .path(StoragePath.of("s3://bucket/file.parquet"))
            .projectedColumns(List.of("a"))
            .attributes(List.of())
            .batchSize(1000)
            .maxBufferSize(10)
            .executor(DIRECT)
            .config(Map.of())
            .partitionColumnNames(partitionCols)
            .build();

        expectThrows(UnsupportedOperationException.class, () -> ctx.partitionColumnNames().add("extra"));
    }

    public void testBackwardCompatConstructorHasEmptyPartitionColumns() {
        SourceOperatorContext ctx = new SourceOperatorContext(
            "file",
            StoragePath.of("s3://bucket/file.parquet"),
            List.of("a"),
            List.of(),
            1000,
            10,
            DIRECT,
            Map.of()
        );

        assertNotNull(ctx.partitionColumnNames());
        assertTrue(ctx.partitionColumnNames().isEmpty());
    }
}
