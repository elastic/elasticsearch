/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Semi-integration tests for limit pushdown through the external source operator pipeline.
 * Uses real FormatReader implementations with in-memory storage to verify that row limits
 * reduce the number of files opened and rows read.
 */
public class ExternalSourceLimitTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    public void testMultiFileLimitOpensFewerFiles() throws Exception {
        AtomicInteger filesRead = new AtomicInteger(0);
        int rowsPerFile = 100;
        int fileCount = 5;

        List<StorageEntry> entries = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            entries.add(new StorageEntry(StoragePath.of("s3://bucket/data/f" + i + ".csv"), 100, Instant.EPOCH));
        }
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.csv");

        FormatReader formatReader = new RowGeneratingFormatReader(filesRead, rowsPerFile);
        StorageProvider storageProvider = new StubStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data/f0.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        // With rowLimit=50, should stop after 1 file (100 rows >= 50)
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).rowLimit(50).fileList(fileList).build();

        try (SourceOperator operator = factory.get(driverContext)) {
            List<Page> pages = drainOperator(operator);

            int totalRows = 0;
            for (Page p : pages) {
                totalRows += p.getPositionCount();
                p.releaseBlocks();
            }

            // Should have read only 1 file; LimitingIterator trims to exactly 50 rows
            assertEquals(1, filesRead.get());
            assertEquals(50, totalRows);
        }
    }

    public void testMultiFileNoLimitReadsAllFiles() throws Exception {
        AtomicInteger filesRead = new AtomicInteger(0);
        int rowsPerFile = 10;
        int fileCount = 3;

        List<StorageEntry> entries = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            entries.add(new StorageEntry(StoragePath.of("s3://bucket/data/f" + i + ".csv"), 100, Instant.EPOCH));
        }
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.csv");

        FormatReader formatReader = new RowGeneratingFormatReader(filesRead, rowsPerFile);
        StorageProvider storageProvider = new StubStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data/f0.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        // NO_LIMIT should read all files
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).build();

        try (SourceOperator operator = factory.get(driverContext)) {
            List<Page> pages = drainOperator(operator);

            int totalRows = 0;
            for (Page p : pages) {
                totalRows += p.getPositionCount();
                p.releaseBlocks();
            }

            assertEquals(fileCount, filesRead.get());
            assertEquals(fileCount * rowsPerFile, totalRows);
        }
    }

    public void testSingleFileLimitStopsEarly() throws Exception {
        AtomicInteger filesRead = new AtomicInteger(0);
        int rowsPerFile = 1000;

        FormatReader formatReader = new RowGeneratingFormatReader(filesRead, rowsPerFile);
        StorageProvider storageProvider = new StubStorageProvider();

        StoragePath path = StoragePath.of("s3://bucket/data/big.csv");
        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            )
        );

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        // Single file with rowLimit=10 — the LimitingIterator should stop early
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            path,
            attributes,
            50, // batchSize
            10,
            (Runnable r) -> r.run()
        ).rowLimit(10).build();

        try (SourceOperator operator = factory.get(driverContext)) {
            List<Page> pages = drainOperator(operator);

            int totalRows = 0;
            for (Page p : pages) {
                totalRows += p.getPositionCount();
                p.releaseBlocks();
            }

            // LimitingIterator trims the last page to exactly the budget
            assertEquals(10, totalRows);
        }
    }

    private static List<Page> drainOperator(SourceOperator operator) {
        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }
        return pages;
    }

    private static class RowGeneratingFormatReader implements FormatReader {
        private final AtomicInteger filesRead;
        private final int rowsPerFile;

        RowGeneratingFormatReader(AtomicInteger filesRead, int rowsPerFile) {
            this.filesRead = filesRead;
            this.rowsPerFile = rowsPerFile;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            filesRead.incrementAndGet();
            int limit = context.rowLimit();
            int totalRows = limit == FormatReader.NO_LIMIT ? rowsPerFile : Math.min(rowsPerFile, limit);
            return new RowIterator(totalRows, context.batchSize());
        }

        @Override
        public String formatName() {
            return "test";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".csv");
        }

        @Override
        public void close() {}
    }

    private static class RowIterator implements CloseableIterator<Page> {
        private int remaining;
        private final int batchSize;

        RowIterator(int totalRows, int batchSize) {
            this.remaining = totalRows;
            this.batchSize = batchSize;
        }

        @Override
        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
        public Page next() {
            if (remaining <= 0) {
                throw new NoSuchElementException();
            }
            int count = Math.min(batchSize, remaining);
            remaining -= count;
            try (IntBlock.Builder builder = TEST_BLOCK_FACTORY.newIntBlockBuilder(count)) {
                for (int i = 0; i < count; i++) {
                    builder.appendInt(i);
                }
                return new Page(builder.build());
            }
        }

        @Override
        public void close() {}
    }

    private static class StubStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return stubObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return stubObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return stubObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public StorageEntry next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public boolean exists(StoragePath path) {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}

        private static StorageObject stubObject(StoragePath path) {
            return new StorageObject() {
                @Override
                public InputStream newStream() throws IOException {
                    return new ByteArrayInputStream(new byte[0]);
                }

                @Override
                public InputStream newStream(long position, long length) throws IOException {
                    return new ByteArrayInputStream(new byte[0]);
                }

                @Override
                public long length() {
                    return 0;
                }

                @Override
                public Instant lastModified() {
                    return Instant.EPOCH;
                }

                @Override
                public boolean exists() {
                    return true;
                }

                @Override
                public StoragePath path() {
                    return path;
                }
            };
        }
    }
}
