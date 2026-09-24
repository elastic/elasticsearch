/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 * Verifies that counters passed via {@link FormatReadContext#readCounters()} are populated after a real
 * read drains a CSV file. Complements {@link CsvReaderCountersTests} (which exercises the counter
 * struct in isolation) by exercising the full FormatReader → batch-iterator wiring.
 */
public class CsvFormatReaderStatusSnapshotTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() throws Exception {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testCountersPopulatedAfterDrain() throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice
            2,Bob
            3,Carol
            """;
        StorageObject object = inMemoryCsv(csv);
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        CsvReaderCounters counters = (CsvReaderCounters) reader.newReadCounters();

        FormatReadContext context = FormatReadContext.builder().batchSize(10).readCounters(counters).build();
        try (CloseableIterator<Page> iterator = reader.read(object, context)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }

        var snapshot = counters.snapshot();
        assertEquals("3 data rows parsed (header excluded)", 3L, snapshot.rowsEmitted());
        assertEquals("no malformed rows in this fixture", 0L, snapshot.parseErrors());
        assertEquals("header row detected", true, snapshot.headerDetected());
    }

    public void testSiblingQueryReadersHaveIsolatedCounters() throws IOException {
        CsvFormatReader reader = new CsvFormatReader(blockFactory);
        CsvReaderCounters firstCounters = (CsvReaderCounters) reader.newReadCounters();
        CsvReaderCounters secondCounters = (CsvReaderCounters) reader.newReadCounters();

        drain(reader, firstCounters);

        assertTrue("the reader that ran must report its own work", firstCounters.snapshot().rowsEmitted() > 0);
        assertEquals("the sibling counters must not see it", 0L, secondCounters.snapshot().rowsEmitted());
    }

    public void testPerFileReadConfigCopyUsesTheSameCounters() throws IOException {
        CsvFormatReader query = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfigTrackingConsumedKeys(Map.of("delimiter", ","))
            .value();
        CsvFormatReader perFile = query.withReadConfig("0123456789abcdef0123456789abcdef");
        CsvReaderCounters counters = (CsvReaderCounters) query.newReadCounters();

        drain(perFile, counters);

        assertTrue(
            "withReadConfig runs per file; passing the same counters object threads work through to the caller",
            counters.snapshot().rowsEmitted() > 0
        );
    }

    private void drain(CsvFormatReader reader, CsvReaderCounters counters) throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice
            2,Bob
            3,Carol
            """;
        FormatReadContext context = FormatReadContext.builder().batchSize(10).readCounters(counters).build();
        try (CloseableIterator<Page> iterator = reader.read(inMemoryCsv(csv), context)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }
    }

    private static StorageObject inMemoryCsv(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(bytes);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException("Range reads not needed");
            }

            @Override
            public long length() {
                return bytes.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://snapshot-test.csv");
            }
        };
    }
}
