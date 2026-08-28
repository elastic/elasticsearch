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
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Verifies that {@link CsvFormatReader#statusSnapshot()} reports populated counters after a real
 * read drains a CSV file. Complements {@link CsvReaderCountersTests} (which exercises the counter
 * struct in isolation) by exercising the full FormatReader → batch-iterator wiring.
 * <p>
 * The last two pin the COUNTER LIFETIME in both directions, mirroring the NDJSON suite. CSV has the correct
 * shape today — only {@link CsvFormatReader#withReadConfig} shares its parent's counters — and these exist to
 * keep it: the chain's root is the node-lifetime reader the format registry hands out, so a wither that shares
 * above the per-query seam mixes concurrent queries' telemetry, while a fork at the per-file seam leaves the
 * reader that reads reporting into a copy nobody snapshots. Neither is observable from a single drained reader,
 * which is why both assert on a SECOND live copy.
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

        // Snapshot before drain: counters should be at zero, header_detected false.
        var before = reader.statusSnapshot();
        assertEquals(0L, before.rowsEmitted());
        assertEquals(0L, before.parseErrors());
        assertEquals(false, before.headerDetected());
        assertEquals(0L, before.readNanos());

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id", "name"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }

        var after = reader.statusSnapshot();
        assertEquals("3 data rows parsed (header excluded)", 3L, after.rowsEmitted());
        assertEquals("no malformed rows in this fixture", 0L, after.parseErrors());
        assertEquals("header row detected", true, after.headerDetected());
        assertTrue("read_nanos should be > 0 after at least one batch", after.readNanos() > 0);
    }

    public void testSiblingQueryReadersDoNotShareCounters() throws IOException {
        CsvFormatReader base = new CsvFormatReader(blockFactory);
        CsvFormatReader first = (CsvFormatReader) base.withConfigTrackingConsumedKeys(Map.of("delimiter", ",")).value();
        CsvFormatReader second = (CsvFormatReader) base.withConfigTrackingConsumedKeys(Map.of("delimiter", ",")).value();

        drain(first);

        assertTrue("the reader that read must report its own work", first.statusSnapshot().rowsEmitted() > 0);
        assertEquals("a sibling query's reader must not see it", 0L, second.statusSnapshot().rowsEmitted());
        assertEquals("nor may it reach the registry's shared reader", 0L, base.statusSnapshot().rowsEmitted());
    }

    public void testPerFileReadConfigCopyReportsThroughItsParent() throws IOException {
        CsvFormatReader query = (CsvFormatReader) new CsvFormatReader(blockFactory).withConfigTrackingConsumedKeys(Map.of("delimiter", ","))
            .value();
        CsvFormatReader perFile = query.withReadConfig("0123456789abcdef0123456789abcdef");

        drain(perFile);

        assertTrue(
            "withReadConfig runs per file, below the reader the status envelope snapshots, so its work must land"
                + " in the parent — a fork here is the zero-read-time defect this pins against",
            query.statusSnapshot().rowsEmitted() > 0
        );
    }

    private void drain(CsvFormatReader reader) throws IOException {
        String csv = """
            id:long,name:keyword
            1,Alice
            2,Bob
            3,Carol
            """;
        try (CloseableIterator<Page> iterator = reader.read(inMemoryCsv(csv), List.of("id", "name"), 10)) {
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
