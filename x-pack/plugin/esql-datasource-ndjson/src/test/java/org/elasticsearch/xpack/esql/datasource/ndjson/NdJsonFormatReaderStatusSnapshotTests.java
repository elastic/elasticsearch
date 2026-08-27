/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

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

/**
 * Verifies that {@link NdJsonFormatReader#statusSnapshot()} reports populated counters after a real
 * read drains an NDJSON file. Complements {@link NdJsonReaderCountersTests} (which exercises the
 * counter struct in isolation) by exercising the full FormatReader → iterator → decoder wiring.
 */
public class NdJsonFormatReaderStatusSnapshotTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Before
    public void initBlockFactory() {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testCountersPopulatedAfterDrain() throws IOException {
        String ndjson = """
            {"a": 1, "b": "x"}
            {"a": 2, "b": "y"}
            {"a": 3, "b": "z"}
            """;
        var object = new BytesStorageObject("memory://snapshot-test.ndjson", ndjson.getBytes(StandardCharsets.UTF_8));
        var reader = new NdJsonFormatReader(null, blockFactory);

        // Snapshot before drain: counters should be at zero, format identifier present.
        var before = reader.statusSnapshot();
        assertEquals("ndjson", before.format());
        assertEquals(0L, before.parseErrors());
        assertEquals(0L, before.readNanos());

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("a", "b"), 10)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                Releasables.close(page::releaseBlocks);
            }
        }

        var after = reader.statusSnapshot();
        assertEquals("ndjson", after.format());
        assertEquals("no malformed lines in this fixture", 0L, after.parseErrors());
        assertTrue("read_nanos should be > 0 after at least one decodePage call", after.readNanos() > 0);
    }

    /**
     * Verifies that {@link NdJsonFormatReader} accumulates {@code asyncCpuNanos()} from the
     * {@link StorageObject} into its own CPU counter via {@code withAsyncCpuOnClose}.
     */
    public void testAsyncCpuNanosAccumulated() throws IOException {
        long injectedAsyncCpu = 5_000_000L; // 5 ms injected from a "GCS-like" storage object
        String ndjson = """
            {"id": 1}
            """;
        byte[] bytes = ndjson.getBytes(StandardCharsets.UTF_8);
        StorageObject object = new StorageObject() {
            @Override
            public long asyncCpuNanos() {
                return injectedAsyncCpu;
            }

            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(bytes);
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException("range reads not needed");
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
                return StoragePath.of("memory://async-cpu-test.ndjson");
            }
        };
        var reader = new NdJsonFormatReader(null, blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(object, List.of("id"), 10)) {
            while (iterator.hasNext()) {
                Releasables.close(iterator.next()::releaseBlocks);
            }
        }

        assertTrue(
            "readCpuNanos must include asyncCpuNanos from the storage object",
            reader.statusSnapshot().readCpuNanos() >= injectedAsyncCpu
        );
    }
}
