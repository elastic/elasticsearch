/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.CountingBreaker;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that the CSV reader sizes a keyword column's byte storage up-front. The reader collects the
 * whole batch before building the page, so it knows each keyword column's exact byte size and passes it to
 * {@link BlockFactory#newBytesRefBlockBuilder(int, long)}; the backing {@code BytesRefArray} then allocates
 * its byte buffer once instead of regrowing as values are appended.
 * <p>
 * The test asserts this by comparing the byte-buffer reservations the reader makes on the {@link BigArrays}
 * circuit breaker against an explicitly byte-hinted build of the same values. Equal-length values are used so
 * the fixed-length offset elision in {@code BytesRefArray} avoids offset allocations and the breaker observes
 * only byte-buffer reservations.
 */
public class CsvFormatReaderByteHintTests extends ESTestCase {

    private static final int ROWS = 200;
    private static final int VALUE_BYTES = 20;

    public void testCsvKeywordColumnSizesByteStorageUpFront() throws IOException {
        List<BytesRef> values = new ArrayList<>(ROWS);
        StringBuilder csv = new StringBuilder("name:keyword\n");
        for (int i = 0; i < ROWS; i++) {
            StringBuilder value = new StringBuilder(VALUE_BYTES);
            for (int c = 0; c < VALUE_BYTES; c++) {
                value.append((char) ('a' + ((i + c) % 26)));
            }
            values.add(new BytesRef(value.toString().getBytes(StandardCharsets.US_ASCII)));
            csv.append(value).append('\n');
        }
        long columnBytes = (long) ROWS * VALUE_BYTES;

        int idealReservations = countDirectHintedBuildReservations(values, columnBytes);
        int readerReservations = countCsvReaderReservations(csv.toString(), values);

        assertThat(
            "CSV keyword emission must size the byte buffer up-front like a byte-hinted build (no regrow)",
            readerReservations,
            equalTo(idealReservations)
        );
    }

    /** Byte-buffer reservations when the buffer is sized correctly up-front via an explicit byte hint. */
    private int countDirectHintedBuildReservations(List<BytesRef> values, long byteHint) {
        CountingBreaker breaker = new CountingBreaker();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker.service());
        BlockFactory factory = BlockFactory.builder(bigArrays).breaker(new NoopCircuitBreaker("test-factory")).build();
        breaker.reset();
        try (BytesRefBlock.Builder builder = factory.newBytesRefBlockBuilder(values.size(), byteHint)) {
            for (BytesRef value : values) {
                builder.appendBytesRef(value);
            }
            try (BytesRefBlock block = builder.build()) {
                assertThat(block.getPositionCount(), equalTo(values.size()));
            }
        }
        assertThat(breaker.used(), equalTo(0L));
        return breaker.positiveReservations();
    }

    /** Byte-buffer reservations when the same column is emitted through the CSV reader. */
    private int countCsvReaderReservations(String csv, List<BytesRef> expected) throws IOException {
        CountingBreaker breaker = new CountingBreaker();
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breaker.service());
        BlockFactory factory = BlockFactory.builder(bigArrays).breaker(new NoopCircuitBreaker("test-factory")).build();
        CsvFormatReader reader = new CsvFormatReader(factory);
        breaker.reset();
        try (CloseableIterator<Page> iterator = reader.read(storageObject(csv), null, ROWS)) {
            assertTrue(iterator.hasNext());
            try (Page page = iterator.next()) {
                assertThat(page.getBlockCount(), equalTo(1));
                BytesRefBlock block = page.getBlock(0);
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < expected.size(); i++) {
                    assertThat(block.getBytesRef(i, scratch), equalTo(expected.get(i)));
                }
            }
        }
        assertThat(breaker.used(), equalTo(0L));
        return breaker.positiveReservations();
    }

    private static StorageObject storageObject(String csv) {
        byte[] bytes = csv.getBytes(StandardCharsets.UTF_8);
        return new StorageObject() {
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
                return StoragePath.of("memory://test.csv");
            }
        };
    }

}
