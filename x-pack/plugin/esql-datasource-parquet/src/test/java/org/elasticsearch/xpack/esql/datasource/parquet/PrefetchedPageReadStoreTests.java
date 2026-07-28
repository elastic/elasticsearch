/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PrefetchedPageReadStoreTests extends ESTestCase {

    private PlainCompressionCodecFactory codecFactory;
    private BlockFactory blockFactory;
    private BufferAllocator allocator;

    @Before
    public void initCodecAndAllocator() {
        codecFactory = new PlainCompressionCodecFactory();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        allocator = blockFactory.arrowAllocator();
    }

    @After
    public void releaseCodecFactory() {
        codecFactory.release();
    }

    public void testRoutesPageReaderByDescriptor() throws IOException {
        ColumnDescriptor a = newColumn("a");
        ColumnDescriptor b = newColumn("b");
        PrefetchedPageReader readerA = newPageReader(15);
        PrefetchedPageReader readerB = newPageReader(7);

        try (PrefetchedPageReadStore store = new PrefetchedPageReadStore(Map.of(a, readerA, b, readerB), 22)) {
            assertThat(store.getPageReader(a), sameInstance(readerA));
            assertThat(store.getPageReader(b), sameInstance(readerB));
            assertThat(store.getRowCount(), equalTo(22L));
        }
    }

    public void testReadDictionaryPageDelegatesToColumnReader() throws IOException {
        ColumnDescriptor desc = newColumn("dict");
        byte[] payload = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        DictionaryPage compressedDict = new DictionaryPage(BytesInput.from(payload), payload.length, 4, Encoding.PLAIN);
        PrefetchedPageReader reader = new PrefetchedPageReader(
            codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
            allocator,
            List.of(),
            compressedDict,
            0
        );
        try (PrefetchedPageReadStore store = new PrefetchedPageReadStore(Map.of(desc, reader), 0)) {
            DictionaryPage out = store.readDictionaryPage(desc);
            assertThat(out, notNullValue());
            assertThat(out.getBytes().toByteArray(), equalTo(payload));
        }
    }

    public void testReadDictionaryPageReturnsNullForUnknownColumn() {
        ColumnDescriptor known = newColumn("known");
        ColumnDescriptor unknown = newColumn("unknown");
        try (PrefetchedPageReadStore store = new PrefetchedPageReadStore(Map.of(known, newPageReader(0)), 0)) {
            assertThat(store.readDictionaryPage(unknown), nullValue());
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> store.getPageReader(unknown));
            assertThat(e.getMessage(), containsString("No prefetched reader for column"));
        }
    }

    public void testCloseIsIdempotent() {
        // close() now actively releases ArrowBufs owned by per-column readers; the second call
        // must be a safe no-op.
        PrefetchedPageReadStore store = new PrefetchedPageReadStore(Map.of(newColumn("a"), newPageReader(0)), 0);
        store.close();
        store.close();
    }

    private PrefetchedPageReader newPageReader(int valueCount) {
        DataPage page = new DataPageV1(
            BytesInput.from(new byte[] { 0, 1, 2, 3 }),
            valueCount,
            4,
            new IntStatistics(),
            Encoding.RLE,
            Encoding.RLE,
            Encoding.PLAIN
        );
        return new PrefetchedPageReader(
            codecFactory.getDecompressor(CompressionCodecName.UNCOMPRESSED),
            allocator,
            List.of(new PrefetchedPageReader.CompressedPage(page, -1L)),
            null,
            valueCount
        );
    }

    private static ColumnDescriptor newColumn(String name) {
        PrimitiveType type = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(name);
        return new ColumnDescriptor(new String[] { name }, type, 0, 0);
    }
}
