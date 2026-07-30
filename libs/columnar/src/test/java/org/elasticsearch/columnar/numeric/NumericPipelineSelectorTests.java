/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Verifies that {@link NumericPipelineSelector} is called per field and that the pipeline it
 * returns is reflected in the on-disk column metadata. Tests write a column, read the metadata
 * back, and assert the frozen transform ids match the expected pipeline.
 */
public class NumericPipelineSelectorTests extends ESTestCase {

    // NOTE: expected transform-id arrays are derived from the frozen IDs in each stage class.
    // DeltaTransform.ID=0, OffsetTransform.ID=1, GcdTransform.ID=2,
    // SplitDeltaTransform.ID=3, AlpDoubleTransform.ID=4.
    private static final byte[] DEFAULT_TRANSFORM_IDS = { 0, 1, 2 };
    private static final byte[] SPLIT_DELTA_TRANSFORM_IDS = { 3, 0, 1, 2 };
    private static final byte[] ALP_GAUGE_TRANSFORM_IDS = { 4, 0, 1, 2 };
    private static final byte[] ALP_COUNTER_TRANSFORM_IDS = { 4, 3, 0, 1, 2 };

    public void testSelectorIsInvokedPerField() throws IOException {
        final AtomicReference<String> capturedName = new AtomicReference<>();
        final NumericPipelineSelector selector = (fieldName, blockSize) -> {
            capturedName.set(fieldName);
            return NumericPipeline.defaultPipeline(blockSize);
        };
        writeAndReadMetadata("my_field", selector, longValues());
        assertEquals("my_field", capturedName.get());
    }

    public void testDefaultPipelineTransformIds() throws IOException {
        assertTransformIds((f, bs) -> NumericPipeline.defaultPipeline(bs), longValues(), DEFAULT_TRANSFORM_IDS);
    }

    public void testSplitDeltaPipelineTransformIds() throws IOException {
        assertTransformIds((f, bs) -> NumericPipeline.monotonicLongPipeline(bs), monotonicLongs(), SPLIT_DELTA_TRANSFORM_IDS);
    }

    public void testAlpGaugePipelineTransformIds() throws IOException {
        assertTransformIds((f, bs) -> NumericPipeline.doubleGaugePipeline(bs), doubleBits(), ALP_GAUGE_TRANSFORM_IDS);
    }

    public void testAlpCounterPipelineTransformIds() throws IOException {
        assertTransformIds((f, bs) -> NumericPipeline.doubleCounterPipeline(bs), doubleBits(), ALP_COUNTER_TRANSFORM_IDS);
    }

    public void testSelectorCanVaryPipelineByFieldName() throws IOException {
        final NumericPipelineSelector selector = (fieldName, blockSize) -> fieldName.equals("alp_field")
            ? NumericPipeline.doubleGaugePipeline(blockSize)
            : NumericPipeline.defaultPipeline(blockSize);

        assertArrayEquals(ALP_GAUGE_TRANSFORM_IDS, writeAndReadMetadata("alp_field", selector, doubleBits()).transformIds());
        assertArrayEquals(DEFAULT_TRANSFORM_IDS, writeAndReadMetadata("other_field", selector, longValues()).transformIds());
    }

    private void assertTransformIds(NumericPipelineSelector selector, long[] values, byte[] expectedIds) throws IOException {
        assertArrayEquals(expectedIds, writeAndReadMetadata("f", selector, values).transformIds());
    }

    private static long[] longValues() {
        final int n = 200;
        final long[] values = new long[n];
        for (int i = 0; i < n; i++) {
            values[i] = randomLong();
        }
        return values;
    }

    private static long[] monotonicLongs() {
        final int n = 200;
        final long[] values = new long[n];
        long ts = 1_700_000_000_000L;
        for (int i = 0; i < n; i++) {
            ts += randomIntBetween(1, 1000);
            values[i] = ts;
        }
        return values;
    }

    private static long[] doubleBits() {
        // NOTE: ALP requires doubles that look like real floating-point data; plain
        // random longs interpreted as double bits may all be NaN or subnormal and cause
        // ALP to fall back to raw encoding. Use a small set of realistic gauge values instead.
        final int n = 200;
        final long[] values = new long[n];
        for (int i = 0; i < n; i++) {
            values[i] = Double.doubleToLongBits(1.0 + i * 0.01 + randomDouble() * 0.001);
        }
        return values;
    }

    private NumericColumnMetadata writeAndReadMetadata(final String fieldName, final NumericPipelineSelector selector, final long[] values)
        throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);

        try (Directory dir = newDirectory()) {
            final NumericColumnMetadata written;
            try (IndexOutput out = dir.createOutput("num.cnd", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(out, "ColumnarNumericData", segmentId, "");
                final NumericPipeline pipeline = selector.select(fieldName, NumericColumnWriter.BLOCK_SIZE);
                written = NumericColumnWriter.write(
                    values.length,
                    values.length,
                    values.length,
                    () -> singleValuedCursor(values),
                    pipeline,
                    BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                    SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID),
                    dir,
                    IOContext.DEFAULT,
                    out
                );
                ColumnarCodecUtil.writeFooter(out);
            }
            try (IndexOutput meta = dir.createOutput("num.cnm", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(meta, "ColumnarNumericMeta", segmentId, "");
                written.writeTo(meta);
                ColumnarCodecUtil.writeFooter(meta);
            }
            try (ChecksumIndexInput meta = dir.openChecksumInput("num.cnm")) {
                ColumnarCodecUtil.checkHeader(meta, "ColumnarNumericMeta", segmentId, "");
                final NumericColumnMetadata read = NumericColumnMetadata.readFrom(meta, values.length);
                ColumnarCodecUtil.checkFooter(meta);
                try (IndexInput data = dir.openInput("num.cnd", IOContext.DEFAULT)) {
                    CodecUtil.checksumEntireFile(data);
                    ColumnarCodecUtil.checkHeader(data, "ColumnarNumericData", segmentId, "");
                    final NumericColumnReader reader = new NumericColumnReader(read, data);
                    final ColumnIterator iterator = reader.iterator();
                    int idx = 0;
                    for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        assertEquals(values[idx++], reader.valueForOrdinal(reader.firstOrdinal(iterator.index())));
                    }
                }
                return read;
            }
        }
    }

    private static NumericColumnValues singleValuedCursor(final long[] values) {
        return new NumericColumnValues() {
            private int doc = -1;

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public long nextValue() {
                return values[doc];
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return ++doc < values.length ? doc : (doc = DocIdSetIterator.NO_MORE_DOCS);
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return values.length;
            }
        };
    }
}
