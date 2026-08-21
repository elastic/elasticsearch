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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.elasticsearch.columnar.ColumnarTestUtils.readNumericMeta;
import static org.elasticsearch.columnar.ColumnarTestUtils.singleValuedCursor;

/**
 * Verifies that {@link NumericPipelineSelector} is called per field and that the pipeline it
 * returns is reflected in the on-disk column metadata. Tests write a column, read the metadata
 * back, and assert the frozen transform ids match the expected pipeline.
 */
public class NumericPipelineSelectorTests extends ESTestCase {

    private static final byte[] DEFAULT_TRANSFORM_IDS = { DeltaTransform.ID, OffsetTransform.ID, GcdTransform.ID };
    private static final byte[] SPLIT_DELTA_TRANSFORM_IDS = {
        SplitDeltaTransform.ID,
        DeltaTransform.ID,
        OffsetTransform.ID,
        GcdTransform.ID };
    private static final byte[] ALP_GAUGE_TRANSFORM_IDS = { AlpDoubleTransform.ID, DeltaTransform.ID, OffsetTransform.ID, GcdTransform.ID };
    private static final byte[] ALP_COUNTER_TRANSFORM_IDS = {
        AlpDoubleTransform.ID,
        SplitDeltaTransform.ID,
        DeltaTransform.ID,
        OffsetTransform.ID,
        GcdTransform.ID };

    public void testSelectorIsInvokedPerField() throws IOException {
        final NumericPipelineSelector selector = (fieldName, type) -> {
            assertEquals("my_field", fieldName);
            return NumericPipeline::defaultPipeline;
        };
        writeAndReadMetadata("my_field", selector, longValues());
    }

    public void testDefaultPipelineTransformIds() throws IOException {
        assertTransformIds((f, t) -> NumericPipeline::defaultPipeline, longValues(), DEFAULT_TRANSFORM_IDS);
    }

    public void testSplitDeltaPipelineTransformIds() throws IOException {
        assertTransformIds((f, t) -> NumericPipeline::monotonicLongPipeline, monotonicLongs(), SPLIT_DELTA_TRANSFORM_IDS);
    }

    public void testAlpGaugePipelineTransformIds() throws IOException {
        assertTransformIds((f, t) -> NumericPipeline::doubleGaugePipeline, doubleBits(), ALP_GAUGE_TRANSFORM_IDS);
    }

    public void testAlpCounterPipelineTransformIds() throws IOException {
        assertTransformIds((f, t) -> NumericPipeline::doubleCounterPipeline, doubleBits(), ALP_COUNTER_TRANSFORM_IDS);
    }

    public void testConsumerPassesBlockSizeToTemplate() throws IOException {
        final int blockSize = randomValidBlockSize();
        final int[] capturedBlockSize = new int[1];

        final NumericPipelineSelector capturingSelector = (fieldName, type) -> bs -> {
            capturedBlockSize[0] = bs;
            return NumericPipeline.defaultPipeline(bs);
        };

        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(capturingSelector, blockSize);

        final FieldType fieldType = columnarBinaryFieldType(ColumnarFieldType.LONG);
        final BytesRefBuilder builder = new BytesRefBuilder();
        try (
            Directory dir = newDirectory();
            IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec(format)))
        ) {
            final Document doc = new Document();
            doc.add(new Field("value", BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { 42L }, 1, builder)), fieldType));
            writer.addDocument(doc);
            writer.commit();
        }

        assertEquals(blockSize, capturedBlockSize[0]);
    }

    public void testSelectorCanVaryPipelineByFieldName() throws IOException {
        final NumericPipelineSelector selector = (fieldName, type) -> fieldName.equals("alp_field")
            ? NumericPipeline::doubleGaugePipeline
            : NumericPipeline::defaultPipeline;

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
                ColumnarCodecUtil.writeHeader(out, "ColumNARData", FormatVersion.CURRENT, segmentId, "");
                final int blockSize = randomValidBlockSize();
                final NumericPipeline pipeline = selector.select(fieldName, ColumnarFieldType.LONG).build(blockSize);
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
                ColumnarCodecUtil.writeHeader(meta, "ColumNARMeta", FormatVersion.CURRENT, segmentId, "");
                written.writeTo(meta);
                ColumnarCodecUtil.writeFooter(meta);
            }
            final NumericColumnMetadata read = readNumericMeta(dir, "num.cnm", segmentId, values.length);
            try (IndexInput data = dir.openInput("num.cnd", IOContext.DEFAULT)) {
                CodecUtil.checksumEntireFile(data);
                ColumnarCodecUtil.checkHeader(data, "ColumNARData", segmentId, "");
                final NumericColumnReader reader = new NumericColumnReader(read, data);
                final ColumnIterator iterator = reader.iterator();
                int idx = 0;
                for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                    assertEquals(values[idx++], reader.valueForOrdinal(reader.firstOrdinal(iterator.rank())));
                }
            }
            return read;
        }
    }

}
