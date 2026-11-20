/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class TSDBOrdinalBlockCodecTests extends ESTestCase {

    public void testCreateReaderReturnsFreshInstances() {
        final int blockSize = randomBlockSize();
        final NumericReadContext ctx = new NumericReadContext(blockSize, null, 0);
        final TSDBOrdinalBlockCodec codec = new TSDBOrdinalBlockCodec();

        final OrdinalFieldReader first = codec.createReader(ctx);
        final OrdinalFieldReader second = codec.createReader(ctx);
        assertThat(second, not(sameInstance(first)));
    }

    public void testCreateWriterReturnsFreshInstances() {
        final int blockSize = randomBlockSize();
        final NumericWriteContext ctx = new NumericWriteContext(null, null, null, null, 0, blockSize, 0, null);
        final TSDBOrdinalBlockCodec codec = new TSDBOrdinalBlockCodec();

        final OrdinalFieldWriter first = codec.createWriter(ctx);
        final OrdinalFieldWriter second = codec.createWriter(ctx);
        assertThat(second, not(sameInstance(first)));
    }

    public void testWriterEncoderIsStable() {
        final int blockSize = randomBlockSize();
        final NumericWriteContext ctx = new NumericWriteContext(null, null, null, null, 0, blockSize, 0, null);
        final TSDBOrdinalBlockCodec codec = new TSDBOrdinalBlockCodec();
        final OrdinalFieldWriter writer = codec.createWriter(ctx);

        assertThat(writer.encoder(), sameInstance(writer.encoder()));
    }

    /**
     * Two writers created from the same codec encode identical ordinal blocks identically.
     */
    public void testTwoWritersProduceIdenticalOutputForSameInput() throws IOException {
        final int blockSize = randomBlockSize();
        final NumericWriteContext ctx = new NumericWriteContext(null, null, null, null, 0, blockSize, 0, null);
        final TSDBOrdinalBlockCodec codec = new TSDBOrdinalBlockCodec();

        final long[] ordinals = randomOrdinals(blockSize);
        final int bitsPerOrd = 4;

        final byte[] firstOutput = encodeOrdinals(codec.createWriter(ctx).encoder(), ordinals.clone(), bitsPerOrd);
        final byte[] secondOutput = encodeOrdinals(codec.createWriter(ctx).encoder(), ordinals.clone(), bitsPerOrd);

        assertThat(secondOutput, equalTo(firstOutput));
    }

    /**
     * Two readers created from the same codec decode identical encoded bytes identically.
     */
    public void testTwoReadersProduceIdenticalOutputForSameInput() throws IOException {
        final int blockSize = randomBlockSize();
        final NumericWriteContext writeCtx = new NumericWriteContext(null, null, null, null, 0, blockSize, 0, null);
        final NumericReadContext readCtx = new NumericReadContext(blockSize, null, 0);
        final TSDBOrdinalBlockCodec codec = new TSDBOrdinalBlockCodec();

        final long[] ordinals = randomOrdinals(blockSize);
        final int bitsPerOrd = 4;

        final byte[] encoded = encodeOrdinals(codec.createWriter(writeCtx).encoder(), ordinals.clone(), bitsPerOrd);

        final long[] decoded1 = decodeOrdinals(codec.createReader(readCtx).decoder(blockSize), encoded, blockSize, bitsPerOrd);
        final long[] decoded2 = decodeOrdinals(codec.createReader(readCtx).decoder(blockSize), encoded, blockSize, bitsPerOrd);

        assertThat(decoded1, equalTo(ordinals));
        assertThat(decoded2, equalTo(ordinals));
    }

    /**
     * Verifies that {@link OrdinalFieldWriter#writeFieldEntry} does not throw when maxOrd is 0
     * (a field with no values, hence no ordinals).
     */
    public void testMaxOrdinalGuard() throws IOException {
        final int blockSize = randomBlockSize();
        final ByteBuffersDataOutput metaOut = new ByteBuffersDataOutput();
        final ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (
            IndexOutput meta = new ByteBuffersIndexOutput(metaOut, "test-meta", "test-meta");
            IndexOutput data = new ByteBuffersIndexOutput(dataOut, "test-data", "test-data")
        ) {
            final NumericWriteContext ctx = new NumericWriteContext(meta, data, null, null, 0, blockSize, 0, null);
            final TSDBOrdinalBlockCodec codec = new TSDBOrdinalBlockCodec();
            final OrdinalFieldWriter writer = codec.createWriter(ctx);

            final TsdbDocValuesProducer valuesSource = new TsdbDocValuesProducer(new DocValuesConsumerUtil.MergeStats(true, 0, 0, 0, 0));
            final FieldInfo field = new FieldInfo(
                "test_field",
                0,
                false,
                false,
                false,
                IndexOptions.NONE,
                DocValuesType.SORTED,
                DocValuesSkipIndexType.NONE,
                -1,
                Map.of(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );

            try {
                writer.writeFieldEntry(field, valuesSource, 0, null, null);
            } catch (IllegalArgumentException e) {
                fail("maxValue should be guarded in writeFieldEntry [" + e.getMessage() + "]");
            }
        }
    }

    private static byte[] encodeOrdinals(final OrdinalFieldWriter.Encoder encoder, final long[] ordinals, final int bitsPerOrd)
        throws IOException {
        final ByteBuffersDataOutput buf = new ByteBuffersDataOutput();
        try (IndexOutput out = new ByteBuffersIndexOutput(buf, "test", "test")) {
            encoder.encodeOrdinals(ordinals, out, bitsPerOrd);
        }
        return buf.toArrayCopy();
    }

    private static long[] decodeOrdinals(
        final OrdinalFieldReader.Decoder decoder,
        final byte[] encoded,
        final int blockSize,
        final int bitsPerOrd
    ) throws IOException {
        final long[] out = new long[blockSize];
        decoder.decodeOrdinals(new org.apache.lucene.store.ByteArrayDataInput(encoded), out, bitsPerOrd);
        return out;
    }

    private static long[] randomOrdinals(final int blockSize) {
        final long[] ordinals = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            ordinals[i] = randomLongBetween(0, 15);
        }
        return ordinals;
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(7, 9);
    }
}
