/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

public abstract class NumericPipelineTestCase extends ESTestCase {

    protected static final Logger logger = LogManager.getLogger(NumericPipelineTestCase.class);

    protected static final int BLOCK_SIZE = ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;

    // NOTE: BitPackCodecStage requires blockSize >= 128 (multiple of ForUtil.BLOCK_SIZE).
    protected static int randomBlockSize() {
        return 1 << randomIntBetween(7, 10);
    }

    protected void assertRoundTrip(final NumericEncoder encoder, final long[] original, final String dataType) throws IOException {
        final EncodeResult result = encodeAndDecode(encoder, original.clone());
        assertArrayEquals(original, result.decoded);
        logCompressionStats(dataType, original.length, result.encodedSize);
    }

    protected void assertRoundTripWithAllDataTypes(final NumericEncoder encoder) throws IOException {
        for (final var ds : NumericDataGenerators.longDataSources()) {
            assertRoundTrip(encoder, ds.generator().apply(encoder.blockSize()), ds.name());
        }
    }

    private void logCompressionStats(final String dataType, int valueCount, int encodedSize) {
        final int originalSize = valueCount * Long.BYTES;
        final double ratio = (double) originalSize / encodedSize;
        final double savings = (1.0 - (double) encodedSize / originalSize) * 100;
        logger.info(
            "[{}] original={} bytes, encoded={} bytes, ratio={}, savings={} %",
            dataType,
            originalSize,
            encodedSize,
            String.format(Locale.ROOT, "%.2fx", ratio),
            String.format(Locale.ROOT, "%.1f", savings)
        );
    }

    protected void assertMultipleBlocks(final NumericEncoder encoder) throws IOException {
        final int bs = encoder.blockSize();
        final NumericBlockEncoder blockEncoder = encoder.newBlockEncoder();
        final NumericDecoder decoder = NumericDecoder.fromDescriptor(encoder.descriptor());
        final NumericBlockDecoder blockDecoder = decoder.newBlockDecoder();

        for (int block = 0; block < randomIntBetween(5, 10); block++) {
            final long[] original = randomFuzzLongs(bs);
            final byte[] buffer = new byte[bs * Long.BYTES * 2 + 4096];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

            blockEncoder.encode(original.clone(), original.length, out);

            final long[] decoded = new long[bs];
            blockDecoder.decode(decoded, new ByteArrayDataInput(buffer, 0, out.getPosition()));

            assertArrayEquals("Block " + block + " failed", original, decoded);
        }
    }

    protected EncodeResult encodeAndDecode(final NumericEncoder encoder, final long[] values) throws IOException {
        final int bs = encoder.blockSize();
        final byte[] buffer = new byte[bs * Long.BYTES * 2 + 4096];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        encoder.newBlockEncoder().encode(values, values.length, out);
        final int encodedSize = out.getPosition();

        final NumericDecoder decoder = NumericDecoder.fromDescriptor(encoder.descriptor());
        final long[] decoded = new long[bs];
        decoder.newBlockDecoder().decode(decoded, new ByteArrayDataInput(buffer, 0, encodedSize));

        return new EncodeResult(decoded, encodedSize);
    }

    protected record EncodeResult(long[] decoded, int encodedSize) {}

    protected void assertRoundTripWithDoubleDataTypes(final NumericEncoder encoder) throws IOException {
        for (final var ds : NumericDataGenerators.doubleDataSources()) {
            final double[] doubles = ds.generator().apply(encoder.blockSize());
            final long[] sortable = new long[doubles.length];
            for (int i = 0; i < doubles.length; i++) {
                sortable[i] = NumericUtils.doubleToSortableLong(doubles[i]);
            }
            assertRoundTrip(encoder, sortable, ds.name());
        }
    }

    protected void assertRoundTripWithFloatDataTypes(final NumericEncoder encoder) throws IOException {
        for (final var ds : NumericDataGenerators.doubleDataSources()) {
            final double[] doubles = ds.generator().apply(encoder.blockSize());
            final long[] sortable = new long[doubles.length];
            for (int i = 0; i < doubles.length; i++) {
                sortable[i] = NumericUtils.floatToSortableInt((float) doubles[i]);
            }
            assertRoundTrip(encoder, sortable, ds.name());
        }
    }

    protected long[] randomFuzzLongs(int size) {
        final long[] data = new long[size];
        for (int i = 0; i < size; i++) {
            data[i] = randomLongBetween(0, 1_000_000L);
        }
        return data;
    }
}
