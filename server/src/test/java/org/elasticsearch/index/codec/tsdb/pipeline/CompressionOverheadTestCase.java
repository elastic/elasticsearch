/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodec;

import java.io.IOException;

public abstract class CompressionOverheadTestCase extends NumericPipelineTestCase {

    protected void assertMaxOneByteOverhead(final String dataType, final long[] original) throws IOException {
        final EncodeDecodeResult es87 = encodeDecodeWithES87(original.clone());
        final EncodeDecodeResult es94 = encodeDecodeWithES94(original.clone());

        final int overhead = es94.encodedSize - es87.encodedSize;
        final int originalSize = original.length * Long.BYTES;

        logger.info(
            "[{}] original={} bytes, ES87={} bytes, ES94={} bytes, overhead={} byte(s)",
            dataType,
            originalSize,
            es87.encodedSize,
            es94.encodedSize,
            overhead
        );

        assertArrayEquals("ES87 decode failed for " + dataType, original, es87.decoded);
        assertArrayEquals("ES94 decode failed for " + dataType, original, es94.decoded);
        assertTrue("ES94 overhead should be <= 1 byte for " + dataType + ", was " + overhead, overhead <= 1);
    }

    private EncodeDecodeResult encodeDecodeWithES87(final long[] values) throws IOException {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(BLOCK_SIZE);

        final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES + 256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        encoder.encode(values, out);
        final int encodedSize = out.getPosition();

        final long[] decoded = new long[BLOCK_SIZE];
        encoder.decode(new ByteArrayDataInput(buffer, 0, encodedSize), decoded);

        return new EncodeDecodeResult(decoded, encodedSize);
    }

    private EncodeDecodeResult encodeDecodeWithES94(final long[] values) throws IOException {
        try (NumericCodec codec = NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().gcd().bitPack().build()) {
            final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES + 256];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

            codec.newEncoder().encode(values, values.length, out);
            final int encodedSize = out.getPosition();

            final long[] decoded = new long[BLOCK_SIZE];
            codec.newDecoder().decode(decoded, new ByteArrayDataInput(buffer, 0, encodedSize));

            return new EncodeDecodeResult(decoded, encodedSize);
        }
    }

    protected record EncodeDecodeResult(long[] decoded, int encodedSize) {}
}
