/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import static org.hamcrest.Matchers.lessThan;

public class Utf8AscTopNEncoderTests extends AbstractUtf8TopNEncoderTests {
    public Utf8AscTopNEncoderTests(TestCase<?> testCase) {
        super(testCase);
    }

    @Override
    protected TopNEncoder encoder() {
        return TopNEncoder.UTF8;
    }

    @Override
    protected void assertMinMax(BreakingBytesRefBuilder min, BreakingBytesRefBuilder max) {
        assertThat(min.bytesRefView(), lessThan(max.bytesRefView()));
    }

    /**
     * Bytes 0xF8–0xFF are outside the original 248-entry lead-byte table.
     * The decoder must throw a controlled IllegalArgumentException rather than
     * an ArrayIndexOutOfBoundsException (defense-in-depth; no current reachable path).
     */
    public void testDecodeHighLeadBytesThrowsIllegalArgument() {
        for (int b = 0xF8; b <= 0xFF; b++) {
            // Construct an encoded stream: [highByte, NUL-terminator]
            byte[] encoded = { (byte) b, Utf8AscTopNEncoder.TERMINATOR };
            BytesRef ref = new BytesRef(encoded, 0, encoded.length);
            int finalB = b;
            assertThrows(
                "expected IllegalArgumentException for byte 0x" + Integer.toHexString(finalB),
                IllegalArgumentException.class,
                () -> TopNEncoder.UTF8.decodeBytesRef(ref, new BytesRef())
            );
        }
    }
}
