/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.numeric.stages;

import org.apache.lucene.util.MathUtil;
import org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext;
import org.elasticsearch.index.codec.tsdb.pipeline.StageId;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecStage;

import java.io.IOException;

/**
 * GCD factoring transform stage.
 *
 * <h2>Effectiveness</h2>
 * <p>Applied when all values share a common divisor greater than 1. Divides all values
 * by the GCD, reducing their magnitude and therefore the number of bits required for
 * bit-packing. Skipped when the GCD is 0 or 1 (unsigned comparison).
 *
 * <h2>Example</h2>
 * <p>Values {@code [100, 200, 300, 400]} with GCD={@code 100}
 * produces {@code [1, 2, 3, 4]}.
 *
 * <h2>Metadata layout</h2>
 * <p>Written to the stage metadata section (see {@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat}):
 * <pre>
 *   +---------------------+
 *   | VLong(gcd - 2)      |  1-9 bytes, unsigned variable-length
 *   +---------------------+
 * </pre>
 * <p>The GCD is always {@code >= 2} when the stage applies, so subtracting 2
 * before encoding as unsigned VLong saves one byte for small divisors
 * (e.g., GCD=2 stores as 0, which is a single byte).
 *
 * <h2>Power-of-two optimization</h2>
 * <p>When the GCD is a power of two, division and multiplication are replaced by
 * arithmetic shifts. The JIT cannot optimize division by a runtime variable (it always
 * emits {@code idiv}, ~20-90 cycles), while shifts are single-cycle and SIMD-friendly.
 */
public final class GcdCodecStage implements NumericCodecStage {

    /** Singleton instance. */
    public static final GcdCodecStage INSTANCE = new GcdCodecStage();

    private GcdCodecStage() {}

    @Override
    public byte id() {
        return StageId.GCD_STAGE.id;
    }

    @Override
    public void encode(final long[] values, final int valueCount, final EncodingContext context) {
        assert valueCount >= 1 : "valueCount must be at least 1";

        long gcd = values[0];
        for (int i = 1; i < valueCount; i++) {
            gcd = MathUtil.gcd(gcd, values[i]);
        }

        if (Long.compareUnsigned(gcd, 1) <= 0) {
            return;
        }

        divideByGcd(values, valueCount, gcd);

        context.metadata().writeVLong(gcd - 2);
    }

    @Override
    public void decode(final long[] values, final int valueCount, final DecodingContext context) throws IOException {
        assert valueCount >= 1 : "valueCount must be at least 1";
        final long gcd = context.metadata().readVLong() + 2;
        multiplyByGcd(values, valueCount, gcd);
    }

    private static void divideByGcd(final long[] values, final int valueCount, final long gcd) {
        if ((gcd & (gcd - 1)) == 0) {
            final int shift = Long.numberOfTrailingZeros(gcd);
            for (int i = 0; i < valueCount; i++) {
                values[i] >>= shift;
            }
        } else {
            for (int i = 0; i < valueCount; i++) {
                values[i] /= gcd;
            }
        }
    }

    private static void multiplyByGcd(final long[] values, final int valueCount, final long gcd) {
        if ((gcd & (gcd - 1)) == 0) {
            final int shift = Long.numberOfTrailingZeros(gcd);
            for (int i = 0; i < valueCount; i++) {
                values[i] <<= shift;
            }
        } else {
            for (int i = 0; i < valueCount; i++) {
                values[i] *= gcd;
            }
        }
    }
}
