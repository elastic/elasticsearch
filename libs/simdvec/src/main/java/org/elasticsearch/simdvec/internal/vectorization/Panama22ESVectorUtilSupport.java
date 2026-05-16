/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import static jdk.incubator.vector.VectorOperators.AND;
import static jdk.incubator.vector.VectorOperators.LSHL;
import static jdk.incubator.vector.VectorOperators.LSHR;

public sealed class Panama22ESVectorUtilSupport extends PanamaESVectorUtilSupport permits Native22ESVectorUtilSupport {

    // BFloats (2 bytes) needs to be half the float vector bitsize
    private static final VectorSpecies<Short> BFLOAT_SPECIES;

    static {
        VectorSpecies<Short> species;
        try {
            species = VectorSpecies.of(short.class, VectorShape.forBitSize(FLOAT_SPECIES.vectorBitSize() / 2));
        } catch (IllegalArgumentException e) {
            species = null;
        }
        BFLOAT_SPECIES = species;
    }

    @Override
    public void floatToBFloat16(float[] floats, int floatOffset, byte[] bfloats, int bfloatOffset, int count, ByteOrder byteOrder) {
        if (BFLOAT_SPECIES == null) {
            DefaultESVectorUtilSupport.floatToBFloat16Impl(floats, floatOffset, bfloats, bfloatOffset, count, byteOrder);
        } else {
            MemorySegment buffer = MemorySegment.ofArray(bfloats);
            final int vectorEnd = FLOAT_SPECIES.loopBound(count);

            for (int i = 0; i < vectorEnd; i += FLOAT_SPECIES.length()) {
                IntVector bits = FloatVector.fromArray(FLOAT_SPECIES, floats, i + floatOffset).reinterpretAsInts();
                // roundingBias = 0x7fff + ((bits >> 16) & 1)
                IntVector bias = bits.lanewise(LSHR, 16).lanewise(AND, 1).add(0x7fff);
                bits = bits.add(bias);
                bits.lanewise(LSHR, 16)
                    .convertShape(VectorOperators.I2S, BFLOAT_SPECIES, 0)
                    .intoMemorySegment(buffer, (long) i * Short.BYTES + bfloatOffset, byteOrder);
            }

            if (vectorEnd < count) {
                // scalar tail
                DefaultESVectorUtilSupport.floatToBFloat16Impl(
                    floats,
                    vectorEnd + floatOffset,
                    bfloats,
                    vectorEnd * Short.BYTES + bfloatOffset,
                    count - vectorEnd,
                    byteOrder
                );
            }
        }
    }

    @Override
    public void bFloat16ToFloat(byte[] bfloats, int bfloatOffset, float[] floats, int floatOffset, int count, ByteOrder byteOrder) {
        if (BFLOAT_SPECIES == null) {
            DefaultESVectorUtilSupport.bFloat16ToFloatImpl(bfloats, bfloatOffset, floats, floatOffset, count, byteOrder);
        } else {
            MemorySegment buffer = MemorySegment.ofArray(bfloats);
            int vectorEnd = BFLOAT_SPECIES.loopBound(count);

            for (int i = 0; i < vectorEnd; i += BFLOAT_SPECIES.length()) {
                ShortVector sv = ShortVector.fromMemorySegment(BFLOAT_SPECIES, buffer, (long) i * Short.BYTES + bfloatOffset, byteOrder);
                sv.convertShape(VectorOperators.ZERO_EXTEND_S2I, INTEGER_SPECIES, 0)
                    .lanewise(LSHL, 16)
                    .reinterpretAsFloats()
                    .intoArray(floats, i + floatOffset);
            }

            if (vectorEnd < count) {
                // scalar tail
                DefaultESVectorUtilSupport.bFloat16ToFloatImpl(
                    bfloats,
                    vectorEnd * Short.BYTES + bfloatOffset,
                    floats,
                    vectorEnd + floatOffset,
                    count - vectorEnd,
                    byteOrder
                );
            }
        }
    }
}
