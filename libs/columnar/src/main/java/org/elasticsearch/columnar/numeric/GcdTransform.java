/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.util.MathUtil;

import java.io.IOException;

/** Divides a block by a common divisor when one greater than one exists. Frozen id 2. */
public final class GcdTransform implements BlockTransform {

    static final byte ID = 2;

    /** Shared stateless instance. */
    public static final GcdTransform INSTANCE = new GcdTransform();

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public boolean tryEncode(long[] block, int valueCount, MetadataWriter params) throws IOException {
        long gcd = 0;
        for (int i = 0; i < valueCount; ++i) {
            gcd = MathUtil.gcd(gcd, block[i]);
            if (gcd == 1) {
                break;
            }
        }
        if (Long.compareUnsigned(gcd, 1) <= 0) {
            return false;
        }
        if ((gcd & (gcd - 1)) == 0) {
            // Power-of-two divisor: shift instead of divide.
            int shift = Long.numberOfTrailingZeros(gcd);
            for (int i = 0; i < valueCount; ++i) {
                block[i] >>>= shift;
            }
        } else {
            for (int i = 0; i < valueCount; ++i) {
                block[i] /= gcd;
            }
        }
        params.writeVLong(gcd - 2);
        return true;
    }

    @Override
    public void decode(long[] block, int valueCount, MetadataReader params) throws IOException {
        long gcd = 2 + params.readVLong();
        if ((gcd & (gcd - 1)) == 0) {
            // Power-of-two divisor: shift instead of multiply.
            int shift = Long.numberOfTrailingZeros(gcd);
            for (int i = 0; i < valueCount; ++i) {
                block[i] <<= shift;
            }
        } else {
            for (int i = 0; i < valueCount; ++i) {
                block[i] *= gcd;
            }
        }
    }
}
