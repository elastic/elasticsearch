/*
* Licensed to Elasticsearch under one or more contributor
* license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright
* ownership. Elasticsearch licenses this file to you under
* the Apache License, Version 2.0 (the "License"); you may
* not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.lucene.util;

import java.math.BigInteger;

/**
 * Copies from {@link org.apache.lucene.util.NumericUtils} to support {@link java.math.BigInteger}
 */
public final class BigNumericUtils {

    public static final int VALUE_SIZE_DEFAULT = 128;

    public static enum BigNumericType {
        BIG_INT
    }

    private static int getBufferSize(int valueSize) {
        return (valueSize - 1)/7 + 2;
    }

    public static int bigIntToPrefixCoded(final BigInteger val, final int shift, final BytesRef bytes, final int valueSize) {
        bigIntToPrefixCodedBytes(val, shift, bytes, valueSize);
        return bytes.hashCode();
    }

    public static void bigIntToPrefixCodedBytes(final BigInteger val, final int shift, final BytesRef bytes, final int valueSize) {
        if (shift < 0 || shift > valueSize)  // ensure shift is 0..127
            throw new IllegalArgumentException("Illegal shift value, must be 0.." + valueSize);
        int nChars = (valueSize - 1 - shift) / 7 + 1;
        bytes.offset = 0;
        bytes.length = nChars+1;   // one extra for the byte that contains the shift info
        if (bytes.bytes.length < bytes.length) {
            bytes.bytes = new byte[getBufferSize(valueSize)];  // use the max
        }
        bytes.bytes[0] = (byte)(shift);
        BigInteger sortableBits = val.xor(BigInteger.valueOf(-1).shiftLeft(valueSize - 1));
        sortableBits = sortableBits.shiftRight(shift);
        while (nChars > 0) {
            // Store 7 bits per byte for compatibility
            // with UTF-8 encoding of terms
            bytes.bytes[nChars--] = sortableBits.and(BigInteger.valueOf(0x7f)).byteValue();
            sortableBits = sortableBits.shiftRight(7);
        }
    }

    public static void splitRange(
            final BigIntegerRangeBuilder builder, final int valSize,
            final int precisionStep, BigInteger minBound, BigInteger maxBound
    ) {
        if (precisionStep < 1)
            throw new IllegalArgumentException("precisionStep must be >=1");
        if (minBound.compareTo(maxBound) > 0) return;
        for (int shift=0; ; shift += precisionStep) {
            // calculate new bounds for inner precision
            final BigInteger diff = BigInteger.valueOf(1L << (shift+precisionStep)) ,
                    mask = BigInteger.valueOf(((1L<<precisionStep) - 1L) << shift) ;
            final boolean
                    hasLower = !minBound.and(mask).equals(BigInteger.ZERO),
                    hasUpper = !minBound.and(mask).equals(mask);
            final BigInteger
                    nextMinBound = (hasLower ? minBound.add(diff) : minBound).and(mask.not()),
                    nextMaxBound = (hasUpper ? maxBound.subtract(diff) : maxBound).and(mask.not());
            final boolean
                    lowerWrapped = nextMinBound.compareTo(minBound) < 0,
                    upperWrapped = nextMaxBound.compareTo(maxBound) > 0;

            if (shift+precisionStep>=valSize || nextMinBound.compareTo(nextMaxBound) > 0 || lowerWrapped || upperWrapped) {
                // We are in the lowest precision or the next precision is not available.
                addRange(builder, valSize, minBound, maxBound, shift);
                // exit the split recursion loop
                break;
            }

            if (hasLower)
                addRange(builder, valSize, minBound, minBound.or(mask), shift);
            if (hasUpper)
                addRange(builder, valSize, maxBound.add(mask.not()), maxBound, shift);

            // recurse to next precision
            minBound = nextMinBound;
            maxBound = nextMaxBound;
        }
    }

    private static void addRange(
            final BigIntegerRangeBuilder builder, final int valSize,
            BigInteger minBound, BigInteger maxBound,
            final int shift
    ) {
        maxBound = maxBound.or(BigInteger.valueOf((1L << shift) - 1L));
        builder.addRange(minBound, maxBound, shift, valSize);
    }


    public static abstract class BigIntegerRangeBuilder {

        /**
         * Overwrite this method, if you like to receive the already prefix encoded range bounds.
         * You can directly build classical (inclusive) range queries from them.
         */
        public void addRange(BytesRef minPrefixCoded, BytesRef maxPrefixCoded) {
            throw new UnsupportedOperationException();
        }

        /**
         * Overwrite this method, if you like to receive the raw long range bounds.
         * You can use this for e.g. debugging purposes (print out range bounds).
         */
        public void addRange(final BigInteger min, final BigInteger max, final int shift, final int valueSize) {
            final BytesRef minBytes = new BytesRef(getBufferSize(valueSize)), maxBytes = new BytesRef(getBufferSize(valueSize));
            bigIntToPrefixCodedBytes(min, shift, minBytes, valueSize);
            bigIntToPrefixCodedBytes(max, shift, maxBytes, valueSize);
            addRange(minBytes, maxBytes);
        }

    }

}

