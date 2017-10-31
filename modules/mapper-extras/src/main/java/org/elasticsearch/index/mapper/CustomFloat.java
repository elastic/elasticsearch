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

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.PackedInts;

import java.util.function.DoubleToLongFunction;
import java.util.function.LongToDoubleFunction;

class CustomFloat {

    private CustomFloat() {}

    private static void checkNumSigBits(int numSigBits) {
        if (numSigBits < 1) {
            throw new IllegalArgumentException("numSigBits must be >= 1, got " + numSigBits);
        }
        if (numSigBits > 53) {
            throw new IllegalArgumentException("numSigBits must be <= 53, got " + numSigBits);
        }
    }

    private static void checkZeroExponent(int zeroExponent) {
        if (zeroExponent < -1023) {
            throw new IllegalArgumentException("zeroExponent must be >= -1023, got " + zeroExponent);
        }
        if (zeroExponent > 1023) {
            // we could support greater values in the future, but 0 looks like a good start
            // for a max value to me
            throw new IllegalArgumentException("zeroExponent must be <= 1023, got " + zeroExponent);
        }
    }

    /**
     * Get an encoder that preserves ordering and does not collapse -0 and +0.
     * The encoder only supports finite values.
     * @param numSigBits the number of significant bits, must be in 1..53
     * @param zeroExponent the exponent that is used to represent 0, must be in -1023..0
     */
    public static DoubleToLongFunction getEncoder(int numSigBits, int zeroExponent) {
        return new Encoder(numSigBits, zeroExponent)::encode;
    }

    /** Get a decoder of encoded values. */
    public static LongToDoubleFunction getDecoder(int numSigBits, int zeroExponent) {
        return new Decoder(numSigBits, zeroExponent)::decode;
    }

    private static class Encoder {
        
        private final int shift;
        private final long expDelta;
        
        Encoder(int numSigBits, int zeroExponent) {
            checkNumSigBits(numSigBits);
            checkZeroExponent(zeroExponent);
            this.shift = 53 - numSigBits;
            this.expDelta = (zeroExponent - -1023L);
        }
        
        long encode(double value) {
            if (Double.isFinite(value) == false) {
                throw new IllegalArgumentException("Cannot encode non-finite value: " + value);
            }
            long bits = Double.doubleToLongBits(value);
            long mantissaAndExponent = bits & 0x7fffffffffffffffL;

            long exponent = mantissaAndExponent >>> 52;
            if (exponent > expDelta) {
                // should be the common case
                mantissaAndExponent -= expDelta << 52;
            } else if (exponent != 0) {
                // need to go from normal to subnormal
                // restore the implicit bit
                mantissaAndExponent = (1L << 52) | (mantissaAndExponent & ((1L << 52) - 1));
                // and shift right, taking the min with 63 since shifts are mod 64
                mantissaAndExponent >>>= Math.min(63, 1 + expDelta - exponent);
            } else if (mantissaAndExponent != 0) {
                // need to go from subnormal to subnormal with a different base
                // taking the min with 63 since shifts are mod 64
                mantissaAndExponent >>>= Math.min(63, expDelta);
            }

            if (shift > 0) {
                // add 2^(shift-1) so that subsequent shift round rather than truncate
                mantissaAndExponent += 1L << (shift - 1);
                // and subtract the shift-th bit so that we round to even in case of tie
                // just like casting from double to float does
                mantissaAndExponent -= (mantissaAndExponent >>> shift) & 1;

                // make sure the rounding does not make us encode infinities
                long newExponent = (mantissaAndExponent >>> 52) + expDelta;
                if (newExponent >= 0x7ff) {
                    throw new IllegalArgumentException("Value " + value + " is too large and was rounded to +/-Infinity");
                }

                // clear trailing bits
                mantissaAndExponent &= (~0L << shift);
            }

            bits = bits & 0x8000000000000000L; // take the sign
            bits |= mantissaAndExponent; // and add bits back
            
            long encoded = NumericUtils.sortableDoubleBits(bits);
            return encoded >> shift; // signed shift on purpose
        }

    }

    private static class Decoder {

        private final int shift;
        private final long expDelta;
        
        Decoder(int numSigBits, int zeroExponent) {
            checkNumSigBits(numSigBits);
            checkZeroExponent(zeroExponent);
            this.shift = 53 - numSigBits;
            this.expDelta = (zeroExponent - -1023);
        }

        double decode(long bits) {
            bits <<= shift;
            if ((bits & 0x8000000000000000L) != 0) {
                bits |= ((1L << shift) - 1);
            }
            bits = NumericUtils.sortableDoubleBits(bits);
            long mantissaAndExponent = bits & 0x7fffffffffffffffL;
            long exponent = mantissaAndExponent >>> 52;
            if (exponent > 0) {
                // common case: normal
                bits += expDelta << 52;
            } else if (mantissaAndExponent != 0) {
                // non-zero subnormal
                int numLeadingZeros = Long.numberOfLeadingZeros(mantissaAndExponent);
                int s = numLeadingZeros - (64 - 53);
                if (s > expDelta) {
                    // remains subnormal
                    bits = (bits & 0x8000000000000000L) // sign
                            | (mantissaAndExponent << expDelta); // mantissa
                } else {
                    // becomes normal
                    bits = (bits & 0x8000000000000000L) // sign
                            | ((expDelta - s + 1) << 52) // exponent
                            | ((mantissaAndExponent << s) & ((1L << 52) - 1)); // mantissa
                }
            }
            return Double.longBitsToDouble(bits);
        }

    }
}
