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

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

enum BinaryRangeUtil {

    ;

    static BytesRef encodeLongRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
        List<RangeFieldMapper.Range> sortedRanges = new ArrayList<>(ranges);
        sortedRanges.sort((r1, r2) -> {
            long r1From = ((Number) r1.from).longValue();
            long r2From = ((Number) r2.from).longValue();
            int cmp = Long.compare(r1From, r2From);
            if (cmp != 0) {
                return cmp;
            } else {
                long r1To = ((Number) r1.from).longValue();
                long r2To = ((Number) r2.from).longValue();
                return Long.compare(r1To, r2To);
            }
        });

        final byte[] encoded = new byte[5 + ((5 + 9) * 2) * sortedRanges.size()];
        ByteArrayDataOutput out = new ByteArrayDataOutput(encoded);
        out.writeVInt(sortedRanges.size());
        for (RangeFieldMapper.Range range : sortedRanges) {
            byte[] encodedFrom = encode(((Number) range.from).longValue());
            out.writeVInt(encodedFrom.length);
            out.writeBytes(encodedFrom, encodedFrom.length);
            byte[] encodedTo = encode(((Number) range.to).longValue());
            out.writeVInt(encodedTo.length);
            out.writeBytes(encodedTo, encodedTo.length);
        }
        return new BytesRef(encoded, 0, out.getPosition());
    }

    static BytesRef encodeDoubleRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
        List<RangeFieldMapper.Range> sortedRanges = new ArrayList<>(ranges);
        sortedRanges.sort((r1, r2) -> {
            double r1From = ((Number) r1.from).doubleValue();
            double r2From = ((Number) r2.from).doubleValue();
            int cmp = Double.compare(r1From, r2From);
            if (cmp != 0) {
                return cmp;
            } else {
                double r1To = ((Number) r1.from).doubleValue();
                double r2To = ((Number) r2.from).doubleValue();
                return Double.compare(r1To, r2To);
            }
        });

        final byte[] encoded = new byte[5 + ((5 + 9) * 2) * sortedRanges.size()];
        ByteArrayDataOutput out = new ByteArrayDataOutput(encoded);
        out.writeVInt(sortedRanges.size());
        for (RangeFieldMapper.Range range : sortedRanges) {
            byte[] encodedFrom = BinaryRangeUtil.encode(((Number) range.from).doubleValue());
            out.writeVInt(encodedFrom.length);
            out.writeBytes(encodedFrom, encodedFrom.length);
            byte[] encodedTo = BinaryRangeUtil.encode(((Number) range.to).doubleValue());
            out.writeVInt(encodedTo.length);
            out.writeBytes(encodedTo, encodedTo.length);
        }
        return new BytesRef(encoded, 0, out.getPosition());
    }

    /**
     * Encodes the specified number of type long in a variable-length byte format.
     * The byte format preserves ordering, which means the returned byte array can be used for comparing as is.
     */
    static byte[] encode(long number) {
        int sign = 1; // means positive
        if (number < 0) {
            number = -1 - number;
            sign = 0;
        }
        return encode(number, sign);
    }

    /**
     * Encodes the specified number of type double in a variable-length byte format.
     * The byte format preserves ordering, which means the returned byte array can be used for comparing as is.
     */
    static byte[] encode(double number) {
        long l;
        int sign;
        if (number < 0.0) {
            l = Double.doubleToRawLongBits(-0d - number);
            sign = 0;
        } else {
            l = Double.doubleToRawLongBits(number);
            sign = 1; // means positive
        }
        return encode(l, sign);
    }

    private static byte[] encode(long l, int sign) {
        assert l >= 0;
        int bits = 64 - Long.numberOfLeadingZeros(l);

        int numBytes = (bits + 7) / 8; // between 0 and 8
        byte[] encoded = new byte[1 + numBytes];
        // encode the sign first to make sure positive values compare greater than negative values
        // and then the number of bytes, to make sure that large values compare greater than low values
        if (sign > 0) {
            encoded[0] = (byte) ((sign << 4) | numBytes);
        } else {
            encoded[0] = (byte) ((sign << 4) | (8 - numBytes));
        }
        for (int b = 0; b < numBytes; ++b) {
            if (sign == 1) {
                encoded[encoded.length - 1 - b] = (byte) (l >>> (8 * b));
            } else if (sign == 0) {
                encoded[encoded.length - 1 - b] = (byte) (0xFF - ((l >>> (8 * b)) & 0xFF));
            } else {
                throw new AssertionError();
            }
        }
        return encoded;
    }

}
