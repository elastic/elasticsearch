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
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

enum BinaryRangeUtil {

    ;

    static BytesRef encodeLongRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
        List<RangeFieldMapper.Range> sortedRanges = new ArrayList<>(ranges);
        Comparator<RangeFieldMapper.Range> fromComparator = Comparator.comparingLong(range -> ((Number) range.from).longValue());
        Comparator<RangeFieldMapper.Range> toComparator = Comparator.comparingLong(range -> ((Number) range.to).longValue());
        sortedRanges.sort(fromComparator.thenComparing(toComparator));

        final byte[] encoded = new byte[5 + (9 * 2) * sortedRanges.size()];
        ByteArrayDataOutput out = new ByteArrayDataOutput(encoded);
        out.writeVInt(sortedRanges.size());
        for (RangeFieldMapper.Range range : sortedRanges) {
            byte[] encodedFrom = encodeLong(((Number) range.from).longValue());
            out.writeBytes(encodedFrom, encodedFrom.length);
            byte[] encodedTo = encodeLong(((Number) range.to).longValue());
            out.writeBytes(encodedTo, encodedTo.length);
        }
        return new BytesRef(encoded, 0, out.getPosition());
    }

    static BytesRef encodeDoubleRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
        List<RangeFieldMapper.Range> sortedRanges = new ArrayList<>(ranges);
        Comparator<RangeFieldMapper.Range> fromComparator = Comparator.comparingDouble(range -> ((Number) range.from).doubleValue());
        Comparator<RangeFieldMapper.Range> toComparator = Comparator.comparingDouble(range -> ((Number) range.to).doubleValue());
        sortedRanges.sort(fromComparator.thenComparing(toComparator));

        final byte[] encoded = new byte[5 + (8 * 2) * sortedRanges.size()];
        ByteArrayDataOutput out = new ByteArrayDataOutput(encoded);
        out.writeVInt(sortedRanges.size());
        for (RangeFieldMapper.Range range : sortedRanges) {
            byte[] encodedFrom = encodeDouble(((Number) range.from).doubleValue());
            out.writeBytes(encodedFrom, encodedFrom.length);
            byte[] encodedTo = encodeDouble(((Number) range.to).doubleValue());
            out.writeBytes(encodedTo, encodedTo.length);
        }
        return new BytesRef(encoded, 0, out.getPosition());
    }

    static BytesRef encodeFloatRanges(Set<RangeFieldMapper.Range> ranges) throws IOException {
        List<RangeFieldMapper.Range> sortedRanges = new ArrayList<>(ranges);
        Comparator<RangeFieldMapper.Range> fromComparator = Comparator.comparingDouble(range -> ((Number) range.from).floatValue());
        Comparator<RangeFieldMapper.Range> toComparator = Comparator.comparingDouble(range -> ((Number) range.to).floatValue());
        sortedRanges.sort(fromComparator.thenComparing(toComparator));

        final byte[] encoded = new byte[5 + (4 * 2) * sortedRanges.size()];
        ByteArrayDataOutput out = new ByteArrayDataOutput(encoded);
        out.writeVInt(sortedRanges.size());
        for (RangeFieldMapper.Range range : sortedRanges) {
            byte[] encodedFrom = encodeFloat(((Number) range.from).floatValue());
            out.writeBytes(encodedFrom, encodedFrom.length);
            byte[] encodedTo = encodeFloat(((Number) range.to).floatValue());
            out.writeBytes(encodedTo, encodedTo.length);
        }
        return new BytesRef(encoded, 0, out.getPosition());
    }

    static byte[] encodeDouble(double number) {
        byte[] encoded = new byte[8];
        NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(number), encoded, 0);
        return encoded;
    }

    static byte[] encodeFloat(float number) {
        byte[] encoded = new byte[4];
        NumericUtils.intToSortableBytes(NumericUtils.floatToSortableInt(number), encoded, 0);
        return encoded;
    }

    /**
     * Encodes the specified number of type long in a variable-length byte format.
     * The byte format preserves ordering, which means the returned byte array can be used for comparing as is.
     * The first bit stores the sign and the 4 subsequent bits encode the number of bytes that are used to
     * represent the long value, in addition to the first one.
     */
    static byte[] encodeLong(long number) {
        int sign = 1; // means positive
        if (number < 0) {
            number = -1 - number;
            sign = 0;
        }
        return encode(number, sign);
    }

    private static byte[] encode(long l, int sign) {
        assert l >= 0;

        // the header is formed of:
        // - 1 bit for the sign
        // - 4 bits for the number of additional bytes
        // - up to 3 bits of the value
        // additional bytes are data bytes

        int numBits = 64 - Long.numberOfLeadingZeros(l);
        int numAdditionalBytes = (numBits + 7 - 3) / 8;

        byte[] encoded = new byte[1 + numAdditionalBytes];

        // write data bytes
        int i = encoded.length;
        while (numBits > 0) {
            int index = --i;
            assert index > 0 || numBits <= 3; // byte 0 can't encode more than 3 bits
            encoded[index] = (byte) l;
            l >>>= 8;
            numBits -= 8;
        }
        assert Byte.toUnsignedInt(encoded[0]) <= 0x07;
        assert encoded.length == 1 || encoded[0] != 0 || Byte.toUnsignedInt(encoded[1]) > 0x07;

        if (sign == 0) {
            // reverse the order
            for (int j = 0; j < encoded.length; ++j) {
                encoded[j] = (byte) ~Byte.toUnsignedInt(encoded[j]);
            }
            // the first byte only uses 3 bits, we need the 5 upper bits for the header
            encoded[0] &= 0x07;
        }

        // write the header
        encoded[0] |= sign << 7;
        if (sign > 0) {
            encoded[0] |= numAdditionalBytes << 3;
        } else {
            encoded[0] |= (15 - numAdditionalBytes) << 3;
        }
        return encoded;
    }
}
