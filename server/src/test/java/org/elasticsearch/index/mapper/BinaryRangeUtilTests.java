/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singleton;

public class BinaryRangeUtilTests extends ESTestCase {

    public void testBasics() {
        BytesRef encoded1 = new BytesRef(BinaryRangeUtil.encodeLong(Long.MIN_VALUE));
        BytesRef encoded2 = new BytesRef(BinaryRangeUtil.encodeLong(-1L));
        BytesRef encoded3 = new BytesRef(BinaryRangeUtil.encodeLong(0L));
        BytesRef encoded4 = new BytesRef(BinaryRangeUtil.encodeLong(1L));
        BytesRef encoded5 = new BytesRef(BinaryRangeUtil.encodeLong(Long.MAX_VALUE));

        assertTrue(encoded1.compareTo(encoded2) < 0);
        assertTrue(encoded2.compareTo(encoded1) > 0);
        assertTrue(encoded2.compareTo(encoded3) < 0);
        assertTrue(encoded3.compareTo(encoded2) > 0);
        assertTrue(encoded3.compareTo(encoded4) < 0);
        assertTrue(encoded4.compareTo(encoded3) > 0);
        assertTrue(encoded4.compareTo(encoded5) < 0);
        assertTrue(encoded5.compareTo(encoded4) > 0);

        encoded1 = new BytesRef(BinaryRangeUtil.encodeDouble(Double.NEGATIVE_INFINITY));
        encoded2 = new BytesRef(BinaryRangeUtil.encodeDouble(-1D));
        encoded3 = new BytesRef(BinaryRangeUtil.encodeDouble(-0D));
        encoded4 = new BytesRef(BinaryRangeUtil.encodeDouble(0D));
        encoded5 = new BytesRef(BinaryRangeUtil.encodeDouble(1D));
        BytesRef encoded6 = new BytesRef(BinaryRangeUtil.encodeDouble(Double.POSITIVE_INFINITY));

        assertTrue(encoded1.compareTo(encoded2) < 0);
        assertTrue(encoded2.compareTo(encoded1) > 0);
        assertTrue(encoded2.compareTo(encoded3) < 0);
        assertTrue(encoded3.compareTo(encoded2) > 0);
        assertTrue(encoded3.compareTo(encoded4) < 0);
        assertTrue(encoded4.compareTo(encoded3) > 0);
        assertTrue(encoded4.compareTo(encoded5) < 0);
        assertTrue(encoded5.compareTo(encoded4) > 0);
        assertTrue(encoded5.compareTo(encoded6) < 0);
        assertTrue(encoded6.compareTo(encoded5) > 0);

        encoded1 = new BytesRef(BinaryRangeUtil.encodeFloat(Float.NEGATIVE_INFINITY));
        encoded2 = new BytesRef(BinaryRangeUtil.encodeFloat(-1F));
        encoded3 = new BytesRef(BinaryRangeUtil.encodeFloat(-0F));
        encoded4 = new BytesRef(BinaryRangeUtil.encodeFloat(0F));
        encoded5 = new BytesRef(BinaryRangeUtil.encodeFloat(1F));
        encoded6 = new BytesRef(BinaryRangeUtil.encodeFloat(Float.POSITIVE_INFINITY));

        assertTrue(encoded1.compareTo(encoded2) < 0);
        assertTrue(encoded2.compareTo(encoded1) > 0);
        assertTrue(encoded2.compareTo(encoded3) < 0);
        assertTrue(encoded3.compareTo(encoded2) > 0);
        assertTrue(encoded3.compareTo(encoded4) < 0);
        assertTrue(encoded4.compareTo(encoded3) > 0);
        assertTrue(encoded4.compareTo(encoded5) < 0);
        assertTrue(encoded5.compareTo(encoded4) > 0);
        assertTrue(encoded5.compareTo(encoded6) < 0);
        assertTrue(encoded6.compareTo(encoded5) > 0);
    }

    public void testEncode_long() {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            long number1 = randomLong();
            BytesRef encodedNumber1 = new BytesRef(BinaryRangeUtil.encodeLong(number1));
            long number2 = randomBoolean() ? number1 + 1 : randomLong();
            BytesRef encodedNumber2 = new BytesRef(BinaryRangeUtil.encodeLong(number2));

            int cmp = normalize(Long.compare(number1, number2));
            assertEquals(cmp, normalize(encodedNumber1.compareTo(encodedNumber2)));
            cmp = normalize(Long.compare(number2, number1));
            assertEquals(cmp, normalize(encodedNumber2.compareTo(encodedNumber1)));
        }
    }

    public void testVariableLengthEncoding() {
        for (int i = -8; i <= 7; ++i) {
            assertEquals(1, BinaryRangeUtil.encodeLong(i).length);
        }
        for (int i = -2048; i <= 2047; ++i) {
            if (i < -8 || i > 7) {
                assertEquals(2, BinaryRangeUtil.encodeLong(i).length);
            }
        }
        assertEquals(3, BinaryRangeUtil.encodeLong(-2049).length);
        assertEquals(3, BinaryRangeUtil.encodeLong(2048).length);
        assertEquals(9, BinaryRangeUtil.encodeLong(Long.MIN_VALUE).length);
        assertEquals(9, BinaryRangeUtil.encodeLong(Long.MAX_VALUE).length);
    }

    public void testEncode_double() {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            double number1 = randomDouble();
            BytesRef encodedNumber1 = new BytesRef(BinaryRangeUtil.encodeDouble(number1));
            double number2 = randomBoolean() ? Math.nextUp(number1) : randomDouble();
            BytesRef encodedNumber2 = new BytesRef(BinaryRangeUtil.encodeDouble(number2));

            assertEquals(8, encodedNumber1.length);
            assertEquals(8, encodedNumber2.length);
            int cmp = normalize(Double.compare(number1, number2));
            assertEquals(cmp, normalize(encodedNumber1.compareTo(encodedNumber2)));
            cmp = normalize(Double.compare(number2, number1));
            assertEquals(cmp, normalize(encodedNumber2.compareTo(encodedNumber1)));
        }
    }

    public void testEncode_Float() {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            float number1 = randomFloat();
            BytesRef encodedNumber1 = new BytesRef(BinaryRangeUtil.encodeFloat(number1));
            float number2 = randomBoolean() ? Math.nextUp(number1) : randomFloat();
            BytesRef encodedNumber2 = new BytesRef(BinaryRangeUtil.encodeFloat(number2));

            assertEquals(4, encodedNumber1.length);
            assertEquals(4, encodedNumber2.length);
            int cmp = normalize(Double.compare(number1, number2));
            assertEquals(cmp, normalize(encodedNumber1.compareTo(encodedNumber2)));
            cmp = normalize(Double.compare(number2, number1));
            assertEquals(cmp, normalize(encodedNumber2.compareTo(encodedNumber1)));
        }
    }

    public void testDecodeLong() {
        long[] cases = new long[] { Long.MIN_VALUE, -2049, -2048, -128, -3, -1, 0, 1, 3, 125, 2048, 2049, Long.MAX_VALUE };
        for (long expected : cases) {
            byte[] encoded = BinaryRangeUtil.encodeLong(expected);
            int offset = 0;
            int length = RangeType.LengthType.VARIABLE.readLength(encoded, offset);
            assertEquals(expected, BinaryRangeUtil.decodeLong(encoded, offset, length));
        }
    }

    public void testDecodeLongRanges() throws IOException {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            long start = randomLong();
            long end = randomLongBetween(start + 1, Long.MAX_VALUE);
            RangeFieldMapper.Range expected = new RangeFieldMapper.Range(RangeType.LONG, start, end, true, true);
            List<RangeFieldMapper.Range> decoded = BinaryRangeUtil.decodeLongRanges(BinaryRangeUtil.encodeLongRanges(singleton(expected)));
            assertEquals(1, decoded.size());
            RangeFieldMapper.Range actual = decoded.get(0);
            assertEquals(expected, actual);
        }
    }

    public void testDecodeDoubleRanges() throws IOException {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            double start = randomDouble();
            double end = randomDoubleBetween(Math.nextUp(start), Double.MAX_VALUE, false);
            RangeFieldMapper.Range expected = new RangeFieldMapper.Range(RangeType.DOUBLE, start, end, true, true);
            List<RangeFieldMapper.Range> decoded = BinaryRangeUtil.decodeDoubleRanges(
                BinaryRangeUtil.encodeDoubleRanges(singleton(expected))
            );
            assertEquals(1, decoded.size());
            RangeFieldMapper.Range actual = decoded.get(0);
            assertEquals(expected, actual);
        }
    }

    public void testDecodeFloatRanges() throws IOException {
        int iters = randomIntBetween(32, 1024);
        for (int i = 0; i < iters; i++) {
            float start = randomFloat();
            // for some reason, ESTestCase doesn't provide randomFloatBetween
            float end = randomFloat();
            if (start > end) {
                float temp = start;
                start = end;
                end = temp;
            }
            RangeFieldMapper.Range expected = new RangeFieldMapper.Range(RangeType.FLOAT, start, end, true, true);
            List<RangeFieldMapper.Range> decoded = BinaryRangeUtil.decodeFloatRanges(
                BinaryRangeUtil.encodeFloatRanges(singleton(expected))
            );
            assertEquals(1, decoded.size());
            RangeFieldMapper.Range actual = decoded.get(0);
            assertEquals(expected, actual);
        }
    }

    public void testDecodeIPRanges() throws IOException {
        RangeFieldMapper.Range[] cases = { createIPRange("192.168.0.1", "192.168.0.100"), createIPRange("::ffff:c0a8:107", "2001:db8::") };
        for (RangeFieldMapper.Range expected : cases) {
            List<RangeFieldMapper.Range> decoded = BinaryRangeUtil.decodeIPRanges(BinaryRangeUtil.encodeIPRanges(singleton(expected)));
            assertEquals(1, decoded.size());
            RangeFieldMapper.Range actual = decoded.get(0);
            assertEquals(expected, actual);
        }
    }

    private RangeFieldMapper.Range createIPRange(String start, String end) {
        return new RangeFieldMapper.Range(RangeType.IP, InetAddresses.forString(start), InetAddresses.forString(end), true, true);
    }

    private static int normalize(int cmp) {
        if (cmp < 0) {
            return -1;
        } else if (cmp > 0) {
            return 1;
        }
        return 0;
    }

}
