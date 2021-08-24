/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

public class ConvertersTests extends ESTestCase {
    public void testLongToBigIntegerToLong() {
        long[] raw = { Long.MIN_VALUE, Long.MAX_VALUE, ((long) Integer.MIN_VALUE - 1), ((long) Integer.MAX_VALUE + 1), -1L, 0L, 1L };
        Field<Long> src = new Field.LongField("", new FieldValues<Long>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return raw.length;
            }

            @Override
            public List<Long> getValues() {
                return LongStream.of(raw).boxed().collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return raw[0];
            }

            @Override
            public long getLongValue() {
                return raw[0];
            }

            @Override
            public double getDoubleValue() {
                return raw[0];
            }
        });

        Field<BigInteger> dst = src.as(Field.BigInteger);

        List<BigInteger> expected = LongStream.of(raw).mapToObj(BigInteger::valueOf).collect(Collectors.toList());
        assertEquals(expected, dst.getValues());
        assertEquals(expected.get(0), dst.getValue(null));
        assertEquals(raw[0], dst.getLong(10));
        assertEquals((double) raw[0], dst.getDouble(10.0d), 0.1d);

        Field<Long> dstLong = dst.as(Field.Long);
        assertEquals(LongStream.of(raw).boxed().collect(Collectors.toList()), dstLong.getValues());
        assertEquals(Long.valueOf(raw[0]), dstLong.getValue(null));
        assertEquals(raw[0], dstLong.getLong(10));
        assertEquals((double) raw[0], dstLong.getDouble(10.0d), 0.1d);
    }

    public void testDoubleTo() {
        double[] raw = { Double.MAX_VALUE, Double.MIN_VALUE, ((double) Float.MAX_VALUE) * 10d, ((double) Float.MIN_VALUE), 0.1d,
                         Long.MAX_VALUE, Long.MIN_VALUE };
        Field<Double> src = new Field.DoubleField("", new FieldValues<Double>() {
            @Override
            public boolean isEmpty() {
                return false;
            }

            @Override
            public int size() {
                return raw.length;
            }

            @Override
            public List<Double> getValues() {
                return DoubleStream.of(raw).boxed().collect(Collectors.toList());
            }

            @Override
            public Double getNonPrimitiveValue() {
                return raw[0];
            }

            @Override
            public long getLongValue() {
                return (long) raw[0];
            }

            @Override
            public double getDoubleValue() {
                return raw[0];
            }
        });

        Field<BigInteger> dst = src.as(Field.BigInteger);
        BigInteger maxDouble = new BigInteger("17976931348623157" + "0".repeat(292));
        List<BigInteger> expected = List.of(maxDouble, BigInteger.ZERO, new BigInteger("34028234663852886" + "0".repeat(23)),
                                            BigInteger.ZERO, BigInteger.ZERO,
                                            new BigInteger("9223372036854776000"), // Long.MAX_VALUE: 9223372036854775807
                                            new BigInteger("-9223372036854776000")); // Long.MIN_VALUE: -9223372036854775808
        assertEquals(expected, dst.getValues());
        assertEquals(expected.get(0), dst.getValue(null));
        assertEquals(Long.MAX_VALUE, dst.getLong(10));
        assertEquals(Double.MAX_VALUE, dst.getDouble(10.0d), 0.1d);

        Field<Long> lng = src.as(Field.Long);
        List<Long> lngExpected = List.of(Long.MAX_VALUE, 0L, Long.MAX_VALUE, 0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE);
        assertEquals(lngExpected, lng.getValues());
        assertEquals(Long.valueOf(Long.MAX_VALUE), lng.getValue(null));
        assertEquals(Long.MAX_VALUE, lng.getLong(10));
        assertEquals(Double.MAX_VALUE, lng.getDouble(10.0d), 0.1d);
    }

    public void testStringToBigInteger() {
        List<String> raw = List.of(Long.MAX_VALUE + "0", Long.MIN_VALUE + "0", Double.MAX_VALUE + "", Double.MIN_VALUE + "");
        Field<String> src = new Field.StringField("", new ListFieldValues<>(raw));

        Field<BigInteger> dst = src.as(Field.BigInteger);
        BigInteger maxDouble = new BigInteger("17976931348623157" + "0".repeat(292));
        List<BigInteger> expected = List.of(new BigInteger(raw.get(0)), new BigInteger(raw.get(1)), maxDouble, BigInteger.ZERO);
        assertEquals(expected, dst.getValues());
        assertEquals(expected.get(0), dst.getValue(null));
        assertEquals(-10L, dst.getLong(10)); // overflow
        assertEquals(9.223372036854776E19, dst.getDouble(10.0d), 0.1d);
    }

    public void testStringToLong() {
        List<String> raw = List.of(Long.MAX_VALUE + "", Long.MIN_VALUE + "", "0", "100");
        Field<String> src = new Field.StringField("", new ListFieldValues<>(raw));

        Field<Long> dst = src.as(Field.Long);
        assertEquals(List.of(Long.MAX_VALUE, Long.MIN_VALUE, 0L, 100L), dst.getValues());
        assertEquals(Long.valueOf(Long.MAX_VALUE), dst.getValue(null));
        assertEquals(Long.MAX_VALUE, dst.getLong(10)); // overflow
        assertEquals(Long.MAX_VALUE + 0.0d, dst.getDouble(10.0d), 0.1d);
    }

    public void testBooleanTo() {
        List<Boolean> raw = List.of(Boolean.TRUE, Boolean.FALSE);
        Field<Boolean> src = new Field.BooleanField("", new ListFieldValues<>(raw));

        Field<BigInteger> dst = src.as(Field.BigInteger);
        assertEquals(List.of(BigInteger.ONE, BigInteger.ZERO), dst.getValues());
        assertEquals(BigInteger.ONE, dst.getValue(null));
        assertEquals(1L, dst.getLong(10L));
        assertEquals(1.0d, dst.getDouble(1234.0d), 0.1d);

        Field<Long> dstLong = src.as(Field.Long);
        assertEquals(List.of(1L, 0L), dstLong.getValues());
        assertEquals(Long.valueOf(1), dstLong.getValue(null));
        assertEquals(1L, dstLong.getLong(10L));
        assertEquals(1.0d, dstLong.getDouble(1234.0d), 0.1d);

        List<Boolean> rawRev = List.of(Boolean.FALSE, Boolean.TRUE);
        src = new Field.BooleanField("", new ListFieldValues<>(rawRev));
        dst = src.as(Field.BigInteger);

        assertEquals(List.of(BigInteger.ZERO, BigInteger.ONE), dst.getValues());
        assertEquals(BigInteger.ZERO, dst.getValue(null));
        assertEquals(0L, dst.getLong(10L));
        assertEquals(0.0d, dst.getDouble(1234.0d), 0.1d);

        dstLong = src.as(Field.Long);
        assertEquals(List.of(0L, 1L), dstLong.getValues());
        assertEquals(Long.valueOf(0), dstLong.getValue(null));
        assertEquals(0L, dstLong.getLong(10L));
        assertEquals(0.0d, dstLong.getDouble(1234.0d), 0.1d);
    }

    public void testInvalidFieldConversion() {
        Field<GeoPoint> src = new Field.GeoPointField("", new ListFieldValues<>(List.of(new GeoPoint(0, 0))));
        InvalidConversion ic = expectThrows(InvalidConversion.class, () -> src.as(Field.BigInteger));
        assertEquals("Cannot convert from [GeoPointField] using converter [BigIntegerField]", ic.getMessage());

        ic = expectThrows(InvalidConversion.class, () -> src.as(Field.Long));
        assertEquals("Cannot convert from [GeoPointField] using converter [LongField]", ic.getMessage());
    }

    public void testDateMillisTo() {
        long[] rawMilli = { 1629830752000L, 0L, 2040057952000L, -6106212564000L};
        List<JodaCompatibleZonedDateTime> raw = List.of(
            new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(rawMilli[0]), ZoneOffset.ofHours(-7)),
            new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(rawMilli[1]), ZoneOffset.ofHours(-6)),
            new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(rawMilli[2]), ZoneOffset.ofHours(0)),
            new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(rawMilli[3]), ZoneOffset.ofHours(-5))
        );
        Field<JodaCompatibleZonedDateTime> src = new Field.DateMillisField("", new ListFieldValues<>(raw));

        List<BigInteger> expectedBigInteger = LongStream.of(rawMilli).mapToObj(BigInteger::valueOf).collect(Collectors.toList());
        Field<BigInteger> dstBigInteger = src.as(Field.BigInteger);
        assertEquals(expectedBigInteger, dstBigInteger.getValues());
        assertEquals(expectedBigInteger.get(0), dstBigInteger.getValue(null));
        assertEquals(rawMilli[0], dstBigInteger.getLong(-1000L));
        assertEquals((double) rawMilli[0], dstBigInteger.getDouble(-1234.5d), 1.1d);

        Field<Long> dstLong = src.as(Field.Long);
        assertEquals(LongStream.of(rawMilli).boxed().collect(Collectors.toList()), dstLong.getValues());
        assertEquals(LongStream.of(rawMilli).boxed().collect(Collectors.toList()), dstLong.getValues());
        assertEquals(Long.valueOf(rawMilli[0]), dstLong.getValue(-100L));
        assertEquals(rawMilli[0], dstLong.getLong(-100L));
        assertEquals((double) rawMilli[0], dstLong.getDouble(-1234.5d), 1.1d);
    }

    public void testDateNanoTo() {
        long[] rawNanos = { 1629830752000123L, 0L, 2040057952000456L, -6106212564000789L};
        List<JodaCompatibleZonedDateTime> raw = List.of(
            new JodaCompatibleZonedDateTime(Instant.EPOCH.plusNanos(rawNanos[0]), ZoneOffset.ofHours(-7)),
            new JodaCompatibleZonedDateTime(Instant.EPOCH.plusNanos(rawNanos[1]), ZoneOffset.ofHours(-6)),
            new JodaCompatibleZonedDateTime(Instant.EPOCH.plusNanos(rawNanos[2]), ZoneOffset.ofHours(0)),
            new JodaCompatibleZonedDateTime(Instant.EPOCH.plusNanos(rawNanos[3]), ZoneOffset.ofHours(-5))
        );
        Field<JodaCompatibleZonedDateTime> src = new Field.DateNanosField("", new ListFieldValues<>(raw));

        List<BigInteger> expectedBigInteger = LongStream.of(rawNanos).mapToObj(BigInteger::valueOf).collect(Collectors.toList());
        Field<BigInteger> dstBigInteger = src.as(Field.BigInteger);
        assertEquals(expectedBigInteger, dstBigInteger.getValues());
        assertEquals(expectedBigInteger.get(0), dstBigInteger.getValue(null));
        assertEquals(rawNanos[0], dstBigInteger.getLong(-1000L));
        assertEquals((double) rawNanos[0], dstBigInteger.getDouble(-1234.5d), 1.1d);

        Field<Long> dstLong = src.as(Field.Long);
        assertEquals(LongStream.of(rawNanos).boxed().collect(Collectors.toList()), dstLong.getValues());
        assertEquals(LongStream.of(rawNanos).boxed().collect(Collectors.toList()), dstLong.getValues());
        assertEquals(Long.valueOf(rawNanos[0]), dstLong.getValue(-100L));
        assertEquals(rawNanos[0], dstLong.getLong(-100L));
        assertEquals((double) rawNanos[0], dstLong.getDouble(-1234.5d), 1.1d);
    }

    static class ListFieldValues<T> implements FieldValues<T> {
        final List<T> values;

        public ListFieldValues(List<T> values) {
            this.values = values;
        }

        @Override
        public boolean isEmpty() {
            return values.isEmpty();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public List<T> getValues() {
            return values;
        }

        @Override
        public T getNonPrimitiveValue() {
            return values.get(0);
        }

        @Override
        public long getLongValue() {
            return 0;
        }

        @Override
        public double getDoubleValue() {
            return 0;
        }
    }
}
