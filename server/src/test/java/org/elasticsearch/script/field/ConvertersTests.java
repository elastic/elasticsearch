/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class ConvertersTests extends ConvertersTestBase{

    public void testLongFieldToBigIntegerField() {
        // transform: long field to big integer field
        List<BigInteger> expectedBigIntegers = LongStream.of(rawLongValues).mapToObj(BigInteger::valueOf).collect(Collectors.toList());
        Field<BigInteger> toBigInteger = longField.as(BigIntegerField.BigInteger);
        assertEquals(expectedBigIntegers, toBigInteger.getValues());
        assertEquals(expectedBigIntegers.get(0), toBigInteger.getValue(null)); // default value ignored
        assertEquals(rawLongValues[0], toBigInteger.getLong(10)); // default value ignored
        assertEquals((double)rawLongValues[0], toBigInteger.getDouble(10.0), 0.0001); // default value ignored

        // reverse transform (symmetric): big integer field to long field
        Field<Long> toLong = toBigInteger.as(LongField.Long);
        assertEquals(LongStream.of(rawLongValues).boxed().collect(Collectors.toList()), toLong.getValues());
        assertEquals(Long.valueOf(rawLongValues[0]), toLong.getValue(null)); // default value ignored
        assertEquals(rawLongValues[0], toLong.getLong(10)); // default value ignored
        assertEquals((double)rawLongValues[0], toLong.getDouble(10.0d), 0.0001d); // default value ignored
    }

    public void testBigIntegerFieldToLongField() {
        // transform: big integer field to long field
        List<Long> expectedLongs = Stream.of(rawBigIntegerValues).mapToLong(BigInteger::longValue).boxed().collect(Collectors.toList());
        Field<Long> toLong = bigIntegerField.as(LongField.Long);
        assertEquals(expectedLongs, toLong.getValues());
        assertEquals(expectedLongs.get(0), toLong.getValue(null)); // default value ignored
        assertEquals(BigIntegerField.toLong(rawBigIntegerValues[0]), toLong.getLong(10)); // default value ignored
        assertEquals(BigIntegerField.toDouble(rawBigIntegerValues[0]), toLong.getDouble(10.0), 0.0001); // default value ignored

        // reverse transform (asymmetric): long field to big integer field
        Field<BigInteger> toBigInteger = toLong.as(BigIntegerField.BigInteger);
        assertEquals(expectedLongs.stream().map(BigInteger::valueOf).collect(Collectors.toList()), toBigInteger.getValues());
        assertEquals(
                BigInteger.valueOf(BigIntegerField.toLong(rawBigIntegerValues[0])),
                toBigInteger.getValue(null)); // default value ignored
        assertEquals(BigIntegerField.toLong(rawBigIntegerValues[0]), toBigInteger.getLong(10)); // default value ignored
        assertEquals(BigIntegerField.toDouble(rawBigIntegerValues[0]), toBigInteger.getDouble(10.0d), 0.0001d); // default value ignored
    }

    public void testDoubleFieldToBigIntegerField() {
        // transform: double field to big integer field
        List<BigInteger> expectedBigIntegers =
                Arrays.stream(rawDoubleValues).mapToObj(DoubleField::toBigInteger).collect(Collectors.toList());
        Field<BigInteger> toBigInteger = doubleField.as(BigIntegerField.BigInteger);
        assertEquals(expectedBigIntegers, toBigInteger.getValues());
        assertEquals(expectedBigIntegers.get(0), toBigInteger.getValue(null)); // default value ignored
        assertEquals((long)rawDoubleValues[0], toBigInteger.getLong(10)); // default value ignored
        assertEquals(
                BigIntegerField.toDouble(expectedBigIntegers.get(0)),
                toBigInteger.getDouble(10.0d), // default value ignored
                0.00001d);
    }

    public void testDoubleFieldToLongField() {
        // transform: double field to long field
        List<Long> expectedLongs = Arrays.stream(rawDoubleValues).mapToLong(d -> (long)d).boxed().collect(Collectors.toList());
        Field<Long> toLong = doubleField.as(LongField.Long);
        assertEquals(expectedLongs, toLong.getValues());
        assertEquals((Long)(long)rawDoubleValues[0], toLong.getValue(null)); // default value ignored
        assertEquals((long)rawDoubleValues[0], toLong.getLong(10)); // default value ignored
        assertEquals((double)(long)rawDoubleValues[0], toLong.getDouble(10.0d), 0.1d); // default value ignored
    }

    /*

    public void testStringToBigInteger() {
        List<String> raw = List.of(Long.MAX_VALUE + "0", randomLong() + "", Long.MIN_VALUE + "0", Double.MAX_VALUE + "",
                Double.MIN_VALUE + "");
        Field<String> src = new StringField("", new ListFieldValues<>(raw));

        Field<BigInteger> dst = src.as(bigIntegerField.BigInteger);
        BigInteger maxDouble = new BigInteger("17976931348623157" + "0".repeat(292));
        List<BigInteger> expected = List.of(new BigInteger(raw.get(0)), new BigInteger(raw.get(1)), new BigInteger(raw.get(2)), maxDouble,
                BigInteger.ZERO);
        assertEquals(expected, dst.getValues());
        assertEquals(expected.get(0), dst.getValue(null));
        assertEquals(-10L, dst.getLong(10)); // overflow
        assertEquals(9.223372036854776E19, dst.getDouble(10.0d), 0.1d);
    }

    public void testStringToLong() {
        long rand = randomLong();
        List<String> raw = List.of(rand + "", Long.MAX_VALUE + "", Long.MIN_VALUE + "", "0", "100");
        Field<String> src = new StringField("", new ListFieldValues<>(raw));

        Field<Long> dst = src.as(LongField.Long);
        assertEquals(List.of(rand, Long.MAX_VALUE, Long.MIN_VALUE, 0L, 100L), dst.getValues());
        assertEquals(Long.valueOf(rand), dst.getValue(null));
        assertEquals(rand, dst.getLong(10)); // overflow
        assertEquals(rand + 0.0d, dst.getDouble(10.0d), 0.9d);
    }

    public void testBooleanTo() {
        List<Boolean> raw = List.of(Boolean.TRUE, Boolean.FALSE);
        Field<Boolean> src = new BooleanField("", new ListFieldValues<>(raw));

        Field<BigInteger> dst = src.as(bigIntegerField.BigInteger);
        assertEquals(List.of(BigInteger.ONE, BigInteger.ZERO), dst.getValues());
        assertEquals(BigInteger.ONE, dst.getValue(null));
        assertEquals(1L, dst.getLong(10L));
        assertEquals(1.0d, dst.getDouble(1234.0d), 0.1d);

        Field<Long> dstLong = src.as(LongField.Long);
        assertEquals(List.of(1L, 0L), dstLong.getValues());
        assertEquals(Long.valueOf(1), dstLong.getValue(null));
        assertEquals(1L, dstLong.getLong(10L));
        assertEquals(1.0d, dstLong.getDouble(1234.0d), 0.1d);

        List<Boolean> rawRev = List.of(Boolean.FALSE, Boolean.TRUE);
        src = new BooleanField("", new ListFieldValues<>(rawRev));
        dst = src.as(bigIntegerField.BigInteger);

        assertEquals(List.of(BigInteger.ZERO, BigInteger.ONE), dst.getValues());
        assertEquals(BigInteger.ZERO, dst.getValue(null));
        assertEquals(0L, dst.getLong(10L));
        assertEquals(0.0d, dst.getDouble(1234.0d), 0.1d);

        dstLong = src.as(LongField.Long);
        assertEquals(List.of(0L, 1L), dstLong.getValues());
        assertEquals(Long.valueOf(0), dstLong.getValue(null));
        assertEquals(0L, dstLong.getLong(10L));
        assertEquals(0.0d, dstLong.getDouble(1234.0d), 0.1d);
    }

    public void testInvalidFieldConversion() {
        Field<GeoPoint> src = new GeoPointField("", new ListFieldValues<>(List.of(new GeoPoint(0, 0))));
        InvalidConversion ic = expectThrows(InvalidConversion.class, () -> src.as(bigIntegerField.BigInteger));
        assertEquals("Cannot convert from [GeoPointField] using converter [BigIntegerField]", ic.getMessage());

        ic = expectThrows(InvalidConversion.class, () -> src.as(LongField.Long));
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
        Field<JodaCompatibleZonedDateTime> src = new DateMillisField("", new ListFieldValues<>(raw));

        List<BigInteger> expectedBigInteger = LongStream.of(rawMilli).mapToObj(BigInteger::valueOf).collect(Collectors.toList());
        Field<BigInteger> dstBigInteger = src.as(bigIntegerField.BigInteger);
        assertEquals(expectedBigInteger, dstBigInteger.getValues());
        assertEquals(expectedBigInteger.get(0), dstBigInteger.getValue(null));
        assertEquals(rawMilli[0], dstBigInteger.getLong(-1000L));
        assertEquals((double) rawMilli[0], dstBigInteger.getDouble(-1234.5d), 1.1d);

        Field<Long> dstLong = src.as(LongField.Long);
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
        Field<JodaCompatibleZonedDateTime> src = new DateNanosField("", new ListFieldValues<>(raw));

        List<BigInteger> expectedBigInteger = LongStream.of(rawNanos).mapToObj(BigInteger::valueOf).collect(Collectors.toList());
        Field<BigInteger> dstBigInteger = src.as(bigIntegerField.BigInteger);
        assertEquals(expectedBigInteger, dstBigInteger.getValues());
        assertEquals(expectedBigInteger.get(0), dstBigInteger.getValue(null));
        assertEquals(rawNanos[0], dstBigInteger.getLong(-1000L));
        assertEquals((double) rawNanos[0], dstBigInteger.getDouble(-1234.5d), 1.1d);

        Field<Long> dstLong = src.as(LongField.Long);
        assertEquals(LongStream.of(rawNanos).boxed().collect(Collectors.toList()), dstLong.getValues());
        assertEquals(LongStream.of(rawNanos).boxed().collect(Collectors.toList()), dstLong.getValues());
        assertEquals(Long.valueOf(rawNanos[0]), dstLong.getValue(-100L));
        assertEquals(rawNanos[0], dstLong.getLong(-100L));
        assertEquals((double) rawNanos[0], dstLong.getDouble(-1234.5d), 1.1d);
    }

    */
}
