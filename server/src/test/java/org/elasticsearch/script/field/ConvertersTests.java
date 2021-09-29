/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class ConvertersTests extends ConvertersTestBase{

    public void testInvalidFieldConversion() {
        InvalidConversion ic = expectThrows(InvalidConversion.class, () -> getPointField.as(BigIntegerField.BigInteger));
        assertEquals("Cannot convert from [GeoPointField] using converter [BigIntegerField]", ic.getMessage());

        ic = expectThrows(InvalidConversion.class, () -> getPointField.as(LongField.Long));
        assertEquals("Cannot convert from [GeoPointField] using converter [LongField]", ic.getMessage());
    }

    public void testLongFieldToBigIntegerField() {
        // transform: long field to big integer field
        List<BigInteger> expectedBigIntegers = LongStream.of(rawLongValues).mapToObj(BigInteger::valueOf).collect(Collectors.toList());
        Field<BigInteger> toBigIntegerField = longField.as(BigIntegerField.BigInteger);
        assertEquals(expectedBigIntegers, toBigIntegerField.getValues());
        assertEquals(expectedBigIntegers.get(0), toBigIntegerField.getValue(null)); // default value ignored
        assertEquals(rawLongValues[0], toBigIntegerField.getLong(10)); // default value ignored
        assertEquals((double)rawLongValues[0], toBigIntegerField.getDouble(10.0), 0.0001); // default value ignored

        // reverse transform (symmetric): big integer field to long field
        Field<Long> toLongField = toBigIntegerField.as(LongField.Long);
        assertEquals(LongStream.of(rawLongValues).boxed().collect(Collectors.toList()), toLongField.getValues());
        assertEquals(Long.valueOf(rawLongValues[0]), toLongField.getValue(null)); // default value ignored
        assertEquals(rawLongValues[0], toLongField.getLong(10)); // default value ignored
        assertEquals((double)rawLongValues[0], toLongField.getDouble(10.0d), 0.0001d); // default value ignored
    }

    public void testBigIntegerFieldToLongField() {
        // transform: big integer field to long field
        List<Long> expectedLongs = Stream.of(rawBigIntegerValues).mapToLong(BigInteger::longValue).boxed().collect(Collectors.toList());
        Field<Long> toLongField = bigIntegerField.as(LongField.Long);
        assertEquals(expectedLongs, toLongField.getValues());
        assertEquals(expectedLongs.get(0), toLongField.getValue(null)); // default value ignored
        assertEquals(rawBigIntegerValues[0].longValue(), toLongField.getLong(10)); // default value ignored
        assertEquals(rawBigIntegerValues[0].doubleValue(), toLongField.getDouble(10.0), 0.0001); // default value ignored

        // reverse transform (asymmetric): long field to big integer field
        Field<BigInteger> toBigIntegerField = toLongField.as(BigIntegerField.BigInteger);
        assertEquals(expectedLongs.stream().map(BigInteger::valueOf).collect(Collectors.toList()), toBigIntegerField.getValues());
        assertEquals(
                BigInteger.valueOf(rawBigIntegerValues[0].longValue()),
                toBigIntegerField.getValue(null)); // default value ignored
        assertEquals(rawBigIntegerValues[0].longValue(), toBigIntegerField.getLong(10)); // default value ignored
        assertEquals(
                rawBigIntegerValues[0].doubleValue(),
                toBigIntegerField.getDouble(10.0d), // default value ignored
                0.0001d);
    }

    public void testDoubleFieldToBigIntegerField() {
        // transform: double field to big integer field
        List<BigInteger> expectedBigIntegers =
                Arrays.stream(rawDoubleValues).mapToObj(DoubleField::toBigInteger).collect(Collectors.toList());
        Field<BigInteger> toBigIntegerField = doubleField.as(BigIntegerField.BigInteger);
        assertEquals(expectedBigIntegers, toBigIntegerField.getValues());
        assertEquals(expectedBigIntegers.get(0), toBigIntegerField.getValue(null)); // default value ignored
        assertEquals((long)rawDoubleValues[0], toBigIntegerField.getLong(10)); // default value ignored
        assertEquals(
                expectedBigIntegers.get(0).doubleValue(),
                toBigIntegerField.getDouble(10.0d), // default value ignored
                0.00001d);
    }

    public void testDoubleFieldToLongField() {
        // transform: double field to long field
        List<Long> expectedLongs = Arrays.stream(rawDoubleValues).mapToLong(d -> (long)d).boxed().collect(Collectors.toList());
        Field<Long> toLongField = doubleField.as(LongField.Long);
        assertEquals(expectedLongs, toLongField.getValues());
        assertEquals((Long)(long)rawDoubleValues[0], toLongField.getValue(null)); // default value ignored
        assertEquals((long)rawDoubleValues[0], toLongField.getLong(10)); // default value ignored
        assertEquals((double)(long)rawDoubleValues[0], toLongField.getDouble(10.0d), 0.1d); // default value ignored
    }

    public void testStringFieldToBigIntegerField() {
        // transform: string field to big integer field
        List<BigInteger> expectedBigIntegers =
                Arrays.stream(rawStringValuesAsDoubles).map(StringField::toBigInteger).collect(Collectors.toList());
        Field<BigInteger> toBigIntegerField = stringFieldAsDoubles.as(BigIntegerField.BigInteger);
        assertEquals(expectedBigIntegers, toBigIntegerField.getValues());
        assertEquals(expectedBigIntegers.get(0), toBigIntegerField.getValue(null)); // default value ignored
        assertEquals(expectedBigIntegers.get(0).longValue(), toBigIntegerField.getLong(10)); // default value ignored
        assertEquals(expectedBigIntegers.get(0).doubleValue(), toBigIntegerField.getDouble(10.0d), 0.1d); // default value ignored
    }

    public void testStringFieldToLongField() {
        // transform: string field to long field
        List<Long> expectedLongs = Arrays.stream(rawStringValuesAsLongs).map(Long::parseLong).collect(Collectors.toList());
        Field<Long> toLongField = stringFieldAsLongs.as(LongField.Long);
        assertEquals(expectedLongs, toLongField.getValues());
        assertEquals(expectedLongs.get(0), toLongField.getValue(null)); // default value ignored
        assertEquals(expectedLongs.get(0).longValue(), toLongField.getLong(10)); // default value ignored
        assertEquals(expectedLongs.get(0).doubleValue(), toLongField.getDouble(10.0d), 0.1d); // default value ignored
    }

    public void testBooleanFieldToBigIntegerField() {
        // transform: boolean field to big integer field
        List<BigInteger> expectedBigIntegers = new ArrayList<>();
        for (boolean bool : rawBooleanValues) {
            expectedBigIntegers.add(BigInteger.valueOf(BooleanField.toLong(bool)));
        }
        Field<BigInteger> toBigIntegerField = booleanField.as(BigIntegerField.BigInteger);
        assertEquals(expectedBigIntegers, toBigIntegerField.getValues());
        assertEquals(expectedBigIntegers.get(0), toBigIntegerField.getValue(null)); // default value ignored
        assertEquals(expectedBigIntegers.get(0).longValue(), toBigIntegerField.getLong(10)); // default value ignored
        assertEquals(expectedBigIntegers.get(0).doubleValue(), toBigIntegerField.getDouble(10.0d), 0.1d); // default value ignored
    }

    public void testBooleanFieldToLongField() {
        // transform: boolean field to long field
        List<Long> expectedLongs = new ArrayList<>();
        for (boolean bool : rawBooleanValues) {
            expectedLongs.add(BooleanField.toLong(bool));
        }
        Field<Long> toLongField = booleanField.as(LongField.Long);
        assertEquals(expectedLongs, toLongField.getValues());
        assertEquals(expectedLongs.get(0), toLongField.getValue(null)); // default value ignored
        assertEquals(expectedLongs.get(0).longValue(), toLongField.getLong(10)); // default value ignored
        assertEquals(expectedLongs.get(0).doubleValue(), toLongField.getDouble(10.0d), 0.1d); // default value ignored
    }

    public void testDateMillisFieldToBigIntegerField() {
        // transform: boolean field to big integer field
        List<BigInteger> expectedBigIntegers =
                rawDateMillisValues.stream().map(DateMillisField::toLong).map(BigInteger::valueOf).collect(Collectors.toList());
        Field<BigInteger> toBigIntegerField = dateMillisField.as(BigIntegerField.BigInteger);
        assertEquals(expectedBigIntegers, toBigIntegerField.getValues());
        assertEquals(expectedBigIntegers.get(0), toBigIntegerField.getValue(null)); // default value ignored
        assertEquals(expectedBigIntegers.get(0).longValue(), toBigIntegerField.getLong(10)); // default value ignored
        assertEquals(expectedBigIntegers.get(0).doubleValue(), toBigIntegerField.getDouble(10.0d), 0.1d); // default value ignored
    }

    public void testDateMillisFieldToLongField() {
        // transform: boolean field to long field
        List<Long> expectedLongs = rawDateMillisValues.stream().map(DateMillisField::toLong).collect(Collectors.toList());
        Field<Long> toLongField = dateMillisField.as(LongField.Long);
        assertEquals(expectedLongs, toLongField.getValues());
        assertEquals(expectedLongs.get(0), toLongField.getValue(null)); // default value ignored
        assertEquals(expectedLongs.get(0).longValue(), toLongField.getLong(10)); // default value ignored
        assertEquals(expectedLongs.get(0).doubleValue(), toLongField.getDouble(10.0d), 0.1d); // default value ignored
    }

    public void testDateNanosFieldToBigIntegerField() {
        // transform: boolean field to big integer field
        List<BigInteger> expectedBigIntegers =
                rawDateNanosValues.stream().map(DateNanosField::toLong).map(BigInteger::valueOf).collect(Collectors.toList());
        Field<BigInteger> toBigIntegerField = dateNanosField.as(BigIntegerField.BigInteger);
        assertEquals(expectedBigIntegers, toBigIntegerField.getValues());
        assertEquals(expectedBigIntegers.get(0), toBigIntegerField.getValue(null)); // default value ignored
        assertEquals(expectedBigIntegers.get(0).longValue(), toBigIntegerField.getLong(10)); // default value ignored
        assertEquals(expectedBigIntegers.get(0).doubleValue(), toBigIntegerField.getDouble(10.0d), 0.1d); // default value ignored
    }

    public void testDateNanosFieldToLongField() {
        // transform: boolean field to long field
        List<Long> expectedLongs = rawDateNanosValues.stream().map(DateNanosField::toLong).collect(Collectors.toList());
        Field<Long> toLongField = dateNanosField.as(LongField.Long);
        assertEquals(expectedLongs, toLongField.getValues());
        assertEquals(expectedLongs.get(0), toLongField.getValue(null)); // default value ignored
        assertEquals(expectedLongs.get(0).longValue(), toLongField.getLong(10)); // default value ignored
        assertEquals(expectedLongs.get(0).doubleValue(), toLongField.getDouble(10.0d), 0.1d); // default value ignored
    }
}
