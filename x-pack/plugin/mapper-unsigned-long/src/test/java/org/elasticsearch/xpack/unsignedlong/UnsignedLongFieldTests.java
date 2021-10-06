/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.script.field.BigIntegerField;
import org.elasticsearch.script.field.Field;
import org.elasticsearch.script.field.FieldValues;
import org.elasticsearch.script.field.LongField;
import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.elasticsearch.search.DocValueFormat.MASK_2_63;

public class UnsignedLongFieldTests extends ESTestCase {
    static List<BigInteger> VALUES;

    static List<Long> LONG_VALUES;

    protected static UnsignedLongField makeField(List<BigInteger> biValues) {
        return new UnsignedLongField("test", new FieldValues<Long>() {
            protected long[] values = biValues.stream().mapToLong(BigInteger::longValue).toArray();

            @Override
            public boolean isEmpty() {
                return values.length == 0;
            }

            @Override
            public int size() {
                return values.length;
            }

            @Override
            public List<Long> getValues() {
                return Arrays.stream(values).boxed().collect(Collectors.toList());
            }

            @Override
            public Long getNonPrimitiveValue() {
                return UnsignedLongScriptDocValues.shiftedLong(values[0]);
            }

            @Override
            public long getLongValue() {
                return UnsignedLongScriptDocValues.shiftedLong(values[0]);
            }

            @Override
            public double getDoubleValue() {
                return UnsignedLongScriptDocValues.shiftedLong(values[0]);
            }
        });
    }

    public void testLongValues() {
        assertEquals(LONG_VALUES, makeField(VALUES).convert(LongField.Long).getValues());
    }

    public void testUnsignedConverter() {
        Field<Long> thereAndBackAgain = makeField(VALUES).as(BigIntegerField.BigInteger).as(UnsignedLongField.UnsignedLong);
        assertEquals(LONG_VALUES, thereAndBackAgain.getValues());
    }

    public void testLongToUnsignedLong() {
        long[] raw = { Long.MIN_VALUE, Long.MAX_VALUE, ((long) Integer.MIN_VALUE - 1), ((long) Integer.MAX_VALUE + 1), -1L, 0L, 1L };
        Field<Long> src = new LongField("", new FieldValues<Long>() {
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

        Field<Long> dst = src.as(UnsignedLongField.UnsignedLong);
        assertEquals(LongStream.of(raw).map(l -> l ^ MASK_2_63).boxed().collect(Collectors.toList()), dst.getValues());
        assertEquals(Long.valueOf(raw[0] ^ MASK_2_63), dst.getValue(null));
        assertEquals(raw[0] ^ MASK_2_63, dst.getLong(10));
        assertEquals((double) (raw[0] ^ MASK_2_63), dst.getDouble(10.0d), 0.1d);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        long rawRandom = randomLongBetween(0, Long.MAX_VALUE);
        BigInteger rawBigIntegerRandom = BigInteger.valueOf(rawRandom);

        if (randomBoolean()) {
            rawRandom = rawRandom | MASK_2_63;
            rawBigIntegerRandom = rawBigIntegerRandom.add(BigInteger.TWO.pow(63));
        }

        VALUES = List.of(
            rawBigIntegerRandom,
            new BigInteger("18446744073709551615"), // 2^64 - 1
            BigInteger.valueOf(0L),
            new BigInteger("9223372036854775807"),  // 2^63 - 1
            new BigInteger("9223372036854775808"),  // 2^63
            new BigInteger("9223372036854775809")  // 2^63 + 1
        );

        LONG_VALUES = List.of(rawRandom, -1L, 0L, 9223372036854775807L, -9223372036854775808L, -9223372036854775807L);
    }
}
