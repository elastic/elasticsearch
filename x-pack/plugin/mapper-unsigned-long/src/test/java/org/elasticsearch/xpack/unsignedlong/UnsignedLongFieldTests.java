/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.script.Field;
import org.elasticsearch.script.FieldValues;
import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class UnsignedLongFieldTests extends ESTestCase {
    static final BigInteger[] VALUES = {
            BigInteger.valueOf(0L),
            new BigInteger("18446744073709551615"), // 2^64 - 1
            new BigInteger("9223372036854775807"),  // 2^63 - 1
            new BigInteger("9223372036854775808"),  // 2^63
            new BigInteger("9223372036854775809"),  // 2^63 + 1
    };

    UnsignedLongField FIELD = new UnsignedLongField("test", new FieldValues<Long>() {
        protected long[] values = Arrays.stream(UnsignedLongFieldTests.VALUES).mapToLong(BigInteger::longValue).toArray();

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

    public void testLongValues() {
        long[] expected = {0L, -1L, 9223372036854775807L, -9223372036854775808L, -9223372036854775807L};
        List<Long> asLong = Arrays.stream(expected).boxed().collect(Collectors.toList());
        assertEquals(asLong, FIELD.convert(Field.Long).getValues());
    }
}
