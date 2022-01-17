/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.field.AbstractLongDocValuesField;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.search.DocValueFormat.MASK_2_63;
import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.BIGINTEGER_2_64_MINUS_ONE;

public class UnsignedLongDocValuesField extends AbstractLongDocValuesField {

    public UnsignedLongDocValuesField(SortedNumericDocValues input, String name) {
        super(input, name);
    }

    @Override
    public ScriptDocValues<?> newScriptDocValues() {
        return new UnsignedLongScriptDocValues(this);
    }

    /**
     * Applies the formatting from {@link org.elasticsearch.search.DocValueFormat.UnsignedLongShiftedDocValueFormat#format(long)} so
     * that the underlying value can be treated as a primitive long as that method returns either a {@code long} or a {@code BigInteger}.
     */
    @Override
    protected long formatLong(long raw) {
        return raw ^ MASK_2_63;
    }

    /** Return all the values as a {@code List}. */
    public List<Long> getValues() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<Long> longValues = new ArrayList<>(count);

        for (int index = 0; index < count; ++index) {
            longValues.add(getLong(index));
        }

        return longValues;
    }

    /** Returns the 0th index value as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long getValue(long defaultValue) {
        return get(0, defaultValue);
    }

    /** Returns the value at {@code index} as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long getValue(int index, long defaultValue) {
        return get(index, defaultValue);
    }

    protected BigInteger toBigInteger(int index) {
        return BigInteger.valueOf(getLong(index)).and(BIGINTEGER_2_64_MINUS_ONE);
    }

    /** Converts all the values to {@code BigInteger} and returns them as a {@code List}. */
    public List<BigInteger> asBigIntegers() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<BigInteger> bigIntegerValues = new ArrayList<>(count);

        for (int index = 0; index < count; ++index) {
            bigIntegerValues.add(toBigInteger(index));
        }

        return bigIntegerValues;
    }

    /** Returns the 0th index value as a {@code BigInteger} if it exists, otherwise {@code defaultValue}. */
    public BigInteger asBigInteger(BigInteger defaultValue) {
        return asBigInteger(0, defaultValue);
    }

    /** Returns the value at {@code index} as a {@code BigInteger} if it exists, otherwise {@code defaultValue}. */
    public BigInteger asBigInteger(int index, BigInteger defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return toBigInteger(index);
    }
}
