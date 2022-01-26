/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.script.field.DocValuesField;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.search.DocValueFormat.MASK_2_63;
import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.BIGINTEGER_2_64_MINUS_ONE;

public class UnsignedLongDocValuesField implements UnsignedLongField, DocValuesField {

    private final SortedNumericDocValues input;
    private long[] values = new long[0];
    private int count;

    private final String name;

    public UnsignedLongDocValuesField(SortedNumericDocValues input, String name) {
        this.input = input;
        this.name = name;
    }

    @Override
    public void setNextDocId(int docId) throws IOException {
        if (input.advanceExact(docId)) {
            resize(input.docValueCount());
            for (int i = 0; i < count; i++) {
                values[i] = input.nextValue();
            }
        } else {
            resize(0);
        }
    }

    protected void resize(int newSize) {
        count = newSize;
        values = ArrayUtil.grow(values, count);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    @Override
    public int size() {
        return count;
    }

    /**
     * Applies the formatting from {@link org.elasticsearch.search.DocValueFormat.UnsignedLongShiftedDocValueFormat#format(long)} so
     * that the underlying value can be treated as a primitive long as that method returns either a {@code long} or a {@code BigInteger}.
     */
    protected long toFormatted(int index) {
        return values[index] ^ MASK_2_63;
    }

    @Override
    public List<Long> getValues() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<Long> longValues = new ArrayList<>(count);

        for (int index = 0; index < count; ++index) {
            longValues.add(toFormatted(index));
        }

        return longValues;
    }

    @Override
    public long getValue(long defaultValue) {
        return getValue(0, defaultValue);
    }

    @Override
    public long getValue(int index, long defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return toFormatted(index);
    }

    protected BigInteger toBigInteger(int index) {
        return BigInteger.valueOf(toFormatted(index)).and(BIGINTEGER_2_64_MINUS_ONE);
    }

    @Override
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

    @Override
    public BigInteger asBigInteger(BigInteger defaultValue) {
        return asBigInteger(0, defaultValue);
    }

    @Override
    public BigInteger asBigInteger(int index, BigInteger defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return toBigInteger(index);
    }
}
