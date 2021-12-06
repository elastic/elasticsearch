/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.field.DocValuesField;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

import static org.elasticsearch.search.DocValueFormat.MASK_2_63;
import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.BIGINTEGER_2_64_MINUS_ONE;

public class UnsignedLongDocValuesField implements DocValuesField<Long>, ScriptDocValues.Supplier<Long> {

    private final SortedNumericDocValues input;
    private final String name;

    private long[] values = new long[0];
    private int count = 0;

    // used for backwards compatibility for old-style "doc" access
    // as a delegate to this field class
    private UnsignedLongScriptDocValues unsignedLongScriptDocValues = null;

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

    private void resize(int newSize) {
        count = newSize;
        values = ArrayUtil.grow(values, count);
    }

    @Override
    public ScriptDocValues<?> getScriptDocValues() {
        if (unsignedLongScriptDocValues == null) {
            unsignedLongScriptDocValues = new UnsignedLongScriptDocValues(this);
        }

        return unsignedLongScriptDocValues;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isEmpty() {
        return count == 0;
    }

    // this method is required to support the Long return values
    // for the old-style "doc" access in ScriptDocValues
    @Override
    public Long getInternal(int index) {
        return toFormatted(index);
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

    /** Return all the values as a {@code List}. */
    public List<Long> getValues() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<Long> values = new ArrayList<>(count);

        for (int index = 0; index < count; ++index) {
            values.add(toFormatted(index));
        }

        return values;
    }

    /** Returns the 0th index value as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long get(long defaultValue) {
        return get(0, defaultValue);
    }

    /** Returns the value at {@code index} as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long get(int index, long defaultValue) {
        if (isEmpty() || index < 0 || index >= count) {
            return defaultValue;
        }

        return toFormatted(index);
    }

    /** Returns the 0th index value as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long getValue(long defaultValue) {
        return get(0, defaultValue);
    }

    /** Returns the value at {@code index} as an {@code long} if it exists, otherwise {@code defaultValue}. */
    public long getValue(int index, long defaultValue) {
        return get(index, defaultValue);
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {
        return new PrimitiveIterator.OfLong() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Long next() {
                return nextLong();
            }

            @Override
            public long nextLong() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                return toFormatted(index++);
            }
        };
    }

    protected BigInteger toBigInteger(int index) {
        return BigInteger.valueOf(toFormatted(index)).and(BIGINTEGER_2_64_MINUS_ONE);
    }

    /** Converts all the values to {@code BigInteger} and returns them as a {@code List}. */
    public List<BigInteger> asBigIntegers() {
        if (isEmpty()) {
            return Collections.emptyList();
        }

        List<BigInteger> values = new ArrayList<>(count);

        for (int index = 0; index < count; ++index) {
            values.add(toBigInteger(index));
        }

        return values;
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
