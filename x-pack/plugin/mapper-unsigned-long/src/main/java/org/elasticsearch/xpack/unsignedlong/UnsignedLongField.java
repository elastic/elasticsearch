/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.elasticsearch.script.field.Field;

import java.math.BigInteger;
import java.util.List;

public interface UnsignedLongField extends Field {

    /** Return all the values as a {@code List}. */
    List<Long> getValues();

    /** Returns the 0th index value as an {@code long} if it exists, otherwise {@code defaultValue}. */
    long getValue(long defaultValue);

    /** Returns the value at {@code index} as an {@code long} if it exists, otherwise {@code defaultValue}. */
    long getValue(int index, long defaultValue);

    /** Converts all the values to {@code BigInteger} and returns them as a {@code List}. */
    List<BigInteger> asBigIntegers();

    /** Returns the 0th index value as a {@code BigInteger} if it exists, otherwise {@code defaultValue}. */
    BigInteger asBigInteger(BigInteger defaultValue);

    /** Returns the value at {@code index} as a {@code BigInteger} if it exists, otherwise {@code defaultValue}. */
    BigInteger asBigInteger(int index, BigInteger defaultValue);
}
