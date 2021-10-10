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

public interface UnsignedLongField extends Field<Long> {

    long getLong(long defaultValue);

    long getLong(int index, long defaultValue);

    BigInteger getBigInteger(BigInteger defaultValue);

    BigInteger getBigInteger(int index, BigInteger defaultValue);

    List<BigInteger> getBigIntegers();
}
