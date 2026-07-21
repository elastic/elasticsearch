/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datatree;

import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class DataValueTests extends ESTestCase {

    public void testOfNullStringIsNull() {
        assertThat(DataValue.of((String) null), sameInstance(DataNull.INSTANCE));
        assertThat(DataValue.nullValue(), sameInstance(DataNull.INSTANCE));
    }

    public void testOfLongAndDouble() {
        assertThat(DataValue.of(42L), equalTo(new DataLong(42L)));
        assertThat(DataValue.of(1.5d), equalTo(new DataDouble(1.5d)));
    }

    public void testOfNonFiniteDoubleRejected() {
        expectThrows(IllegalArgumentException.class, () -> DataValue.of(Double.NaN));
        expectThrows(IllegalArgumentException.class, () -> DataValue.of(Double.POSITIVE_INFINITY));
    }

    public void testOfBigIntegerFitsOrKeepsArbitraryPrecision() {
        assertThat(DataValue.of(BigInteger.valueOf(Long.MAX_VALUE)), equalTo(new DataLong(Long.MAX_VALUE)));
        assertThat(DataValue.of(BigInteger.valueOf(Long.MIN_VALUE)), equalTo(new DataLong(Long.MIN_VALUE)));
        BigInteger beyondLong = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        assertThat(DataValue.of(beyondLong), equalTo(new DataInteger(beyondLong)));
    }

    public void testOfBigDecimalFitsOrKeepsArbitraryPrecision() {
        assertThat(DataValue.of(new BigDecimal("3.25")), equalTo(new DataDouble(3.25d)));
        BigDecimal beyondDouble = new BigDecimal("3.14159265358979323846");
        assertThat(DataValue.of(beyondDouble), equalTo(new DataDecimal(beyondDouble)));
    }

    public void testRequireTypeMismatchThrows() {
        DataValue string = new DataString("x");
        expectThrows(IllegalStateException.class, string::requireObject);
        expectThrows(IllegalStateException.class, string::requireArray);
        expectThrows(IllegalStateException.class, DataNull.INSTANCE::requireString);
    }
}
