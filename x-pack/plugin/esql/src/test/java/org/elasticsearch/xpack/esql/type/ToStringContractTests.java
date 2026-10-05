/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.booleanToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.intToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.unsignedLongToString;

/**
 * Pins the byte-equality contracts of the direct-ASCII conversion helpers in
 * {@link EsqlDataTypeConverter}.  Each method must produce bytes identical to
 * the reference expression shown in its section.
 */
public class ToStringContractTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // booleanToString — reference: new BytesRef(String.valueOf(b))
    // -----------------------------------------------------------------------

    public void testBooleanToString() {
        assertEquals(new BytesRef("true"), booleanToString(true));
        assertEquals(new BytesRef("false"), booleanToString(false));
        // returned objects are interned constants — same reference on repeated calls
        assertSame(booleanToString(true), booleanToString(true));
        assertSame(booleanToString(false), booleanToString(false));
    }

    // -----------------------------------------------------------------------
    // intToString — reference: new BytesRef(String.valueOf(i))
    // -----------------------------------------------------------------------

    public void testIntEdgeCases() {
        checkInt(
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            0,
            -1,
            1,
            -9,
            9,
            -10,
            10,
            -99,
            99,
            -100,
            100,
            Integer.MIN_VALUE + 1,
            Integer.MAX_VALUE - 1
        );
    }

    public void testIntRandomValues() {
        for (int i = 0; i < 1000; i++) {
            checkInt(randomInt());
        }
    }

    private void checkInt(int... values) {
        for (int i : values) {
            assertEquals("int=" + i, new BytesRef(String.valueOf(i)), intToString(i));
        }
    }

    // -----------------------------------------------------------------------
    // longToString — reference: new BytesRef(String.valueOf(lng))
    // -----------------------------------------------------------------------

    public void testLongEdgeCases() {
        checkLong(
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0L,
            -1L,
            1L,
            -9L,
            9L,
            -10L,
            10L,
            -99L,
            99L,
            -100L,
            100L,
            Long.MIN_VALUE + 1,
            Long.MAX_VALUE - 1
        );
    }

    public void testLongRandomValues() {
        for (int i = 0; i < 1000; i++) {
            checkLong(randomLong());
        }
    }

    private void checkLong(long... values) {
        for (long lng : values) {
            assertEquals("long=" + lng, new BytesRef(String.valueOf(lng)), longToString(lng));
        }
    }

    // -----------------------------------------------------------------------
    // unsignedLongToString — reference: new BytesRef(unsignedLongAsNumber(stored).toString())
    //
    // ES|QL stores unsigned longs as (unsigned_value ^ Long.MIN_VALUE).
    // stored < 0 → fast path (unsigned_value ≤ Long.MAX_VALUE)
    // stored >= 0 → slow path (unsigned_value > Long.MAX_VALUE, needs BigInteger)
    // -----------------------------------------------------------------------

    public void testUnsignedLongEdgeCases() {
        // fast path: stored < 0 → unsigned value in [0, Long.MAX_VALUE]
        checkUnsignedLong(
            Long.MIN_VALUE,      // unsigned = 0
            Long.MIN_VALUE + 1,  // unsigned = 1
            -1L                  // unsigned = Long.MAX_VALUE
        );
        // slow path: stored >= 0 → unsigned value in (Long.MAX_VALUE, 2^64-1]
        checkUnsignedLong(
            0L,                  // unsigned = Long.MIN_VALUE as unsigned (2^63)
            1L,                  // unsigned = 2^63 + 1
            Long.MAX_VALUE       // unsigned = 2^64 - 1
        );
    }

    public void testUnsignedLongRandomValues() {
        for (int i = 0; i < 1000; i++) {
            checkUnsignedLong(randomLong());
        }
    }

    private void checkUnsignedLong(long... storedValues) {
        for (long stored : storedValues) {
            BytesRef expected = new BytesRef(NumericUtils.unsignedLongAsNumber(stored).toString());
            assertEquals("unsignedLong stored=" + stored, expected, unsignedLongToString(stored));
        }
    }
}
