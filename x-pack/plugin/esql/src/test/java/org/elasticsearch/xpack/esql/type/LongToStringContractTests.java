/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToString;

/**
 * Pins the byte-equality contract of {@link EsqlDataTypeConverter#longToString(long)}.
 * Every value must produce bytes identical to {@code new BytesRef(String.valueOf(lng))}.
 */
public class LongToStringContractTests extends ESTestCase {

    public void testEdgeCases() {
        checkAll(
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

    public void testRandomValues() {
        for (int i = 0; i < 1000; i++) {
            checkAll(randomLong());
        }
    }

    private void checkAll(long... values) {
        for (long lng : values) {
            assertEquals("long=" + lng, new BytesRef(String.valueOf(lng)), longToString(lng));
        }
    }
}
