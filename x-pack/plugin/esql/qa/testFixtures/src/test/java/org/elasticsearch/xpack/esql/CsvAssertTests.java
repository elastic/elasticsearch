/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Rows that tie on the first value of a multi-value cell must sort the same way whatever order they arrive in.
 */
public class CsvAssertTests extends ESTestCase {

    private static final String SINGLE = "fe80::cae2:65ff:fece:feb9";
    private static final List<String> LONG_LIST = List.of(SINGLE, "fe80::cae2:65ff:fece:fec0", "fe80::cae2:65ff:fece:fec1");
    private static final List<String> OTHER_LIST = List.of(SINGLE, "fe80::cae2:65ff:fece:fec2");

    public void testIgnoreOrderIgnoresArrivalOrder() {
        List<List<Object>> rows = List.of(List.of(SINGLE), List.of(LONG_LIST), List.of(OTHER_LIST));

        for (int attempt = 0; attempt < 10; attempt++) {
            List<List<Object>> actual = new ArrayList<>(rows);
            Collections.shuffle(actual, random());
            List<List<Object>> expectedRows = new ArrayList<>();
            for (List<Object> row : rows) {
                expectedRows.add(List.of(toExpected(row.get(0))));
            }
            ExpectedResults expected = new ExpectedResults(List.of("ip0"), List.of(Type.IP), expectedRows);
            CsvAssert.assertData(expected, actual, true, null, (type, value) -> value);
        }
    }

    private static Object toExpected(Object value) {
        if (value instanceof List<?> list) {
            return list.stream().map(v -> Type.IP.convert((String) v)).toList();
        }
        return Type.IP.convert((String) value);
    }
}
