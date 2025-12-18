/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.test.ESTestCase;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SubstringToIntegerMapTests extends ESTestCase {

    public static final String[] MONTHS = new String[] {
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec" };

    public static final SubstringToIntegerMap MONTH_MAP = SubstringToIntegerMap.builder()
        .addAll(IntStream.range(0, MONTHS.length).boxed().collect(Collectors.toMap(i -> MONTHS[i], i -> i + 1)))
        .build();

    // same as the above but for a month Map with StringMapToIntMapper
    public void testMap() {
        String testString = "I love the month of July!";
        SubstringView input = new SubstringView(testString);
        assertEquals("Value should be 0 for " + input + " in " + testString, 0, MONTH_MAP.applyAsInt(input));
        int indexOfJul = testString.indexOf("Jul");
        input.set(testString, indexOfJul, indexOfJul + 3);
        assertEquals("Value should be 7 for " + input + " in " + testString, 7, MONTH_MAP.applyAsInt(input));
    }

    public void testAllTrueValues() {
        String testString = "I love the months Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec!";
        SubstringView input = new SubstringView(testString);
        for (int i = 0; i < MONTHS.length; i++) {
            String month = MONTHS[i];
            int startIndex = testString.indexOf(month);
            input.set(startIndex, startIndex + month.length());
            assertEquals("Value should match for " + input + " in " + testString, i + 1, MONTH_MAP.applyAsInt(input));
        }
        input.set(1, 5);
        assertEquals("Value should be 0 for non-matching substring " + input + " in " + testString, 0, MONTH_MAP.applyAsInt(input));
    }
}
