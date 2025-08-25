/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StringToIntSetAndMapTests extends ESTestCase {

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
    public static final Map<String, Integer> MONTH_MAP = IntStream.range(0, MONTHS.length)
        .boxed()
        .collect(Collectors.toMap(i -> MONTHS[i], i -> i + 1));

    public void testSet() {
        Set<String> months = Set.of(MONTHS);
        StringToIntSet monthSet = new StringToIntSet(months);
        String testString = "I love the month of July!";
        SubstringView input = new SubstringView(testString);
        assertEquals("Value should be -1 for " + input + " in " + testString, -1, monthSet.applyAsInt(input));
        int indexOfJul = testString.indexOf("Jul");
        input.set(testString, indexOfJul, indexOfJul + 3);
        assertEquals("Value should be 1 for " + input + " in " + testString, 1, monthSet.applyAsInt(input));
    }

    // same as the above but for a month Map with StringMapToIntMapper
    public void testMap() {
        SubstringToIntMap monthToIntMap = new SubstringToIntMap(MONTH_MAP);
        String testString = "I love the month of July!";
        SubstringView input = new SubstringView(testString);
        assertEquals("Value should be -1 for " + input + " in " + testString, -1, monthToIntMap.applyAsInt(input));
        int indexOfJul = testString.indexOf("Jul");
        input.set(testString, indexOfJul, indexOfJul + 3);
        assertEquals("Value should be 7 for " + input + " in " + testString, 7, monthToIntMap.applyAsInt(input));
    }

    public void testEmptyString() {
        Set<String> set = Set.of("", "non-empty");
        StringToIntSet setMapper = new StringToIntSet(set);
        String testString = "Test string";
        SubstringView input = new SubstringView(testString);
        assertEquals(-1, setMapper.applyAsInt(input));
        input.set(testString, 4, 4);
        assertEquals("Empty string should match", 1, setMapper.applyAsInt(input));
    }

    public void testAllTrueValues() {
        SubstringToIntMap monthMap = new SubstringToIntMap(MONTH_MAP);
        String testString = "I love the months Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec!";
        SubstringView input = new SubstringView(testString);
        for (int i = 0; i < MONTHS.length; i++) {
            String month = MONTHS[i];
            int startIndex = testString.indexOf(month);
            input.set(startIndex, startIndex + month.length());
            assertEquals("Value should match for " + input + " in " + testString, i + 1, monthMap.applyAsInt(input));
        }
        input.set(1, 5);
        assertEquals("Value should be -1 for non-matching substring " + input + " in " + testString, -1, monthMap.applyAsInt(input));
    }
}
