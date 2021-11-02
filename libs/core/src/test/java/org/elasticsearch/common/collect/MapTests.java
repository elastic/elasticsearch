/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.core.Map;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class MapTests extends ESTestCase {

    private static final String[] numbers = { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine" };

    public void testMapOfZero() {
        final java.util.Map<String, Integer> map = Map.of();
        validateMapContents(map, 0);
    }

    public void testMapOfOne() {
        final java.util.Map<String, Integer> map = Map.of(numbers[0], 0);
        validateMapContents(map, 1);
    }

    public void testMapOfTwo() {
        final java.util.Map<String, Integer> map = Map.of(numbers[0], 0, numbers[1], 1);
        validateMapContents(map, 2);
    }

    public void testMapOfThree() {
        final java.util.Map<String, Integer> map = Map.of(numbers[0], 0, numbers[1], 1, numbers[2], 2);
        validateMapContents(map, 3);
    }

    public void testMapOfFour() {
        final java.util.Map<String, Integer> map = Map.of(numbers[0], 0, numbers[1], 1, numbers[2], 2, numbers[3], 3);
        validateMapContents(map, 4);
    }

    public void testMapOfFive() {
        final java.util.Map<String, Integer> map = Map.of(numbers[0], 0, numbers[1], 1, numbers[2], 2, numbers[3], 3, numbers[4], 4);
        validateMapContents(map, 5);
    }

    public void testMapOfSix() {
        final java.util.Map<String, Integer> map = Map.of(
            numbers[0],
            0,
            numbers[1],
            1,
            numbers[2],
            2,
            numbers[3],
            3,
            numbers[4],
            4,
            numbers[5],
            5
        );
        validateMapContents(map, 6);
    }

    public void testMapOfSeven() {
        final java.util.Map<String, Integer> map = Map.of(
            numbers[0],
            0,
            numbers[1],
            1,
            numbers[2],
            2,
            numbers[3],
            3,
            numbers[4],
            4,
            numbers[5],
            5,
            numbers[6],
            6
        );
        validateMapContents(map, 7);
    }

    public void testMapOfEight() {
        final java.util.Map<String, Integer> map = Map.of(
            numbers[0],
            0,
            numbers[1],
            1,
            numbers[2],
            2,
            numbers[3],
            3,
            numbers[4],
            4,
            numbers[5],
            5,
            numbers[6],
            6,
            numbers[7],
            7
        );
        validateMapContents(map, 8);
    }

    public void testMapOfNine() {
        final java.util.Map<String, Integer> map = Map.of(
            numbers[0],
            0,
            numbers[1],
            1,
            numbers[2],
            2,
            numbers[3],
            3,
            numbers[4],
            4,
            numbers[5],
            5,
            numbers[6],
            6,
            numbers[7],
            7,
            numbers[8],
            8
        );
        validateMapContents(map, 9);
    }

    public void testMapOfTen() {
        final java.util.Map<String, Integer> map = Map.of(
            numbers[0],
            0,
            numbers[1],
            1,
            numbers[2],
            2,
            numbers[3],
            3,
            numbers[4],
            4,
            numbers[5],
            5,
            numbers[6],
            6,
            numbers[7],
            7,
            numbers[8],
            8,
            numbers[9],
            9
        );
        validateMapContents(map, 10);
    }

    private static void validateMapContents(java.util.Map<String, Integer> map, int size) {
        assertThat(map.size(), equalTo(size));
        for (int k = 0; k < map.size(); k++) {
            assertEquals(Integer.class, map.get(numbers[k]).getClass());
            assertThat(k, equalTo(map.get(numbers[k])));
        }
        expectThrows(UnsupportedOperationException.class, () -> map.put("foo", 42));
    }

    public void testOfEntries() {
        final java.util.Map<String, Integer> map = Map.ofEntries(
            Map.entry(numbers[0], 0),
            Map.entry(numbers[1], 1),
            Map.entry(numbers[2], 2)
        );
        validateMapContents(map, 3);
    }

    public void testCopyOf() {
        final java.util.Map<String, String> map1 = Map.of("fooK", "fooV", "barK", "barV", "bazK", "bazV");
        final java.util.Map<String, String> copy = Map.copyOf(map1);
        assertThat(map1.size(), equalTo(copy.size()));
        for (java.util.Map.Entry<String, String> entry : map1.entrySet()) {
            assertEquals(entry.getValue(), copy.get(entry.getKey()));
        }
        expectThrows(UnsupportedOperationException.class, () -> copy.put("foo", "bar"));
    }
}
