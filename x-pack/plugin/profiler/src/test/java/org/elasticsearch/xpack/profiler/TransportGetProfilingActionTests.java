/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

public class TransportGetProfilingActionTests extends ESTestCase {
    public void testSliceEmptyList() {
        assertEquals(List.of(List.of()), TransportGetProfilingAction.sliced(Collections.emptyList(), 4));
    }

    public void testSliceListSmallerOrEqualToSliceCount() {
        int slices = 7;
        List<String> input = randomList(0, slices, () -> randomAlphaOfLength(3));
        List<List<String>> sliced = TransportGetProfilingAction.sliced(input, slices);
        assertEquals(1, sliced.size());
        assertEquals(input, sliced.get(0));
    }

    public void testSliceListMultipleOfSliceCount() {
        int slices = 2;
        List<String> input = List.of("a", "b", "c", "d");
        List<List<String>> sliced = TransportGetProfilingAction.sliced(input, slices);
        assertEquals(slices, sliced.size());
        assertEquals(List.of("a", "b"), sliced.get(0));
        assertEquals(List.of("c", "d"), sliced.get(1));
    }

    public void testSliceListGreaterThanSliceCount() {
        int slices = 3;
        List<String> input = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
        List<List<String>> sliced = TransportGetProfilingAction.sliced(input, slices);
        assertEquals(slices, sliced.size());
        assertEquals(List.of("a", "b", "c", "d"), sliced.get(0));
        assertEquals(List.of("e", "f", "g", "h"), sliced.get(1));
        assertEquals(List.of("i", "j"), sliced.get(2));
    }
}
