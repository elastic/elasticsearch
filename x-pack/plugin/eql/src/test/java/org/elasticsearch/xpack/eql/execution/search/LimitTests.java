/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static java.util.Arrays.asList;


public class LimitTests extends ESTestCase {

    private final List<Integer> list = asList(1, 2, 3, 4, 5, 6, 7);

    public void testLimitUnderResults() throws Exception {
        assertEquals(asList(1, 2, 3, 4, 5), new Limit(5, 0).view(list));
    }

    public void testLimitWithOffsetEqualResults() throws Exception {
        assertEquals(asList(5, 6, 7), new Limit(3, 4).view(list));
    }

    public void testLimitWithOffsetUnderResults() throws Exception {
        assertEquals(asList(5, 6), new Limit(2, 4).view(list));
    }

    public void testLimitOverResultsNoOffset() throws Exception {
        assertEquals(list, new Limit(8, randomInt(100)).view(list));
    }

    public void testLimitEqualResults() throws Exception {
        assertEquals(list, new Limit(7, randomInt(100)).view(list));
    }

    public void testLimitOverResultsWithHigherOffset() throws Exception {
        assertEquals(asList(6, 7), new Limit(2, 8).view(list));
    }

    public void testLimitOverResultsWithEqualOffset() throws Exception {
        assertEquals(asList(6, 7), new Limit(2, 7).view(list));
    }

    public void testLimitOverResultsWithSmallerOffset() throws Exception {
        assertEquals(asList(3, 4, 5, 6, 7), new Limit(5, 6).view(list));
    }
}
