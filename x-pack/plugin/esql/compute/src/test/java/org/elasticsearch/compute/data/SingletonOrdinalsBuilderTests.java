/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;

public class SingletonOrdinalsBuilderTests extends ESTestCase {
    public void testCompactWithNulls() {
        assertCompactToUnique(new int[] { -1, -1, -1, -1, 0, 1, 2 }, List.of(0, 1, 2));
    }

    public void testCompactNoNulls() {
        assertCompactToUnique(new int[] { 0, 1, 2 }, List.of(0, 1, 2));
    }

    public void testCompactSkips() {
        assertCompactToUnique(new int[] { 2, 7, 1000 }, List.of(2, 7, 1000));
    }

    private void assertCompactToUnique(int[] sortedOrds, List<Integer> expected) {
        int uniqueLength = SingletonOrdinalsBuilder.compactToUnique(sortedOrds);
        assertMap(Arrays.stream(sortedOrds).mapToObj(Integer::valueOf).limit(uniqueLength).toList(), matchesList(expected));
    }
}
