/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.support.filtering;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterPathTests extends ESTestCase {
    public void testHasNoDoubleWildcard() {
        Set<String> filters = new HashSet<>();
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            String item = randomList(1, 5, () -> randomAlphaOfLength(5)).stream().collect(Collectors.joining("."));
            filters.add(item);
        }

        FilterPath[] filterPaths = FilterPath.compile(filters);
        assertEquals(filterPaths.length, 1);
        assertFalse(filterPaths[0].hasDoubleWildcard());
    }

    public void testHasDoubleWildcard() {
        Set<String> filters = new HashSet<>();
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            String item = randomList(1, 5, () -> randomAlphaOfLength(5)).stream().collect(Collectors.joining("."));
            filters.add(item);
        }

        String item = randomList(1, 5, () -> randomAlphaOfLength(5)).stream().collect(Collectors.joining("."));
        filters.add(item + ".test**doubleWildcard");

        FilterPath[] filterPaths = FilterPath.compile(filters);
        assertEquals(filterPaths.length, 1);
        assertTrue(filterPaths[0].hasDoubleWildcard());
    }

    public void testNoMatchesFilter() {
        Set<String> filters = new HashSet<>();
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            String item = randomList(1, 5, () -> randomAlphaOfLength(5)).stream().collect(Collectors.joining("."));
            filters.add(item);
        }

        FilterPath[] filterPaths = FilterPath.compile(filters);
        assertEquals(filterPaths.length, 1);

        List<FilterPath> nextFilters = new ArrayList<>();
        FilterPath filterPath = filterPaths[0];
        assertFalse(filterPath.matches(randomAlphaOfLength(10), nextFilters));
        assertEquals(nextFilters.size(), 0);
    }
}
