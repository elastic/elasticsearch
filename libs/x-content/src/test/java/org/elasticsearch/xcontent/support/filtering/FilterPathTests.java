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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;

public class FilterPathTests extends ESTestCase {
    public void testSimpleFilterPath() {
        final String input = "test";

        FilterPath[] filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        List<FilterPath> nextFilters = new ArrayList<>();
        FilterPath filterPath = filterPaths[0];
        assertThat(filterPath.matches("test", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);
    }

    public void testFilterPathWithSubField() {
        final String input = "foo.bar";

        FilterPath[] filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        List<FilterPath> nextFilters = new ArrayList<>();
        FilterPath filterPath = filterPaths[0];
        assertNotNull(filterPath);
        assertThat(filterPath.matches("foo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        assertNotNull(filterPath);
        assertThat(filterPath.matches("bar", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 1);
    }

    public void testFilterPathWithSubFields() {
        final String input = "foo.bar.quz";

        FilterPath[] filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        List<FilterPath> nextFilters = new ArrayList<>();
        FilterPath filterPath = filterPaths[0];
        assertNotNull(filterPath);
        assertThat(filterPath.matches("foo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("bar", nextFilters, false), is(false));

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("quz", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);
    }

    public void testEmptyFilterPath() {
        FilterPath[] filterPaths = FilterPath.compile(singleton(""));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));
    }

    public void testNullFilterPath() {
        FilterPath[] filterPaths = FilterPath.compile(singleton(null));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));
    }

    public void testFilterPathWithEscapedDots() {
        String input = "w.0.0.t";

        FilterPath[] filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        List<FilterPath> nextFilters = new ArrayList<>();
        FilterPath filterPath = filterPaths[0];
        assertNotNull(filterPath);
        assertThat(filterPath.matches("w", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("0", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("0", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("t", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);

        input = "w\\.0\\.0\\.t";

        filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        nextFilters = new ArrayList<>();
        filterPath = filterPaths[0];
        assertTrue(filterPath.matches("w.0.0.t", nextFilters, false));
        assertEquals(nextFilters.size(), 0);

        input = "w\\.0.0\\.t";

        filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        filterPath = filterPaths[0];
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("w.0", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("0.t", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);
    }

    public void testSimpleWildcardFilterPath() {
        FilterPath[] filterPaths = FilterPath.compile(singleton("*"));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        FilterPath filterPath = filterPaths[0];
        List<FilterPath> nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("foo", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);
    }

    public void testWildcardInNameFilterPath() {
        String input = "f*o.bar";

        FilterPath[] filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        FilterPath filterPath = filterPaths[0];
        List<FilterPath> nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("foo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);
        assertThat(filterPath.matches("flo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 2);
        assertThat(filterPath.matches("foooo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 3);
        assertThat(filterPath.matches("boo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 3);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("bar", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);
    }

    public void testDoubleWildcardFilterPath() {
        FilterPath[] filterPaths = FilterPath.compile(singleton("**"));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        FilterPath filterPath = filterPaths[0];
        List<FilterPath> nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("foo", nextFilters, false), is(true));
        assertThat(filterPath.hasDoubleWildcard(), is(true));
        assertEquals(nextFilters.size(), 0);
    }

    public void testStartsWithDoubleWildcardFilterPath() {
        String input = "**.bar";

        FilterPath[] filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        FilterPath filterPath = filterPaths[0];
        List<FilterPath> nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("foo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("bar", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);
    }

    public void testContainsDoubleWildcardFilterPath() {
        String input = "foo.**.bar";

        FilterPath[] filterPaths = FilterPath.compile(singleton(input));
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        FilterPath filterPath = filterPaths[0];
        List<FilterPath> nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("foo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("test", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("bar", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);
    }

    public void testMultipleFilterPaths() {
        Set<String> inputs = new LinkedHashSet<>(Arrays.asList("foo.**.bar.*", "test.dot\\.ted"));

        FilterPath[] filterPaths = FilterPath.compile(inputs);
        assertNotNull(filterPaths);
        assertThat(filterPaths, arrayWithSize(1));

        // foo.**.bar.*
        FilterPath filterPath = filterPaths[0];
        List<FilterPath> nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("foo", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("test", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("bar", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 2);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("test2", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);

        // test.dot\.ted
        filterPath = filterPaths[0];
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("test", nextFilters, false), is(false));
        assertEquals(nextFilters.size(), 1);

        filterPath = nextFilters.get(0);
        nextFilters = new ArrayList<>();
        assertNotNull(filterPath);
        assertThat(filterPath.matches("dot.ted", nextFilters, false), is(true));
        assertEquals(nextFilters.size(), 0);
    }

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
        assertFalse(filterPath.matches(randomAlphaOfLength(10), nextFilters, false));
        assertEquals(nextFilters.size(), 0);
    }

    public void testDotInFieldName() {
        // FilterPath match
        FilterPath[] filterPaths = FilterPath.compile(singleton("foo"));
        List<FilterPath> nextFilters = new ArrayList<>();
        assertTrue(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 0);

        // FilterPath not match
        filterPaths = FilterPath.compile(singleton("bar"));
        assertFalse(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 0);

        // FilterPath equals to fieldName
        filterPaths = FilterPath.compile(singleton("foo.bar"));
        assertTrue(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 0);

        // FilterPath longer than fieldName
        filterPaths = FilterPath.compile(singleton("foo.bar.test"));
        assertFalse(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 1);
        nextFilters.clear();

        // partial match
        filterPaths = FilterPath.compile(singleton("foo.bar.test"));
        assertFalse(filterPaths[0].matches("foo.bar.text", nextFilters, true));
        assertEquals(nextFilters.size(), 0);

        // wildcard
        filterPaths = FilterPath.compile(singleton("*.bar"));
        assertFalse(filterPaths[0].matches("foo", nextFilters, true));
        assertEquals(nextFilters.size(), 1);
        nextFilters.clear();
        filterPaths = FilterPath.compile(singleton("*.bar"));
        assertTrue(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 0);
        filterPaths = FilterPath.compile(singleton("f*.bar"));
        assertTrue(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 0);
        filterPaths = FilterPath.compile(singleton("foo.*"));
        assertTrue(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 0);
        filterPaths = FilterPath.compile(singleton("foo.*ar"));
        assertTrue(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 0);
        filterPaths = FilterPath.compile(singleton("*.*"));
        assertTrue(filterPaths[0].matches("foo.bar", nextFilters, true));
        assertEquals(nextFilters.size(), 0);

        // test double wildcard
        filterPaths = FilterPath.compile(singleton("**.c"));
        assertFalse(filterPaths[0].matches("a.b", nextFilters, true));
        assertEquals(nextFilters.size(), 1);
        nextFilters.clear();
        assertTrue(filterPaths[0].matches("a.c", nextFilters, true));
        assertEquals(nextFilters.size(), 0);
        assertTrue(filterPaths[0].matches("a.b.c", nextFilters, true));
        assertEquals(nextFilters.size(), 0);
        assertTrue(filterPaths[0].matches("a.b.d.c", nextFilters, true));
        assertEquals(nextFilters.size(), 0);
        assertTrue(filterPaths[0].matches("a.b.c.d", nextFilters, true));
        assertEquals(nextFilters.size(), 0);
    }
}
