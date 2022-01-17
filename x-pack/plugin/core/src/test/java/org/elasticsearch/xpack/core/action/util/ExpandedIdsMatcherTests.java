/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action.util;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ExpandedIdsMatcherTests extends ESTestCase {

    public void testMatchingResourceIds() {
        ExpandedIdsMatcher matcher = new ExpandedIdsMatcher(new String[] { "*" }, false);
        assertThat(matcher.unmatchedIds(), hasSize(1));
        assertTrue(matcher.hasUnmatchedIds());
        matcher.filterMatchedIds(Collections.singletonList("foo"));
        assertFalse(matcher.hasUnmatchedIds());
        assertThat(matcher.unmatchedIds(), empty());
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher("", false);
        assertThat(matcher.unmatchedIds(), hasSize(1));
        matcher.filterMatchedIds(Collections.singletonList("foo"));
        assertThat(matcher.unmatchedIds(), empty());
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher(ExpandedIdsMatcher.tokenizeExpression(null), false);
        assertThat(matcher.unmatchedIds(), hasSize(1));
        matcher.filterMatchedIds(Collections.singletonList("foo"));
        assertThat(matcher.unmatchedIds(), empty());
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher(ExpandedIdsMatcher.tokenizeExpression(null), false);
        assertThat(matcher.unmatchedIds(), hasSize(1));
        matcher.filterMatchedIds(Collections.emptyList());
        assertThat(matcher.unmatchedIds(), hasSize(1));
        assertThat(matcher.unmatchedIds().get(0), equalTo("*"));
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher("_all", false);
        assertThat(matcher.unmatchedIds(), hasSize(1));
        matcher.filterMatchedIds(Collections.singletonList("foo"));
        assertThat(matcher.unmatchedIds(), empty());
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher(new String[] { "foo*" }, false);
        assertThat(matcher.unmatchedIds(), hasSize(1));
        matcher.filterMatchedIds(Arrays.asList("foo1", "foo2"));
        assertThat(matcher.unmatchedIds(), empty());
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("foo1"));
        assertFalse(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher(new String[] { "foo*", "bar" }, false);
        assertThat(matcher.unmatchedIds(), hasSize(2));
        matcher.filterMatchedIds(Arrays.asList("foo1", "foo2"));
        assertThat(matcher.unmatchedIds(), hasSize(1));
        assertEquals("bar", matcher.unmatchedIds().get(0));
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("foo1"));
        assertTrue(matcher.idMatches("bar"));
        assertFalse(matcher.idMatches("bar1"));

        matcher = new ExpandedIdsMatcher(new String[] { "foo*", "bar" }, false);
        assertThat(matcher.unmatchedIds(), hasSize(2));
        matcher.filterMatchedIds(Arrays.asList("foo1", "bar"));
        assertFalse(matcher.hasUnmatchedIds());
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("foo1"));
        assertTrue(matcher.idMatches("bar"));
        assertFalse(matcher.idMatches("bar1"));

        matcher = new ExpandedIdsMatcher(new String[] { "foo*", "bar" }, false);
        assertThat(matcher.unmatchedIds(), hasSize(2));
        matcher.filterMatchedIds(Collections.singletonList("bar"));
        assertThat(matcher.unmatchedIds(), hasSize(1));
        assertEquals("foo*", matcher.unmatchedIds().get(0));
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("foo1"));
        assertTrue(matcher.idMatches("bar"));
        assertFalse(matcher.idMatches("bar1"));

        matcher = new ExpandedIdsMatcher("foo,bar,baz,wild*", false);
        assertThat(matcher.unmatchedIds(), hasSize(4));
        matcher.filterMatchedIds(Arrays.asList("foo", "baz"));
        assertThat(matcher.unmatchedIds(), hasSize(2));
        assertThat(matcher.unmatchedIds().get(0), is(oneOf("bar", "wild*")));
        assertThat(matcher.unmatchedIds().get(1), is(oneOf("bar", "wild*")));
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertFalse(matcher.idMatches("foo1"));
        assertTrue(matcher.idMatches("bar"));
        assertTrue(matcher.idMatches("wild"));
        assertTrue(matcher.idMatches("wild1"));

        matcher = new ExpandedIdsMatcher(new String[] { "foo", "bar" }, false);
        assertThat(matcher.unmatchedIds(), hasSize(2));
        matcher.filterMatchedIds(Collections.singletonList("bar"));
        assertThat(matcher.unmatchedIds(), hasSize(1));
        assertEquals("foo", matcher.unmatchedIds().get(0));
        assertTrue(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertFalse(matcher.idMatches("foo1"));
        assertTrue(matcher.idMatches("bar"));
    }

    public void testMatchingResourceIds_allowNoMatch() {
        ExpandedIdsMatcher matcher = new ExpandedIdsMatcher(new String[] { "*" }, true);
        assertThat(matcher.unmatchedIds(), empty());
        assertFalse(matcher.hasUnmatchedIds());
        matcher.filterMatchedIds(Collections.emptyList());
        assertThat(matcher.unmatchedIds(), empty());
        assertFalse(matcher.hasUnmatchedIds());
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher(new String[] { "foo*", "bar" }, true);
        assertThat(matcher.unmatchedIds(), hasSize(1));
        assertTrue(matcher.hasUnmatchedIds());
        matcher.filterMatchedIds(Collections.singletonList("bar"));
        assertThat(matcher.unmatchedIds(), empty());
        assertFalse(matcher.hasUnmatchedIds());
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("foo1"));
        assertTrue(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher(new String[] { "foo*", "bar" }, true);
        assertThat(matcher.unmatchedIds(), hasSize(1));
        matcher.filterMatchedIds(Collections.emptyList());
        assertThat(matcher.unmatchedIds(), hasSize(1));
        assertEquals("bar", matcher.unmatchedIds().get(0));
        assertFalse(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertTrue(matcher.idMatches("foo1"));
        assertTrue(matcher.idMatches("bar"));

        matcher = new ExpandedIdsMatcher(new String[] { "foo", "bar" }, true);
        assertThat(matcher.unmatchedIds(), hasSize(2));
        matcher.filterMatchedIds(Collections.singletonList("bar"));
        assertThat(matcher.unmatchedIds(), hasSize(1));
        assertEquals("foo", matcher.unmatchedIds().get(0));
        assertTrue(matcher.isOnlyExact());
        assertTrue(matcher.idMatches("foo"));
        assertFalse(matcher.idMatches("foo1"));
        assertTrue(matcher.idMatches("bar"));
    }

    public void testSimpleMatcher() {
        {
            ExpandedIdsMatcher.SimpleIdsMatcher matcher = new ExpandedIdsMatcher.SimpleIdsMatcher(new String[] { "*" });
            assertTrue(matcher.idMatches(randomAlphaOfLength(5)));
        }
        {
            ExpandedIdsMatcher.SimpleIdsMatcher matcher = new ExpandedIdsMatcher.SimpleIdsMatcher(new String[] { "foo*", "bar" });
            assertTrue(matcher.idMatches("foo1"));
            assertTrue(matcher.idMatches("bar"));
            assertFalse(matcher.idMatches("car"));
        }
    }
}
