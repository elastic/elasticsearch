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
        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(new String[] {"*"}, false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        assertTrue(requiredMatches.hasUnmatchedIds());
        requiredMatches.filterMatchedIds(Collections.singletonList("foo"));
        assertFalse(requiredMatches.hasUnmatchedIds());
        assertThat(requiredMatches.unmatchedIds(), empty());
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(ExpandedIdsMatcher.tokenizeExpression(""), false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        requiredMatches.filterMatchedIds(Collections.singletonList("foo"));
        assertThat(requiredMatches.unmatchedIds(), empty());
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(ExpandedIdsMatcher.tokenizeExpression(null), false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        requiredMatches.filterMatchedIds(Collections.singletonList("foo"));
        assertThat(requiredMatches.unmatchedIds(), empty());
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(ExpandedIdsMatcher.tokenizeExpression(null), false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        requiredMatches.filterMatchedIds(Collections.emptyList());
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        assertThat(requiredMatches.unmatchedIds().get(0), equalTo("*"));
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(ExpandedIdsMatcher.tokenizeExpression("_all"), false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        requiredMatches.filterMatchedIds(Collections.singletonList("foo"));
        assertThat(requiredMatches.unmatchedIds(), empty());
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(new String[] {"foo*"}, false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        requiredMatches.filterMatchedIds(Arrays.asList("foo1","foo2"));
        assertThat(requiredMatches.unmatchedIds(), empty());
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(new String[] {"foo*","bar"}, false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(2));
        requiredMatches.filterMatchedIds(Arrays.asList("foo1","foo2"));
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        assertEquals("bar", requiredMatches.unmatchedIds().get(0));
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(new String[] {"foo*","bar"}, false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(2));
        requiredMatches.filterMatchedIds(Arrays.asList("foo1","bar"));
        assertFalse(requiredMatches.hasUnmatchedIds());
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(new String[] {"foo*","bar"}, false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(2));
        requiredMatches.filterMatchedIds(Collections.singletonList("bar"));
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        assertEquals("foo*", requiredMatches.unmatchedIds().get(0));
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(ExpandedIdsMatcher.tokenizeExpression("foo,bar,baz,wild*"), false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(4));
        requiredMatches.filterMatchedIds(Arrays.asList("foo","baz"));
        assertThat(requiredMatches.unmatchedIds(), hasSize(2));
        assertThat(requiredMatches.unmatchedIds().get(0), is(oneOf("bar", "wild*")));
        assertThat(requiredMatches.unmatchedIds().get(1), is(oneOf("bar", "wild*")));
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(new String[] {"foo","bar"}, false);
        assertThat(requiredMatches.unmatchedIds(), hasSize(2));
        requiredMatches.filterMatchedIds(Collections.singletonList("bar"));
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        assertEquals("foo", requiredMatches.unmatchedIds().get(0));
        assertTrue(requiredMatches.isOnlyExact());
    }

    public void testMatchingResourceIds_allowNoMatch() {
        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(new String[] {"*"}, true);
        assertThat(requiredMatches.unmatchedIds(), empty());
        assertFalse(requiredMatches.hasUnmatchedIds());
        requiredMatches.filterMatchedIds(Collections.emptyList());
        assertThat(requiredMatches.unmatchedIds(), empty());
        assertFalse(requiredMatches.hasUnmatchedIds());
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(new String[] {"foo*","bar"}, true);
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        assertTrue(requiredMatches.hasUnmatchedIds());
        requiredMatches.filterMatchedIds(Collections.singletonList("bar"));
        assertThat(requiredMatches.unmatchedIds(), empty());
        assertFalse(requiredMatches.hasUnmatchedIds());
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(new String[] {"foo*","bar"}, true);
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        requiredMatches.filterMatchedIds(Collections.emptyList());
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        assertEquals("bar", requiredMatches.unmatchedIds().get(0));
        assertFalse(requiredMatches.isOnlyExact());

        requiredMatches = new ExpandedIdsMatcher(new String[] {"foo","bar"}, true);
        assertThat(requiredMatches.unmatchedIds(), hasSize(2));
        requiredMatches.filterMatchedIds(Collections.singletonList("bar"));
        assertThat(requiredMatches.unmatchedIds(), hasSize(1));
        assertEquals("foo", requiredMatches.unmatchedIds().get(0));
        assertTrue(requiredMatches.isOnlyExact());
    }

    public void testIdMatches() {
        {
            ExpandedIdsMatcher matcher = new ExpandedIdsMatcher(new String[]{"*"}, true);
            assertTrue(matcher.idMatches(randomAlphaOfLength(5)));
        }

        {
            ExpandedIdsMatcher matcher = new ExpandedIdsMatcher(new String[]{"foo*","bar"}, true);
            assertTrue(matcher.idMatches("foo1"));
            assertTrue(matcher.idMatches("bar"));
            assertFalse(matcher.idMatches("car"));
        }
    }
}
