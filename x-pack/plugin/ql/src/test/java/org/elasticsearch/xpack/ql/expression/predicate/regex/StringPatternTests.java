/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.util.StringUtils;

public class StringPatternTests extends ESTestCase {

    private LikePattern like(String pattern, char escape) {
        return new LikePattern(pattern, escape);
    }

    private RLikePattern rlike(String pattern) {
        return new RLikePattern(pattern);
    }

    private boolean matchesAll(String pattern, char escape) {
        return like(pattern, escape).matchesAll();
    }

    private boolean exactMatch(String pattern, char escape) {
        String escaped = pattern.replace(Character.toString(escape), StringUtils.EMPTY);
        return escaped.equals(like(pattern, escape).exactMatch());
    }

    private boolean matchesAll(String pattern) {
        return rlike(pattern).matchesAll();
    }

    private boolean exactMatch(String pattern) {
        return pattern.equals(rlike(pattern).exactMatch());
    }

    public void testWildcardMatchAll() throws Exception {
        assertTrue(matchesAll("%", '0'));
        assertTrue(matchesAll("%%", '0'));

        assertFalse(matchesAll("a%", '0'));
        assertFalse(matchesAll("%_", '0'));
        assertFalse(matchesAll("%_%_%", '0'));
        assertFalse(matchesAll("_%", '0'));
        assertFalse(matchesAll("0%", '0'));
    }

    public void testRegexMatchAll() throws Exception {
        assertTrue(matchesAll(".*"));
        assertTrue(matchesAll(".*.*"));
        assertTrue(matchesAll(".*.?"));
        assertTrue(matchesAll(".?.*"));
        assertTrue(matchesAll(".*.?.*"));

        assertFalse(matchesAll("..*"));
        assertFalse(matchesAll("ab."));
        assertFalse(matchesAll("..?"));
    }

    public void testWildcardExactMatch() throws Exception {
        assertTrue(exactMatch("0%", '0'));
        assertTrue(exactMatch("0_", '0'));
        assertTrue(exactMatch("123", '0'));
        assertTrue(exactMatch("1230_", '0'));
        assertTrue(exactMatch("1230_321", '0'));

        assertFalse(exactMatch("%", '0'));
        assertFalse(exactMatch("%%", '0'));
        assertFalse(exactMatch("a%", '0'));
        assertFalse(exactMatch("a_", '0'));
    }

    public void testRegexExactMatch() throws Exception {
        assertFalse(exactMatch(".*"));
        assertFalse(exactMatch(".*.*"));
        assertFalse(exactMatch(".*.?"));
        assertFalse(exactMatch(".?.*"));
        assertFalse(exactMatch(".*.?.*"));
        assertFalse(exactMatch("..*"));
        assertFalse(exactMatch("ab."));
        assertFalse(exactMatch("..?"));

        assertTrue(exactMatch("abc"));
        assertTrue(exactMatch("12345"));
    }
}
