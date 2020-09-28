/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.test.ESTestCase;

public class StringPatternTests extends ESTestCase {

    private boolean isTotalWildcard(String pattern, char escape) {
        return new LikePattern(pattern, escape).matchesAll();
    }

    private boolean isTotalRegex(String pattern) {
        return new RLikePattern(pattern).matchesAll();
    }

    public void testWildcardMatchAll() throws Exception {
        assertTrue(isTotalWildcard("%", '0'));
        assertTrue(isTotalWildcard("%%", '0'));

        assertFalse(isTotalWildcard("a%", '0'));
        assertFalse(isTotalWildcard("%_", '0'));
        assertFalse(isTotalWildcard("%_%_%", '0'));
        assertFalse(isTotalWildcard("_%", '0'));
        assertFalse(isTotalWildcard("0%", '0'));
    }

    public void testRegexMatchAll() throws Exception {
        assertTrue(isTotalRegex(".*"));
        assertTrue(isTotalRegex(".*.*"));
        assertTrue(isTotalRegex(".*.?"));
        assertTrue(isTotalRegex(".?.*"));
        assertTrue(isTotalRegex(".*.?.*"));

        assertFalse(isTotalRegex("..*"));
        assertFalse(isTotalRegex("ab."));
        assertFalse(isTotalRegex("..?"));
    }
}
