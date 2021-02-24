/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml;


import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.BlacklistedPathPatternMatcher;

public class BlacklistedPathPatternMatcherTests extends ESTestCase {

    public void testMatchesExact() {
        // suffix match
        assertMatch("cat.aliases/10_basic/Empty cluster", "/some/suite_path/cat.aliases/10_basic/Empty cluster");
        // exact match
        assertMatch("cat.aliases/10_basic/Empty cluster", "cat.aliases/10_basic/Empty cluster");
        // additional text at the end should not match
        assertNoMatch("cat.aliases/10_basic/Empty cluster", "cat.aliases/10_basic/Empty clusters in here");
    }

    public void testMatchesSimpleWildcardPatterns() {
        assertMatch("termvector/20_issue7121/*", "/suite/termvector/20_issue7121/test_first");
        assertMatch("termvector/20_issue7121/*", "/suite/termvector/20_issue7121/");
        // do not cross segment boundaries
        assertNoMatch("termvector/20_issue7121/*", "/suite/termvector/20_issue7121/test/first");
    }

    public void testMatchesMultiWildcardPatterns() {
        assertMatch("indices.get/10_basic/*allow_no_indices*", "/suite/indices.get/10_basic/we_allow_no_indices");
        assertMatch("indices.get/10_basic/*allow_no_indices*", "/suite/indices.get/10_basic/we_allow_no_indices_at_all");
        assertNoMatch("indices.get/10_basic/*allow_no_indices*", "/suite/indices.get/10_basic/we_allow_no_indices_at_all/here");
        assertMatch("indices.get/*/*allow_no_indices*", "/suite/indices.get/10_basic/we_allow_no_indices_at_all");
        assertMatch("indices.get/*/*allow_no_indices*", "/suite/indices.get/20_basic/we_allow_no_indices_at_all");
        assertMatch("*/*/*allow_no_indices*", "/suite/path/to/test/indices.get/20_basic/we_allow_no_indices_at_all");
    }

    public void testMatchesPatternsWithEscapedCommas() {
        assertMatch("indices.get/10_basic\\,20_advanced/foo", "/suite/indices.get/10_basic,20_advanced/foo");
    }

    public void testMatchesMixedPatterns() {
        assertMatch("indices.get/*/10_basic\\,20_advanced/*foo*", "/suite/indices.get/all/10_basic,20_advanced/foo");
        assertMatch("indices.get/*/10_basic\\,20_advanced/*foo*", "/suite/indices.get/all/10_basic,20_advanced/my_foo");
        assertMatch("indices.get/*/10_basic\\,20_advanced/*foo*", "/suite/indices.get/all/10_basic,20_advanced/foo_bar");
    }

    public void testIgnoresUnsupportedSyntax() {
        assertMatch("indices.get/10_basic/[foo]{bar}baz?quux.", "indices.get/10_basic/[foo]{bar}baz?quux.");
    }


    private void assertMatch(String pattern, String path) {
        BlacklistedPathPatternMatcher matcher = new BlacklistedPathPatternMatcher(pattern);
        assertTrue("Pattern [" + pattern + "] should have matched path [" + path + "]", matcher.isSuffixMatch(path));
    }

    private void assertNoMatch(String pattern, String path) {
        BlacklistedPathPatternMatcher matcher = new BlacklistedPathPatternMatcher(pattern);
        assertFalse("Pattern [" + pattern + "] should not have matched path [" + path + "]", matcher.isSuffixMatch(path));
    }
}
