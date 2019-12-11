/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.sql.util.StringUtils.likeToJavaPattern;
import static org.elasticsearch.xpack.sql.util.StringUtils.likeToLuceneWildcard;
import static org.elasticsearch.xpack.sql.util.StringUtils.likeToUnescaped;

public class LikeConversionTests extends ESTestCase {

    private static String regex(String pattern) {
        return likeToJavaPattern(pattern, '|');
    }

    private static String wildcard(String pattern) {
        return likeToLuceneWildcard(pattern, '|');
    }

    private static String unescape(String pattern) {
        return likeToUnescaped(pattern, '|');
    }

    public void testNoRegex() {
        assertEquals("^fooBar$", regex("fooBar"));
    }

    public void testEscapedSqlWildcard() {
        assertEquals("^foo\\\\_bar$", regex("foo\\|_bar"));
    }

    public void testEscapedSqlWildcardGreedy() {
        assertEquals("^foo.*%bar$", regex("foo%|%bar"));
    }

    public void testSimpleSqlRegex1() {
        assertEquals("^foo.bar$", regex("foo_bar"));
    }

    public void testSimpleSqlRegex2() {
        assertEquals("^foo.*bar$", regex("foo%bar"));
    }

    public void testMultipleSqlRegexes() {
        assertEquals("^foo.*bar.$", regex("foo%bar_"));
    }

    public void testJavaRegexNoSqlRegex() {
        assertEquals("^foo\\.\\*bar$", regex("foo.*bar"));
    }

    public void testMultipleRegexAndSqlRegex() {
        assertEquals("^foo\\\\\\.\\*bar\\..*$", regex("foo\\.*bar.%"));
    }

    public void testEscapedJavaRegex() {
        assertEquals("^\\[a-zA-Z\\]$", regex("[a-zA-Z]"));
    }

    public void testComplicatedJavaRegex() {
        assertEquals("^\\^\\[0\\.\\.9\\]\\.\\*\\$$", regex("^[0..9].*$"));
    }

    public void testNoWildcard() {
        assertEquals("foo", wildcard("foo"));
    }

    public void testQuestionMarkWildcard() {
        assertEquals("foo?bar", wildcard("foo_bar"));
    }

    public void testStarWildcard() {
        assertEquals("foo*", wildcard("foo%"));
    }

    public void testWildcardEscapeLuceneWildcard() {
        assertEquals("foo\\*bar*", wildcard("foo*bar%"));
    }

    public void testWildcardEscapedWildcard() {
        assertEquals("foo\\*bar%", wildcard("foo*bar|%"));
    }

    public void testEscapedLuceneEscape() {
        assertEquals("foo\\\\\\*bar", wildcard("foo\\*bar"));
    }

    public void testMixOfEscapedLuceneAndSqlEscapes() {
        assertEquals("foo\\\\?_\\*bar*", wildcard("foo\\_|_*bar%"));
    }

    public void testWildcardIgnoreEscapedWildcard() {
        assertEquals("foo\\\\\\*bar*", wildcard("foo\\*bar%"));
    }

    public void testWildcardDoubleEscaping() {
        assertEquals("foo\\\\\\\\bar", wildcard("foo\\\\bar"));
    }

    public void testWildcardTripleEscaping() {
        assertEquals("foo\\\\\\\\bar\\?\\\\?", wildcard("foo\\\\bar?\\_"));
    }

    public void testWildcardIgnoreDoubleEscapedButSkipEscapingOfSql() {
        assertEquals("foo\\\\\\*bar\\\\?\\?", wildcard("foo\\*bar\\_?"));
    }

    public void testUnescapeLiteral() {
        assertEquals("foo", unescape("foo"));
    }

    public void testUnescapeEscaped() {
        assertEquals("foo_bar", unescape("foo|_bar"));
    }

    public void testUnescapeEscapedEscape() {
        assertEquals("foo|_bar", unescape("foo||_bar"));
    }

    public void testUnescapeLastCharEscape() {
        assertEquals("foo_bar|", unescape("foo|_bar|"));
    }

    public void testUnescapeMultipleEscapes() {
        assertEquals("foo|_bar|", unescape("foo|||_bar||"));
    }

}